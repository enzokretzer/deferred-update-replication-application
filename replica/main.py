# replica/main.py
import json
import os
import socket
import threading
import time
import uuid
from concurrent.futures import Future
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request

REPLICA_ID = os.environ["REPLICA_ID"]
DATABASE_URL = os.environ["DATABASE_URL"]
SEQ_HOST, SEQ_PORT = os.environ["SEQUENCER_ADDR"].split(":")

app = Flask(__name__)

# tx_id -> {"readset": {chave: versao}, "writeset": {chave: valor}, "conn": ...}
transacoes: dict[str, dict] = {}

# tx_id -> Future que o laço de entrega resolve com "commit" ou "abort"
# (só existe na réplica de ORIGEM da transação)
pendentes: dict[str, Future] = {}
lock_pendentes = threading.Lock()


def conectar_bd(snapshot: bool = True):
    espera = 1
    max_tentativas = 10
    for tentativa in range(1, max_tentativas + 1):
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
            if snapshot:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            return conn
        except psycopg2.OperationalError as e:
            print(f"[REPLICA {REPLICA_ID}] Falha ao conectar o banco. Tentativa {tentativa}/{max_tentativas}.")
            if tentativa == max_tentativas:
                raise
            time.sleep(espera)
            espera = min(espera * 2, 30)



def ler_ultimo_seq(conn) -> int:
    with conn.cursor() as cursor:
        cursor.execute("SELECT ultimo_seq FROM controle WHERE id = 1")
        seq = cursor.fetchone()
        return seq[0]


@app.post("/tx")
def iniciar_tx():
    tx_id = f"{REPLICA_ID}-{uuid4().hex[:8]}"
    conn = conectar_bd()
    contexto = {"readset": {}, "writeset": {}, "conn": conn}
    transacoes[tx_id] = contexto
    return {"tx_id": tx_id}


@app.get("/tx/<tx_id>/<chave>")
def ler(tx_id, chave):
    transacao = transacoes.get(tx_id)
    if not transacao:
        return jsonify(erro=f"transacao {tx_id} nao existe ou ja terminou"), 404

    if chave in transacao["writeset"]:
        valor = transacao["writeset"][chave]
        return {"chave": chave, "valor": valor}

    valor = None
    versao = 0

    conn = transacao["conn"]
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT valor, versao FROM kv WHERE chave = %s",
            (chave,)
        )
        resultado = cursor.fetchone()
        if resultado:
            valor, versao = resultado

    transacao["readset"].setdefault(chave, versao)
    return {"chave": chave, "valor": valor, "versao": versao}


@app.put("/tx/<tx_id>/<chave>")
def escrever(tx_id, chave):
    transacao = transacoes.get(tx_id)
    if not transacao:
        return jsonify(erro=f"transacao {tx_id} nao existe ou ja terminou"), 404
    writeset = transacao["writeset"]
    writeset[chave] = request.get_json()["valor"]
    return {"chave": chave, "ok": True}


@app.post("/tx/<tx_id>/commit")
def commit(tx_id):
    transacao = transacoes.get(tx_id)
    if not transacao:
        return jsonify(erro=f"transacao {tx_id} nao existe ou ja terminou"), 404
    writeset = transacao["writeset"]
    readset = transacao["readset"]
    conn = transacao["conn"]
    resultado = None

    if not writeset:
        resultado = "commit"
        conn.commit()
        conn.close()
        transacoes.pop(tx_id, None)
    else:
        fut = Future()
        with lock_pendentes:
            pendentes[tx_id] = fut

        try:
            enviar_ao_sequenciador({"tx_id": tx_id, "origem": REPLICA_ID, "readset": readset, "writeset": writeset})
            resultado = fut.result(timeout=10)
        except TimeoutError:
            return jsonify(erro="Timeout ao esperar resposta do sequenciador"), 504
        except Exception:
            return jsonify(erro="Erro ao enviar mensagem ao sequenciador"), 503
        finally:
            conn.close()
            transacoes.pop(tx_id)
            with lock_pendentes:
                pendentes.pop(tx_id, None)

    return {"resultado": resultado}



@app.post("/tx/<tx_id>/abort")
def abortar(tx_id):
    transacao = transacoes.pop(tx_id, None)
    if transacao is None:
        return jsonify(erro="transação não existe"), 404
    transacao["conn"].close()
    return {"resultado": "abort"}


def enviar_ao_sequenciador(msg: dict) -> int:
    with socket.create_connection((SEQ_HOST, int(SEQ_PORT))) as s:
        commit_msg = json.dumps({"op": "broadcast", **msg}) + "\n"
        s.send(commit_msg.encode())

        f = s.makefile()
        linha = f.readline()
        if not linha:
            return
        resposta = json.loads(linha)
        if resposta.get("seq"):
            return resposta.get("seq")



def laco_de_entrega():
    conn_bd = conectar_bd(snapshot=False)
    while True:
        try:
            ultimo = ler_ultimo_seq(conn_bd)
            s = socket.create_connection((SEQ_HOST, int(SEQ_PORT)))
            subscribe = json.dumps({"op": "subscribe", "from": ultimo + 1}) + "\n"
            s.sendall(subscribe.encode())
            f = s.makefile("rb")
            for linha in f:
                msg = json.loads(linha)
                if msg.get("op") == "ping":
                    continue
                processar_entrega(conn_bd, msg)
        except (ConnectionError, OSError):
            pass
        except Exception as e:
            print(e)
            conn_bd.rollback()

        time.sleep(1)
        

def processar_entrega(conn_bd, msg: dict):
    if msg["seq"] <= ler_ultimo_seq(conn_bd):
        conn_bd.rollback()
        return

    resultado = certificar(conn_bd, msg)
    with conn_bd.cursor() as cursor:
        try:
            seq = msg["seq"]
            if resultado == "commit":
                for chave, valor in msg["writeset"].items():
                    cursor.execute(
                        """
                        INSERT INTO kv (chave, valor, versao)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (chave)
                        DO UPDATE SET
                            valor = EXCLUDED.valor,
                            versao = EXCLUDED.versao
                        """,
                        (chave, valor, seq)
                    )

            cursor.execute("UPDATE controle SET ultimo_seq = %s WHERE id = 1", (seq,))
            conn_bd.commit()
        except:
            conn_bd.rollback()
            raise

    if msg["origem"] == REPLICA_ID:
        with lock_pendentes:
            fut = pendentes.pop(msg["tx_id"], None)
        if fut is not None:
            fut.set_result(resultado)


def certificar(conn_bd, msg: dict) -> str:
    with conn_bd.cursor() as cursor:
        for chave, versao_lida in msg["readset"].items():
            cursor.execute("SELECT versao FROM kv WHERE chave = %s", (chave,))
            resultado = cursor.fetchone()

            versao_atual = 0
            if resultado:
                versao_atual = resultado[0]
            if versao_atual != versao_lida:
                return "abort"
            
    return "commit"


@app.get("/estado")
def estado():
    try:
        conn = conectar_bd(snapshot=False)
        kv = None
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM kv ORDER BY chave")
            kv = cursor.fetchall()

        ultimo_seq = ler_ultimo_seq(conn)
    finally:
        if conn:
            conn.close()

    return {"kv": kv, "ultimo_seq": ultimo_seq}


if __name__ == "__main__":
    t = threading.Thread(target=laco_de_entrega, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8000, threaded=True)