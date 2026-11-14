# sequenciador/main.py
import json
import socket
import threading

HOST, PORT = "0.0.0.0", 9000

log: list[dict] = []          # mensagens ordenadas; seq = índice + 1
vistos: dict[str, int] = {}   # tx_id -> seq (deduplicação)
nova_msg = threading.Condition()  # lock + aviso aos assinantes


def enviar(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode())


def registrar(msg: dict) -> int:
    msg = {k: v for k, v in msg.items() if k != "op"}
    with nova_msg:
        tx_id = msg["tx_id"]
        if tx_id in vistos:
            return vistos[tx_id]
        seq = len(log) + 1
        log.append({"seq": seq, **msg})
        vistos[tx_id] = seq
        nova_msg.notify_all()
        return seq


def tratar_broadcast(f, conn, msg):
    while True:
        seq = registrar(msg)
        enviar(conn, {"seq": seq})
        linha = f.readline()
        if not linha:
            return
        msg = json.loads(linha)
        if msg.get("op") != "broadcast":
            enviar(conn, {"erro": "op inválida"})
            return


def tratar_subscribe(conn, cursor: int):
    cursor = max(cursor, 1)
    while True:
        with nova_msg:
            chegou = nova_msg.wait_for(lambda: len(log) >= cursor, timeout=15)
            msg = log[cursor - 1] if chegou else None
        if msg is None:
            enviar(conn, {"op": "ping"})
        else:
            enviar(conn, msg)
            cursor += 1


def tratar_conexao(conn):
    f = conn.makefile("rb")
    try:
        linha = f.readline()
        if not linha:
            return
        msg = json.loads(linha)
        op = msg.get("op")
        if op == "broadcast":
            tratar_broadcast(f, conn, msg)
        elif op == "subscribe":
            tratar_subscribe(conn, int(msg.get("from", 1)))
        elif op == "log":
            with nova_msg:
                copia = list(log)
            enviar(conn, copia)
        else:
            enviar(conn, {"erro": "op inválida"})
    except (ConnectionError, json.JSONDecodeError, KeyError, ValueError):
        pass
    finally:
        f.close()
        conn.close()


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen()
    print(f"Sequenciador ouvindo em {HOST}:{PORT}", flush=True)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=tratar_conexao, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()