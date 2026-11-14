import json
import sys
import urllib.error
import urllib.request


def requisitar(metodo, url, corpo=None):
    dados = json.dumps(corpo).encode() if corpo is not None else None
    req = urllib.request.Request(url, data=dados, method=metodo, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            texto = resp.read().decode()
            print(f"[{resp.status}] {texto}")
    except urllib.error.HTTPError as e:
        texto = e.read().decode()
        print(f"[{e.code}] {texto}")
    except urllib.error.URLError as e:
        print(f"Erro de conexão: {e.reason}")
        return None
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        return None


def menu_transacao(base, tx_id):
    while True:
        print(f"\nTransação {tx_id}")
        print("1) Ler\n2) Escrever\n3) Finalizar transação\n4) Abortar transação")
        opcao = input("> ").strip()
        if opcao == "1":
            chave = input("Chave: ")
            requisitar("GET", f"{base}/tx/{tx_id}/{chave}")
        elif opcao == "2":
            chave = input("Chave: ")
            valor = input("Valor: ")
            requisitar("PUT", f"{base}/tx/{tx_id}/{chave}", {"valor": valor})
        elif opcao == "3":
            requisitar("POST", f"{base}/tx/{tx_id}/commit")
            return
        elif opcao == "4":
            requisitar("POST", f"{base}/tx/{tx_id}/abort")
            return
        else:
            print("Opção inválida")


def main():
    if len(sys.argv) != 2:
        print(f"Uso: python {sys.argv[0]} IP:PORTA")
        sys.exit(1)
    base = f"http://{sys.argv[1]}"

    while True:
        print("\n1) Iniciar transação\n2) Sair")
        opcao = input("> ").strip()
        if opcao == "1":
            resposta = requisitar("POST", f"{base}/tx")
            if resposta and "tx_id" in resposta:
                menu_transacao(base, resposta["tx_id"])
        elif opcao == "2":
            break
        else:
            print("Opção inválida")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print()
