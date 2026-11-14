# Deferred Update Replication

Aplicação de exemplo do protocolo **Deferred Update Replication (DUR)**, desenvolvida para a disciplina de **Computação Distribuída**.

## Objetivo

O objetivo do trabalho é demonstrar, na prática, como um banco de dados replicado pode manter suas réplicas consistentes sem coordenação durante a execução das transações. No DUR:

1. **Execução local:** cada transação é executada inteiramente em uma única réplica, sobre um snapshot do banco. Leituras são registradas em um *readset* (chave → versão lida) e escritas ficam guardadas em um *writeset*, sem tocar no banco.
2. **Difusão ordenada:** no commit, a transação (readset + writeset) é enviada a todas as réplicas por meio de um serviço de **difusão atômica** (*atomic broadcast*), que garante que todas recebam as transações na mesma ordem.
3. **Certificação determinística:** cada réplica, ao receber uma transação, verifica se as versões lidas ainda são as atuais. Se forem, aplica o writeset (**commit**); caso contrário, descarta a transação (**abort**).

Como todas as réplicas recebem as mesmas transações na mesma ordem e aplicam o mesmo teste, todas chegam à mesma decisão e ao mesmo estado final — sem precisar trocar votos entre si. Transações somente leitura são confirmadas localmente, sem certificação.

## Arquitetura

```
             ┌──────────────┐
             │ Sequenciador │  ordena as transações (atomic broadcast)
             └──────┬───────┘
        ┌───────────┼───────────┐
   ┌────┴────┐ ┌────┴────┐ ┌────┴────┐
   │ Réplica │ │ Réplica │ │ Réplica │  API HTTP + certificação
   │   r1    │ │   r2    │ │   r3    │
   └────┬────┘ └────┬────┘ └────┬────┘
   ┌────┴────┐ ┌────┴────┐ ┌────┴────┐
   │   db1   │ │   db2   │ │   db3   │  PostgreSQL (armazenamento chave-valor)
   └─────────┘ └─────────┘ └─────────┘
```

- **`sequenciador/`** — servidor TCP que atribui um número de sequência global a cada transação e a entrega, em ordem, a todas as réplicas inscritas. Faz o papel do atomic broadcast de forma simplificada (sequenciador fixo).
- **`replica/`** — serviço Flask que executa as transações dos clientes, envia os pedidos de commit ao sequenciador e aplica as transações entregues após certificá-las. Cada réplica usa seu próprio PostgreSQL e guarda o último número de sequência aplicado, podendo retomar de onde parou após uma falha.
- **`cliente.py`** — cliente interativo de linha de comando para abrir transações, ler, escrever e fazer commit/abort em qualquer réplica.

## Como executar

Suba o sistema com Docker Compose:

```bash
docker compose up --build
```

As réplicas ficam expostas nas portas `8001`, `8002` e `8003`. Em outro terminal, conecte o cliente a uma delas:

```bash
python cliente.py localhost:8001
```

Para observar a convergência, abra clientes em réplicas diferentes, execute transações concorrentes sobre as mesmas chaves e compare o estado de cada réplica:

```bash
curl localhost:8001/estado
curl localhost:8002/estado
```

## Limitações

Por se tratar de uma aplicação didática, algumas simplificações foram feitas: o sequenciador é um ponto único de falha e mantém o log apenas em memória, e as transações em andamento ficam guardadas na memória da réplica que as executa.
