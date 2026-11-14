CREATE TABLE kv (
  chave  TEXT PRIMARY KEY,
  valor  TEXT,
  versao BIGINT NOT NULL
);

CREATE TABLE controle (
  id         INT PRIMARY KEY DEFAULT 1,
  ultimo_seq BIGINT NOT NULL DEFAULT 0
);

INSERT INTO controle (id, ultimo_seq) VALUES (1, 0);