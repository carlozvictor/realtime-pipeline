# Pipeline de Analytics em Tempo Real

Pipeline de streaming que consome o firehose do Bluesky (Jetstream), processa
o fluxo com Kafka e Apache Flink, e serve os resultados via ClickHouse e
Grafana. Projeto de aprendizado e portfolio, com foco em processamento de
streams com estado (dedup, janelas, checkpointing) e reconciliação de dados.

## Arquitetura

```
Jetstream (WSS)
   -> Ingest (producer Python)
   -> Kafka (topico bsky.events)
   -> Flink SQL (dedup + agregacao em janela de 1 min)
        -> raw_events              (ClickHouse, linha a linha deduplicada)
        -> agg_events_per_minute_raw (ClickHouse, agregado calculado pelo Flink)
   -> ClickHouse materialized view (agg_events_per_minute, recalculo independente)
   -> Grafana (dashboard lendo as tres tabelas)
```

O agregado calculado pelo Flink e o recalculado pela materialized view nativa
do ClickHouse convivem de proposito: servem de **reconciliacao** — se os dois
baterem, o pipeline esta processando corretamente.

O job do Flink roda em **Application Mode**: o SQL de `processing/sql/` e
empacotado num pequeno bootstrap Java (`processing/jobs/pipeline-job/`) que
sobe junto com o JobManager. Não há passo manual de submissão — o pipeline
começa a processar assim que o cluster sobe.

## Como subir

Requisitos: Docker e Docker Compose.

```
git clone <repo> && cd realtime-pipeline
make up
```

Depois:

- Flink UI em `http://localhost:8082`
- ClickHouse em `http://localhost:8123`
- Grafana em `http://localhost:3000` (login `admin`, senha em `GRAFANA_PASSWORD`)

`make down` para os servicos sem apagar dados (o volume do ClickHouse e dos
checkpoints do Flink persiste).
`make reset` derruba tudo e apaga volumes para um ambiente do zero.
`make logs` acompanha os logs de todos os servicos.

## Estrutura do repositorio

```
docker/
  docker-compose.yml
  flink/Dockerfile          imagem custom: conectores Kafka/JDBC + job compilado
  clickhouse/
    init/01_schema.sql      schema das tabelas e da materialized view
    config/                 protocolo MySQL habilitado + usuarios dedicados (flink, grafana)
  grafana/provisioning/     datasource ClickHouse + dashboard provisionados automaticamente
ingest/                     producer Python: WebSocket -> Kafka
processing/
  sql/                      fonte, transformacoes e sinks do pipeline (Flink SQL)
  jobs/pipeline-job/        bootstrap Java que submete o SQL acima em Application Mode
tests/                      testes unitarios, de integracao e de carga
```

## Pipeline (Flink SQL)

`processing/sql/` é a fonte única de verdade do pipeline, aplicada nesta ordem:

1. `01_sources.sql` — tabela de origem lendo do tópico Kafka. Usa
   `scan.startup.mode = group-offsets`, então um restart do cluster retoma de
   onde parou em vez de reprocessar o histórico inteiro.
2. `02_transforms.sql` — deduplicação (`ROW_NUMBER` por `did+kind+ts`) e
   agregação em janela tumbling de 1 minuto.
3. `03_sinks.sql` — grava no ClickHouse via protocolo MySQL (o conector JDBC
   do Flink não tem dialeto nativo para ClickHouse).

## Tolerância a falhas

- **Checkpointing** habilitado (intervalo de 30s, modo `EXACTLY_ONCE`), com
  armazenamento em volume persistente — o estado de dedup/janela sobrevive a
  falhas de task.
- **Restart strategy** (`fixed-delay`, retry indefinido a cada 10s) — uma
  falha transitória de conexão (ex.: ClickHouse reiniciando) se recupera
  sozinha, sem intervenção manual.
- **Limitação conhecida**: o sink JDBC não é transacional (sem XA), então não
  é exactly-once ponta a ponta — uma janela pequena de linhas pode duplicar no
  `raw_events` exatamente no instante de uma falha entre checkpoints. Uma
  engine `ReplacingMergeTree` no ClickHouse fecharia esse gap.

## Configuração

Todas as variáveis vêm de `.env`, criado a partir de `.env.example` no
primeiro `make up`. Nenhum valor sensível tem default no código.

## Testes

```
cd ingest
pip install -e .[dev]
pytest ../tests/unit
```

Hoje só existe cobertura unitária para a normalização de eventos do Jetstream.
`tests/integration/` e `tests/load/` estão reservados mas ainda vazios.

## Próximos passos

- Testes de integração (Testcontainers com Kafka e ClickHouse reais) e CI
- `ReplacingMergeTree` para fechar o gap de exactly-once no sink
- TTL/retenção nas tabelas do ClickHouse
- Observabilidade: consumer lag, métricas de checkpoint do Flink, alertas no Grafana
