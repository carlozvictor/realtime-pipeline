# Diretriz do Projeto — Pipeline de Analytics em Tempo Real

## 1. Objetivo

Construir uma plataforma de analytics em tempo real que consome um firehose de
eventos, processa o fluxo com semântica de streaming (janelas, dedup, agregação
contínua) e serve os resultados em consultas de baixa latência e dashboards ao
vivo. O projeto é de aprendizado e portfólio, com foco em Kafka, Apache Flink e
ClickHouse.

## 2. Caso de uso

Fonte primária: firehose do Bluesky (Jetstream), JSON via WebSocket, público e
sem autenticação. Alto volume e variável, o que exercita o pipeline sob condições
próximas de produção.

Fonte alternativa para subir rápido: Wikimedia EventStreams (SSE, sem auth).

Métricas-alvo do produto:

- Eventos por segundo, por tipo (post, like, repost, follow).
- Top termos e domínios em janela deslizante.
- Distribuição por idioma.
- Top autores por minuto.
- Sinais simples de spam/bot por janela.

## 3. Princípios

- Docker-first. Todo componente sobe via `docker compose`. Nada depende de
  instalação manual na máquina do desenvolvedor.
- Replicação em poucos comandos. `make up` sobe o ambiente completo do zero.
- Configuração por ambiente. Sem valores fixos no código; tudo vem de `.env`.
- Código limpo. Nomes explícitos, funções curtas, responsabilidade única.
- Comentários só quando o "porquê" não é óbvio pelo código. Nada de comentário
  que repete o que a linha já diz.
- Sem emojis em código, commits, logs ou documentação.
- Determinismo nos testes. Lógica de streaming testada com event-time controlado.
- Idempotência. Reprocessar o mesmo offset não pode duplicar resultado.

## 4. Arquitetura

```
Jetstream (WSS)  ->  Ingest (producer)  ->  Kafka  ->  Flink  ->  ClickHouse  ->  Grafana
```

- Ingest: cliente WebSocket que normaliza o evento e publica no Kafka.
- Kafka: backbone de eventos, particionamento e retenção configuráveis.
- Flink: transformação, dedup, janelas e agregações; grava no ClickHouse.
- ClickHouse: camada OLAP; tabelas MergeTree e materialized views.
- Grafana: dashboards e alertas sobre o ClickHouse e sobre métricas de saúde.

## 5. Stack

Fixe versões explícitas no `docker-compose.yml` e confirme a mais recente estável
de cada imagem antes de pinar. Sugestão de ponto de partida:

- Kafka em modo KRaft (nó único), sem Zookeeper.
- Apache Flink com Flink SQL como abordagem principal.
- ClickHouse Server.
- Grafana.
- Ingest em Python 3.12.

Decisão de processamento: comece por Flink SQL. Aproveita sua base forte de SQL,
reduz boilerplate e cobre janelas e agregações. Escale para PyFlink ou DataStream
(Java) apenas quando precisar de estado customizado que o SQL não expressa bem.

## 6. Estrutura do repositório

```
realtime-analytics/
  docker/
    docker-compose.yml
    clickhouse/
      init/
        01_schema.sql
    grafana/
      provisioning/
        datasources/datasource.yml
        dashboards/dashboards.yml
  ingest/
    Dockerfile
    pyproject.toml
    src/ingest/
      main.py
      config.py
      jetstream.py
      producer.py
  processing/
    sql/
      01_sources.sql
      02_transforms.sql
      03_sinks.sql
    jobs/            # PyFlink/Java quando SQL nao bastar
  tests/
    unit/
    integration/
    load/
  Makefile
  .env.example
  README.md
```

## 7. Ambiente Docker

`docker/docker-compose.yml` como esqueleto. Ajuste imagens e tags antes de usar.

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - ./clickhouse/init:/docker-entrypoint-initdb.d
      - ch-data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

  flink-jobmanager:
    image: flink:1.20
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      FLINK_PROPERTIES: "jobmanager.rpc.address: flink-jobmanager"

  flink-taskmanager:
    image: flink:1.20
    depends_on:
      - flink-jobmanager
    command: taskmanager
    environment:
      FLINK_PROPERTIES: "jobmanager.rpc.address: flink-jobmanager\ntaskmanager.numberOfTaskSlots: 4"

  grafana:
    image: grafana/grafana:11.3.0
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}

  ingest:
    build: ../ingest
    depends_on:
      - kafka
    environment:
      JETSTREAM_URL: ${JETSTREAM_URL}
      KAFKA_BOOTSTRAP: kafka:9092
      KAFKA_TOPIC: ${KAFKA_TOPIC}

volumes:
  ch-data:
```

`.env.example`:

```
JETSTREAM_URL=wss://jetstream.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
KAFKA_TOPIC=bsky.events
GRAFANA_PASSWORD=admin
```

`Makefile` para replicação em um comando:

```makefile
.PHONY: up down logs reset

up:
	cp -n .env.example .env || true
	docker compose -f docker/docker-compose.yml up -d --build

down:
	docker compose -f docker/docker-compose.yml down

logs:
	docker compose -f docker/docker-compose.yml logs -f

reset:
	docker compose -f docker/docker-compose.yml down -v
```

## 8. Como subir

```
git clone <repo> && cd realtime-analytics
make up
```

Depois:

- Flink UI em `http://localhost:8081`.
- ClickHouse em `http://localhost:8123`.
- Grafana em `http://localhost:3000`.

`make reset` derruba tudo e apaga volumes para um ambiente do zero.

## 9. Convenções de código

Ingest (Python):

- Config isolada em `config.py`, lida de variáveis de ambiente, sem default
  sensível hardcoded.
- Reconexão com backoff no cliente WebSocket.
- Producer publica com chave por partição estável (ex.: DID do autor) para
  preservar ordem por chave.
- Logs estruturados em JSON, sem texto decorativo.

Exemplo mínimo de config, sem comentários supérfluos:

```python
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    jetstream_url: str
    kafka_bootstrap: str
    kafka_topic: str

    @staticmethod
    def from_env() -> "Config":
        return Config(
            jetstream_url=os.environ["JETSTREAM_URL"],
            kafka_bootstrap=os.environ["KAFKA_BOOTSTRAP"],
            kafka_topic=os.environ["KAFKA_TOPIC"],
        )
```

ClickHouse (SQL):

- Nome de tabela por camada: `raw_*`, `agg_*`.
- Ingestão do Kafka via engine Kafka + materialized view para a tabela MergeTree.
- Agregações em `AggregatingMergeTree` alimentado por materialized view.

## 10. Modelagem de dados

`docker/clickhouse/init/01_schema.sql` estabelece o alicerce. Esboço:

```sql
CREATE TABLE raw_events
(
    event_time DateTime64(3),
    kind String,
    did String,
    lang String,
    text String
)
ENGINE = MergeTree
ORDER BY (event_time, did);

CREATE TABLE agg_events_per_minute
(
    minute DateTime,
    kind String,
    events AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (minute, kind);

CREATE MATERIALIZED VIEW mv_events_per_minute TO agg_events_per_minute AS
SELECT
    toStartOfMinute(event_time) AS minute,
    kind,
    countState() AS events
FROM raw_events
GROUP BY minute, kind;
```

## 11. Roadmap em fases

- Fase 0 — Fundação. `docker compose` sobe Kafka, ClickHouse, Flink e Grafana.
  `make up` funciona de ponta a ponta em máquina limpa.
- Fase 1 — Ingestão. Producer conecta ao Jetstream, normaliza e publica no Kafka.
  Confirmar throughput e reconexão.
- Fase 2 — Processamento. Flink SQL lê do Kafka, deduplica, agrega em janela e
  grava no ClickHouse.
- Fase 3 — Serving. Tabelas e materialized views no ClickHouse; primeiros
  dashboards no Grafana.
- Fase 4 — Testes. Unit, integração e carga, conforme secao 12.
- Fase 5 — Observabilidade. Consumer lag, checkpoints do Flink e taxa de insert
  no Grafana, com alertas.

## 12. Estratégia de testes

- Unit (Flink). Test harness com event-time controlado para validar janela,
  watermark e dedup de forma determinística.
- Integração (Testcontainers). Kafka e ClickHouse reais em container; lote
  conhecido de eventos e asserção sobre as linhas resultantes.
- Carga. Gerador sintético com vazão controlada; medir consumer lag,
  backpressure, duração de checkpoint e taxa de insert.
- Falha e replay. Derrubar broker no meio do fluxo, reiniciar do checkpoint e
  verificar exactly-once comparando contagem de entrada com a do ClickHouse.
- Reconciliação. Comparar a materialized view em tempo real com um recálculo
  batch em SQL puro sobre os eventos crus.

## 13. Definição de pronto

- `make up` sobe o ambiente completo em máquina limpa, sem passo manual.
- Ingestão sustenta a vazão do firehose sem lag crescente.
- Agregação em tempo real bate com o recálculo batch na reconciliação.
- Falha de broker não gera duplicata nem perda após replay.
- Dashboards refletem o fluxo com latência de segundos.
- README permite a outra pessoa replicar sem contexto adicional.
