# Pipeline de Analytics em Tempo Real

Plataforma de analytics em tempo real que consome o firehose do Bluesky
(Jetstream), processa o fluxo com Kafka e Apache Flink, e serve os resultados
via ClickHouse e Grafana. Projeto de aprendizado e portfolio.

Detalhes de objetivo, arquitetura e roadmap em [DIRETRIZ.md](DIRETRIZ.md).

## Arquitetura

```
Jetstream (WSS)  ->  Ingest (producer)  ->  Kafka  ->  Flink  ->  ClickHouse  ->  Grafana
```

## Como subir

Requisitos: Docker e Docker Compose.

```
git clone <repo> && cd pipeline_realtime
make up
```

Depois:

- Flink UI em `http://localhost:8081`.
- ClickHouse em `http://localhost:8123`.
- Grafana em `http://localhost:3000`.

`make reset` derruba tudo e apaga volumes para um ambiente do zero.
`make logs` acompanha os logs de todos os servicos.
`make down` para os servicos sem apagar dados.

## Estrutura do repositorio

```
docker/         docker-compose.yml e provisionamento de ClickHouse e Grafana
ingest/         producer Python que consome o Jetstream e publica no Kafka
processing/     Flink SQL (fontes, transformacoes, sinks) e jobs customizados
tests/          testes unitarios, de integracao e de carga
```

## Configuracao

Todas as variaveis vem de `.env`, criado a partir de `.env.example` no
primeiro `make up`. Nenhum valor sensivel tem default no codigo.

## Jobs de processamento (Flink SQL)

Os scripts em `processing/sql/` sao aplicados nesta ordem via Flink SQL
Client:

1. `01_sources.sql` — tabela de origem lendo do topico Kafka.
2. `02_transforms.sql` — deduplicacao e agregacao em janela.
3. `03_sinks.sql` — tabelas de destino no ClickHouse e inserts continuos.

## Testes

```
cd ingest
pip install -e .[dev]
pytest ../tests/unit
```

Testes de integracao e carga estao descritos na secao 12 de
[DIRETRIZ.md](DIRETRIZ.md) e usam Testcontainers com Kafka e ClickHouse reais.
