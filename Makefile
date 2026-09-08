.PHONY: up down logs reset flink-submit

up:
	cp -n .env.example .env || true
	cp -n .env docker/.env || true
	docker compose -f docker/docker-compose.yml up -d --build

down:
	docker compose -f docker/docker-compose.yml down

logs:
	docker compose -f docker/docker-compose.yml logs -f

reset:
	docker compose -f docker/docker-compose.yml down -v

flink-submit:
	KAFKA_TOPIC=$$(grep '^KAFKA_TOPIC=' .env | cut -d= -f2) \
	KAFKA_BOOTSTRAP=kafka:9092 \
	CLICKHOUSE_HOST=clickhouse \
	./processing/submit.sh
