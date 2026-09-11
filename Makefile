.PHONY: up down logs reset

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
