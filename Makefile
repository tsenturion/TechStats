.PHONY: help build build-prod up up-dev up-prod down logs logs-all test test-unit test-integration test-all clean restart status

COMPOSE_DEV := docker compose -f docker-compose.yml -f docker-compose.dev.yml
COMPOSE_PROD := docker compose -f docker-compose.yml -f docker-compose.prod.yml

help:
	@echo "Доступные команды:"
	@echo "  make build              - Собрать dev-образы"
	@echo "  make build-prod         - Собрать production-образы"
	@echo "  make up                 - Запустить dev-стек в фоне"
	@echo "  make up-dev             - Запустить dev-стек с логами"
	@echo "  make up-prod            - Запустить production compose"
	@echo "  make down               - Остановить dev-стек"
	@echo "  make logs               - Показать логи API Gateway"
	@echo "  make logs-all           - Показать все логи"
	@echo "  make test-unit          - Запустить unit/smoke тесты всех сервисов"
	@echo "  make test-integration   - Запустить интеграционные тесты gateway"
	@echo "  make test-all           - Запустить unit + integration"
	@echo "  make clean              - Очистить dev containers и volumes"
	@echo "  make status             - Показать статус dev-сервисов"

build:
	$(COMPOSE_DEV) build

build-prod:
	$(COMPOSE_PROD) build

up:
	$(COMPOSE_DEV) up -d

up-dev:
	$(COMPOSE_DEV) up

up-prod:
	$(COMPOSE_PROD) up -d

down:
	$(COMPOSE_DEV) down

logs:
	$(COMPOSE_DEV) logs -f api-gateway

logs-vacancy:
	$(COMPOSE_DEV) logs -f vacancy-service

logs-analyzer:
	$(COMPOSE_DEV) logs -f analyzer-service

logs-cache:
	$(COMPOSE_DEV) logs -f cache-service

logs-cache-cluster:
	$(COMPOSE_DEV) --profile cache-cluster logs -f cache-service cache-service-2 cache-service-3

logs-websocket:
	$(COMPOSE_DEV) logs -f websocket-service

logs-all:
	$(COMPOSE_DEV) logs -f

test:
	$(COMPOSE_DEV) run --rm vacancy-service python -m pytest tests/

test-unit:
	pytest -q shared/tests
	pytest -q api-gateway/tests
	pytest -q vacancy-service/tests
	pytest -q analyzer-service/tests
	pytest -q websocket-service/tests

test-integration:
	pytest -q tests/integration/test_gateway_rbac_runtime_integration.py

test-all: test-unit test-integration

clean:
	$(COMPOSE_DEV) down -v
	docker system prune -f

restart:
	$(COMPOSE_DEV) restart

status:
	$(COMPOSE_DEV) ps

run-vacancy:
	$(COMPOSE_DEV) up -d vacancy-service redis

run-api:
	$(COMPOSE_DEV) up -d api-gateway vacancy-service redis

migrate:
	$(COMPOSE_DEV) run --rm vacancy-service python scripts/migrate.py

vacancy-shell:
	$(COMPOSE_DEV) exec vacancy-service /bin/bash

api-shell:
	$(COMPOSE_DEV) exec api-gateway /bin/bash

analyzer-shell:
	$(COMPOSE_DEV) exec analyzer-service /bin/bash

cache-shell:
	$(COMPOSE_DEV) exec cache-service /bin/bash

cache-cluster-shell:
	$(COMPOSE_DEV) --profile cache-cluster exec cache-service-2 /bin/bash

websocket-shell:
	$(COMPOSE_DEV) exec websocket-service /bin/bash

redis-cli:
	$(COMPOSE_DEV) exec redis redis-cli

monitor:
	open http://localhost:9090
	open http://localhost:3000

monitor-cache:
	open http://localhost:3000/d/cache-service/cache-service-metrics
	open http://localhost:9090/graph?g0.expr=cache_operations_total

run-full-with-cache:
	$(COMPOSE_DEV) up -d api-gateway vacancy-service analyzer-service cache-service redis

run-cache-cluster:
	$(COMPOSE_DEV) --profile cache-cluster up -d cache-service cache-service-2 cache-service-3 redis

run-full:
	$(COMPOSE_DEV) up -d api-gateway vacancy-service analyzer-service cache-service websocket-service redis nginx

test-analyzer:
	$(COMPOSE_DEV) run --rm analyzer-service python -m pytest tests/

test-cache:
	$(COMPOSE_DEV) run --rm cache-service python -m pytest tests/

test-websocket:
	curl -X POST http://localhost:8004/api/v1/ws/sessions/test \
		-H "Content-Type: application/json" \
		-d '{"vacancy_title": "Python Developer", "technology": "Python"}'

monitor-connections:
	curl http://localhost:8004/api/v1/ws/connections
