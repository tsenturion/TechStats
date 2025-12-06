# C:\Users\user\Desktop\TechStats\Makefile
.PHONY: help build up down logs test clean

help:
	@echo "Доступные команды:"
	@echo "  make build              - Собрать все сервисы"
	@echo "  make up                 - Запустить все сервисы в фоне"
	@echo "  make up-dev             - Запустить с логами (разработка)"
	@echo "  make down               - Остановить все сервисы"
	@echo "  make logs               - Показать логи API Gateway"
	@echo "  make logs-vacancy       - Показать логи Vacancy Service"
	@echo "  make logs-analyzer      - Показать логи Analyzer Service"
	@echo "  make logs-cache         - Показать логи Cache Service"
	@echo "  make logs-cache-cluster - Показать логи кластера кэша"
	@echo "  make test               - Запустить тесты"
	@echo "  make clean              - Очистить все (контейнеры, volumes)"
	@echo "  make restart            - Перезапустить все сервисы"
	@echo "  make status             - Показать статус сервисов"

build:
	docker-compose build

up:
	docker-compose up -d

up-dev:
	docker-compose up

down:
	docker-compose down

logs:
	docker-compose logs -f api-gateway

logs-vacancy:
	docker-compose logs -f vacancy-service

logs-all:
	docker-compose logs -f

test:
	docker-compose run --rm vacancy-service python -m pytest tests/

clean:
	docker-compose down -v
	docker system prune -f

restart:
	docker-compose restart

status:
	docker-compose ps

# Запуск конкретного сервиса
run-vacancy:
	docker-compose up -d vacancy-service redis

run-api:
	docker-compose up -d api-gateway vacancy-service redis

# Миграции и обновления
migrate:
	docker-compose run --rm vacancy-service python scripts/migrate.py

# Бэкенд
vacancy-shell:
	docker-compose exec vacancy-service /bin/bash

api-shell:
	docker-compose exec api-gateway /bin/bash

redis-cli:
	docker-compose exec redis redis-cli

# Мониторинг
monitor:
	open http://localhost:9090  # Prometheus
	open http://localhost:3000  # Grafana (admin/admin)

# Локальная разработка
dev-vacancy:
	cd vacancy-service && uvicorn main:app --reload --host 0.0.0.0 --port 8001

dev-api:
	cd api-gateway && uvicorn main:app --reload --host 0.0.0.0 --port 8000

logs-analyzer:
	docker-compose logs -f analyzer-service

# Запуск полной системы
run-full:
	docker-compose up -d api-gateway vacancy-service analyzer-service redis

# Бэкенд
analyzer-shell:
	docker-compose exec analyzer-service /bin/bash

# Тестирование
test-analyzer:
	docker-compose run --rm analyzer-service python -m pytest tests/

logs-cache:
	docker-compose logs -f cache-service

logs-cache-cluster:
	docker-compose logs -f cache-service cache-service-2 cache-service-3

# Запуск полной системы с кэшем
run-full-with-cache:
	docker-compose up -d api-gateway vacancy-service analyzer-service cache-service redis

# Запуск кластера кэша
run-cache-cluster:
	docker-compose up -d cache-service cache-service-2 cache-service-3 redis

# Бэкенд
cache-shell:
	docker-compose exec cache-service /bin/bash

cache-cluster-shell:
	docker-compose exec cache-service-2 /bin/bash

# Тестирование
test-cache:
	docker-compose run --rm cache-service python -m pytest tests/

# Мониторинг
monitor-cache:
	open http://localhost:3000/d/cache-service/cache-service-metrics  # Grafana
	open http://localhost:9090/graph?g0.expr=cache_operations_total  # Prometheus