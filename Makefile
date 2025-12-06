# C:\Users\user\Desktop\TechStats\Makefile
.PHONY: help build up down logs test clean

help:
	@echo "Доступные команды:"
	@echo "  make build     - Собрать все сервисы"
	@echo "  make up        - Запустить все сервисы в фоне"
	@echo "  make up-dev    - Запустить с логами (разработка)"
	@echo "  make down      - Остановить все сервисы"
	@echo "  make logs      - Показать логи API Gateway"
	@echo "  make logs-vacancy - Показать логи Vacancy Service"
	@echo "  make test      - Запустить тесты"
	@echo "  make clean     - Очистить все (контейнеры, volumes)"
	@echo "  make restart   - Перезапустить все сервисы"
	@echo "  make status    - Показать статус сервисов"

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