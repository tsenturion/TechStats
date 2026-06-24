# TechStats

TechStats — микросервисная платформа для анализа вакансий HH.ru с прогрессом в realtime, отслеживанием KPI, runtime-настройками лимитов и наблюдаемостью.

## Возможности

- Поиск вакансий и загрузка деталей через API Gateway
- Поиск технологии в заголовке, сниппете и полном описании
- Вывод KPI: `tech_percentage` и `tech_vacancies/total_vacancies`
- Realtime-пайплайн анализа через WebSocket + синхронный REST fallback
- Ролевой доступ: `guest` / `user` / `admin`
- Админ-интерфейс для runtime-настроек (лимиты, задержки, batch, дефолты форм)
- Стек мониторинга Prometheus + Grafana

## Быстрый старт

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
```

Остановка:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml down
```

Production-like запуск использует отдельный override и требует секреты в локальном `.env`:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

## Основные URL

- Nginx edge / Frontend: `http://localhost:8080`
- API Gateway: `http://localhost:8000`
- Vacancy Service: `http://localhost:8001`
- Analyzer Service: `http://localhost:8002`
- Cache Service: `http://localhost:8003`
- WebSocket Service: `http://localhost:8004`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

## Учетные записи для dev

- `admin / admin`
- `user / user`
- без входа (`guest`) для read-only/public сценариев

## Сервисы

- `nginx` (`:8080`) — HTTP edge proxy без TLS для локального запуска
- `frontend` — SPA-интерфейс за Nginx edge proxy
- `api-gateway` (`:8000`) — входная точка, proxy, auth, aggregation
- `vacancy-service` (`:8001`) — поиск и детали вакансий через HH API
- `analyzer-service` (`:8002`) — анализ технологий и статистика
- `cache-service` (`:8003`) — API кэша, админ и cluster-операции
- `websocket-service` (`:8004`) — realtime-сессии и стримы анализа

## API Документация

- Gateway: `http://localhost:8080/docs`
- Vacancy: `http://localhost:8080/services/vacancy/docs`
- Analyzer: `http://localhost:8080/services/analyzer/docs`
- Cache: `http://localhost:8080/services/cache/docs`
- WebSocket Service: `http://localhost:8080/services/websocket/docs`

## Полная документация

Полная документация проекта доступна прямо в интерфейсе сайта:

- Откройте `http://localhost:8080`
- Перейдите в раздел `Documentation` в левом меню

## Тестирование

Запуск тестов по сервисам:

```bash
pytest -q shared/tests
pytest -q api-gateway/tests
pytest -q vacancy-service/tests
pytest -q analyzer-service/tests
pytest -q websocket-service/tests
pytest -q tests/integration/test_gateway_rbac_runtime_integration.py
```

Только интеграционные:

```bash
pytest -q -m integration
```
