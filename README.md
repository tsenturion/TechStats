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
docker compose up -d --build
```

Остановка:

```bash
docker compose down
```

## Основные URL

- Frontend: `http://localhost:8088`
- API Gateway: `http://localhost:8000`
- Vacancy Service: `http://localhost:8001`
- Analyzer Service: `http://localhost:8002`
- Cache Service: `http://localhost:8003`
- WebSocket Service: `http://localhost:8004`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin/admin`)

## Учетные записи по умолчанию

- `admin / admin`
- `user / user`
- без входа (`guest`) для read-only/public сценариев

## Сервисы

- `frontend` (`:8088`) — SPA-интерфейс
- `api-gateway` (`:8000`) — входная точка, proxy, auth, aggregation
- `vacancy-service` (`:8001`) — поиск и детали вакансий через HH API
- `analyzer-service` (`:8002`) — анализ технологий и статистика
- `cache-service` (`:8003`) — API кэша, админ и cluster-операции
- `websocket-service` (`:8004`) — realtime-сессии и стримы анализа

## API Документация

- Gateway: `http://localhost:8000/docs`
- Vacancy: `http://localhost:8001/docs`
- Analyzer: `http://localhost:8002/docs`
- Cache: `http://localhost:8003/docs`
- WebSocket Service: `http://localhost:8004/docs`

## Полная документация

Полная документация проекта доступна прямо в интерфейсе сайта:

- Откройте `http://localhost:8088`
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
