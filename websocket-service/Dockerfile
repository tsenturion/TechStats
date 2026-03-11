# C:\Users\user\Desktop\TechStats\websocket-service\Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Системные пакеты не устанавливаем: используем только Python-зависимости,
# чтобы сборка не зависела от внешних apt-репозиториев.

# Установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY . .

# Запуск сервера
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8004", "--workers", "1"]
