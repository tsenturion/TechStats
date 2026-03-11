# C:\Users\user\Desktop\TechStats\analyzer-service\Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Системные пакеты не устанавливаем: используем только Python wheels,
# чтобы избежать нестабильных внешних apt-репозиториев при сборке.

# Установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY . .

# Запуск сервера
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
