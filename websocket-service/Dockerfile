FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY websocket-service/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY websocket-service/ /app/
COPY shared /app/shared

RUN chown -R app:app /app
USER app

EXPOSE 8004
HEALTHCHECK --interval=30s --timeout=10s --retries=5 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8004/api/v1/health/ready')"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8004", "--workers", "1"]

FROM base AS dev
USER root
COPY websocket-service/requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt
USER app

FROM base AS runtime
