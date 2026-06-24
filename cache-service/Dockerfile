FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY cache-service/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY cache-service/ /app/
COPY shared /app/shared

RUN chown -R app:app /app
USER app

EXPOSE 8003
HEALTHCHECK --interval=30s --timeout=10s --retries=5 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8003/api/v1/health/ready')"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]

FROM base AS dev
USER root
COPY cache-service/requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt
USER app

FROM base AS runtime
