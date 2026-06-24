# TechStats Frontend

Full SPA for TechStats microservices.

## Stack

- Vue 3
- Vue Router
- Tailwind CSS
- Vite

## Run locally

```bash
npm install
npm run dev
```

Open: `http://localhost:5173`

## Build

```bash
npm run build
npm run preview
```

## Docker

The root `docker-compose.yml` includes `frontend` service.

```bash
docker compose -f ../docker-compose.yml -f ../docker-compose.dev.yml up -d --build frontend nginx
```

Open: `http://localhost:8080`
