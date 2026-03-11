<script setup>
import { computed } from 'vue'

import SectionHeader from '../components/SectionHeader.vue'
import { useUiPrefs } from '../composables/useUiPrefs'

const { language } = useUiPrefs()
const isRu = computed(() => language.value === 'ru')

const ui = computed(() => {
  if (isRu.value) {
    return {
      title: 'Documentation',
      subtitle: 'Полная документация проекта в интерфейсе: как использовать систему, что означает каждый блок, и как диагностировать проблемы.',
      tocTitle: 'Содержание',
      quickTitle: 'Быстрый старт',
      architectureTitle: 'Архитектура и поток данных',
      rolesTitle: 'Роли и права доступа',
      uiGuideTitle: 'Руководство по разделам интерфейса',
      liveTitle: 'Live Analysis: поля, этапы и KPI',
      runtimeTitle: 'Runtime Settings и контроль API лимитов',
      apiTitle: 'API: Swagger и ключевые endpoint-ы',
      cacheTitle: 'Кэширование и request stats',
      metricsTitle: 'Метрики, Prometheus и Grafana',
      troubleshootingTitle: 'Troubleshooting',
      securityTitle: 'Безопасность и production',
      testTitle: 'Тестирование',
      sourceTitle: 'Источник истины',
      fullDocNote: 'Этот раздел является основной полной документацией проекта на сайте.',
      quickRun: 'Запуск стека',
      quickStop: 'Остановка стека',
      quickRestart: 'Перезапуск monitoring',
      linksTitle: 'Основные URL',
      linksAuth: 'Тестовые учетные записи',
      architectureIntro: 'Система построена как набор микросервисов с API Gateway на входе и realtime каналами для live-анализа.',
      dataFlowTitle: 'Поток данных в Live Analysis',
      rolesGuestTitle: 'guest (без входа)',
      rolesUserTitle: 'user',
      rolesAdminTitle: 'admin',
      uiGuideHint: 'Все разделы доступны из левого меню. Часть операций ограничена ролью.',
      liveFieldsTitle: 'Поля формы Live Analysis',
      liveStagesTitle: 'Этапы progress stream',
      liveResultTitle: 'Ключевые поля final result',
      runtimeHint: 'Раздел Admin Runtime Settings позволяет менять лимиты, задержки, batch-параметры и дефолты форм без правок кода.',
      runtimeExamplesTitle: 'Критичные настройки для контроля лимитов HH/API',
      apiHint: 'Полный список всегда доступен через Swagger и API Explorer.',
      cacheHint: 'Поле use_cache включает/выключает кэширование в поддерживаемых endpoint-ах.',
      metricsHint: 'Grafana dashboard provisioning уже включен в проект.',
      troubleshootingHint: 'Если что-то выглядит неверно (например, спорное совпадение технологии), начинайте с этих проверок.',
      securityHint: 'Текущая конфигурация ориентирована на local/dev.',
      sourceHint: 'При расхождении информации приоритет у фактического API и кода.',
      servicePurpose: 'Назначение',
      servicePort: 'Порт',
      serviceName: 'Сервис',
      swaggerService: 'Сервис',
      swaggerUrl: 'Swagger',
      endpointGroup: 'Группа',
      endpointList: 'Endpoint-ы',
      rolesUserDesc: 'Доступны новые поиски и анализы (REST + WebSocket), без админских операций.',
      rolesAdminDesc: 'Полный доступ, включая админские endpoint-ы и Runtime Settings.',
      rolesGuestDesc: 'Доступны общая статистика, исторические данные, диаграммы и read-only операции; прямые новые поиски недоступны.',
      urlHeader: 'URL',
      descriptionHeader: 'Описание',
      sectionHeader: 'Раздел',
      fieldHeader: 'Поле',
    }
  }

  return {
    title: 'Documentation',
    subtitle: 'Full project documentation inside the UI: how to use the system, what each block means, and how to troubleshoot issues.',
    tocTitle: 'Table of Contents',
    quickTitle: 'Quick Start',
    architectureTitle: 'Architecture and Data Flow',
    rolesTitle: 'Roles and Access Control',
    uiGuideTitle: 'UI Section Guide',
    liveTitle: 'Live Analysis: Fields, Stages, and KPI',
    runtimeTitle: 'Runtime Settings and API Limit Control',
    apiTitle: 'API: Swagger and Key Endpoints',
    cacheTitle: 'Caching and Request Stats',
    metricsTitle: 'Metrics, Prometheus, and Grafana',
    troubleshootingTitle: 'Troubleshooting',
    securityTitle: 'Security and Production Notes',
    testTitle: 'Testing',
    sourceTitle: 'Source of Truth',
    fullDocNote: 'This section is the primary full documentation of the project on the website.',
    quickRun: 'Start stack',
    quickStop: 'Stop stack',
    quickRestart: 'Restart monitoring',
    linksTitle: 'Main URLs',
    linksAuth: 'Test credentials',
    architectureIntro: 'The system is a microservice platform with API Gateway as entry point and realtime channels for live analysis.',
    dataFlowTitle: 'Live Analysis data flow',
    rolesGuestTitle: 'guest (without login)',
    rolesUserTitle: 'user',
    rolesAdminTitle: 'admin',
    uiGuideHint: 'All sections are available in the left menu. Some operations are role-restricted.',
    liveFieldsTitle: 'Live Analysis form fields',
    liveStagesTitle: 'Progress stream stages',
    liveResultTitle: 'Final result key fields',
    runtimeHint: 'Admin Runtime Settings allows changing limits, delays, batch parameters, and form defaults without code changes.',
    runtimeExamplesTitle: 'Critical settings to control HH/API limits',
    apiHint: 'The full endpoint list is always available via Swagger and API Explorer.',
    cacheHint: 'The use_cache field enables/disables caching in supported endpoints.',
    metricsHint: 'Grafana dashboard provisioning is already included in the project.',
    troubleshootingHint: 'If something looks wrong (for example, a debatable technology match), start with these checks.',
    securityHint: 'Current configuration is tailored for local/dev usage.',
    sourceHint: 'If information conflicts, actual API behavior and source code have priority.',
    servicePurpose: 'Purpose',
    servicePort: 'Port',
    serviceName: 'Service',
    swaggerService: 'Service',
    swaggerUrl: 'Swagger',
    endpointGroup: 'Group',
    endpointList: 'Endpoints',
    rolesUserDesc: 'Can run new searches and analyses (REST + WebSocket), but no admin operations.',
    rolesAdminDesc: 'Full access including admin endpoints and Runtime Settings.',
    rolesGuestDesc: 'Can view public stats, historical data, charts, and read-only operations; direct new searches are blocked.',
    urlHeader: 'URL',
    descriptionHeader: 'Description',
    sectionHeader: 'Section',
    fieldHeader: 'Field',
  }
})

const tocItems = computed(() => [
  { id: 'quick-start', label: ui.value.quickTitle },
  { id: 'architecture', label: ui.value.architectureTitle },
  { id: 'roles', label: ui.value.rolesTitle },
  { id: 'ui-guide', label: ui.value.uiGuideTitle },
  { id: 'live-analysis', label: ui.value.liveTitle },
  { id: 'runtime-settings', label: ui.value.runtimeTitle },
  { id: 'api', label: ui.value.apiTitle },
  { id: 'cache', label: ui.value.cacheTitle },
  { id: 'metrics', label: ui.value.metricsTitle },
  { id: 'troubleshooting', label: ui.value.troubleshootingTitle },
  { id: 'security', label: ui.value.securityTitle },
  { id: 'testing', label: ui.value.testTitle },
  { id: 'source-of-truth', label: ui.value.sourceTitle },
])

const serviceRows = computed(() => {
  if (isRu.value) {
    return [
      { service: 'frontend', port: '8088', purpose: 'SPA-интерфейс со всеми рабочими сценариями.' },
      { service: 'api-gateway', port: '8000', purpose: 'Единая точка входа, proxy, auth и aggregation.' },
      { service: 'vacancy-service', port: '8001', purpose: 'Интеграция с HH API: поиск и детали вакансий.' },
      { service: 'analyzer-service', port: '8002', purpose: 'Анализ технологий, patterns CRUD, stats.' },
      { service: 'cache-service', port: '8003', purpose: 'Кэш, maintenance, admin/cluster endpoint-ы.' },
      { service: 'websocket-service', port: '8004', purpose: 'Realtime прогресс анализа и управление сессиями.' },
      { service: 'prometheus', port: '9090', purpose: 'Сбор метрик.' },
      { service: 'grafana', port: '3000', purpose: 'Дашборды и визуализация.' },
    ]
  }

  return [
    { service: 'frontend', port: '8088', purpose: 'SPA interface with all operational workflows.' },
    { service: 'api-gateway', port: '8000', purpose: 'Single entry point, proxy, auth, and aggregation.' },
    { service: 'vacancy-service', port: '8001', purpose: 'HH API integration: vacancy search and details.' },
    { service: 'analyzer-service', port: '8002', purpose: 'Technology analysis, patterns CRUD, stats.' },
    { service: 'cache-service', port: '8003', purpose: 'Caching, maintenance, admin/cluster endpoints.' },
    { service: 'websocket-service', port: '8004', purpose: 'Realtime analysis progress and session management.' },
    { service: 'prometheus', port: '9090', purpose: 'Metrics collection.' },
    { service: 'grafana', port: '3000', purpose: 'Dashboards and visualization.' },
  ]
})

const uiSections = computed(() => {
  if (isRu.value) {
    return [
      { name: 'Overview', desc: 'Состояние сервисов, health payloads, live metrics stream.' },
      { name: 'Live Analysis', desc: 'Realtime pipeline через WebSocket и fallback Sync REST.' },
      { name: 'Vacancy Explorer', desc: 'Поиск вакансий, детали, batch, справочники HH.' },
      { name: 'Analyzer Lab', desc: 'Sync/async/batch/text analysis, patterns CRUD, analyzer stats.' },
      { name: 'Cache Studio', desc: 'Кэш-операции, maintenance, admin и cluster endpoint-ы.' },
      { name: 'WebSocket Ops', desc: 'WS каналы, сессии/соединения, admin операции.' },
      { name: 'Metrics', desc: 'Prometheus text метрики и JSON summary.' },
      { name: 'API Explorer', desc: 'OpenAPI по сервисам и универсальные запросы.' },
      { name: 'Admin Runtime Settings', desc: 'Изменение runtime-констант без изменений кода.' },
      { name: 'Documentation', desc: 'Полный системный guide на сайте.' },
    ]
  }

  return [
    { name: 'Overview', desc: 'Service status, health payloads, live metrics stream.' },
    { name: 'Live Analysis', desc: 'Realtime pipeline through WebSocket with Sync REST fallback.' },
    { name: 'Vacancy Explorer', desc: 'Vacancy search, details, batch, HH reference data.' },
    { name: 'Analyzer Lab', desc: 'Sync/async/batch/text analysis, patterns CRUD, analyzer stats.' },
    { name: 'Cache Studio', desc: 'Cache operations, maintenance, admin and cluster endpoints.' },
    { name: 'WebSocket Ops', desc: 'WS channels, sessions/connections, admin operations.' },
    { name: 'Metrics', desc: 'Prometheus text metrics and JSON summary.' },
    { name: 'API Explorer', desc: 'OpenAPI across services and generic requests.' },
    { name: 'Admin Runtime Settings', desc: 'Runtime constants management without code changes.' },
    { name: 'Documentation', desc: 'Complete system guide on the website.' },
  ]
})

const liveFields = computed(() => {
  if (isRu.value) {
    return [
      { key: 'vacancy_title', desc: 'Название вакансии для выборки из HH.' },
      { key: 'technology', desc: 'Технология, которую нужно найти в тексте вакансий.' },
      { key: 'exact_search', desc: 'Более строгий запрос по названию вакансии (с кавычками).' },
      { key: 'area', desc: 'Регион HH (например, 113 = Россия).' },
      { key: 'max_pages', desc: 'Сколько страниц HH искать максимум.' },
      { key: 'per_page', desc: 'Сколько вакансий загружать на страницу (до 100).' },
      { key: 'use_cache', desc: 'Использовать кэшированные ответы там, где поддерживается.' },
    ]
  }

  return [
    { key: 'vacancy_title', desc: 'Vacancy title query used to fetch HH vacancies.' },
    { key: 'technology', desc: 'Technology to detect in vacancy content.' },
    { key: 'exact_search', desc: 'Stricter vacancy title query mode (quoted search).' },
    { key: 'area', desc: 'HH area identifier (for example, 113 = Russia).' },
    { key: 'max_pages', desc: 'Maximum number of HH result pages to fetch.' },
    { key: 'per_page', desc: 'Vacancies per page (up to 100).' },
    { key: 'use_cache', desc: 'Use cached responses where endpoint supports it.' },
  ]
})

const liveStages = [
  'initializing',
  'fetching_vacancies',
  'vacancies_found',
  'fetching_details',
  'analyzing',
  'finalizing',
  'completed',
]

const liveResultFields = computed(() => {
  if (isRu.value) {
    return [
      { key: 'total_vacancies', desc: 'Сколько вакансий попало в анализ.' },
      { key: 'tech_vacancies', desc: 'Сколько вакансий содержат технологию.' },
      { key: 'tech_percentage', desc: 'KPI процент: tech_vacancies / total_vacancies * 100.' },
      { key: 'duplicate_vacancies_count', desc: 'Количество одинаковых вакансий (работодатель + название + описание).' },
      { key: 'vacancies_with_tech[]', desc: 'Список вакансий, где технология найдена.' },
      { key: 'cache_info', desc: 'Использование кэша: new/cached и доля кэша.' },
      { key: 'request_stats', desc: 'Реальные/cached запросы и время обработки.' },
    ]
  }

  return [
    { key: 'total_vacancies', desc: 'How many vacancies were analyzed.' },
    { key: 'tech_vacancies', desc: 'How many vacancies contain the technology.' },
    { key: 'tech_percentage', desc: 'KPI percentage: tech_vacancies / total_vacancies * 100.' },
    { key: 'duplicate_vacancies_count', desc: 'Number of duplicate vacancies (employer + title + description).' },
    { key: 'vacancies_with_tech[]', desc: 'List of vacancies where technology was found.' },
    { key: 'cache_info', desc: 'Cache usage summary: new/cached and cache share.' },
    { key: 'request_stats', desc: 'Real/cached requests and processing time.' },
  ]
})

const runtimeKeys = [
  'search_max_pages_hard_limit',
  'search_per_page_hard_limit',
  'vacancy_batch_max_ids',
  'gateway_*_delay_ms',
  'gateway_*_timeout_sec',
  'live_max_total_vacancies',
  'live_progress_update_interval_sec',
  'live_progress_batch_size',
  'hh_rate_limit_per_second',
  'hh_rate_limit_per_day',
  'analyzer_batch_size',
  'auth_access_token_expire_minutes',
]

const swaggerRows = [
  { service: 'API Gateway', url: 'http://localhost:8000/docs' },
  { service: 'Vacancy Service', url: 'http://localhost:8001/docs' },
  { service: 'Analyzer Service', url: 'http://localhost:8002/docs' },
  { service: 'Cache Service', url: 'http://localhost:8003/docs' },
  { service: 'WebSocket Service', url: 'http://localhost:8004/docs' },
]

const endpointGroups = [
  {
    group: 'API Gateway (/api/v1/*)',
    endpoints: [
      'POST /auth/login',
      'POST /auth/register',
      'GET /auth/me',
      'GET /runtime-settings/public',
      'GET /admin/runtime-settings',
      'PUT /admin/runtime-settings',
      'POST /admin/runtime-settings/reset',
      'GET /vacancies/search',
      'GET /vacancies/{vacancy_id}',
      'GET /vacancies/batch',
      'POST /analyze',
      'WS /ws/analyze',
      'WS /ws/metrics',
      'GET /health',
      'GET /health/services',
      'GET /metrics',
    ],
  },
  {
    group: 'Vacancy Service',
    endpoints: [
      'GET /api/v1/search',
      'GET /api/v1/vacancies/{vacancy_id}',
      'POST /api/v1/vacancies/batch',
      'GET /api/v1/areas',
      'GET /api/v1/metro/{city_id}',
      'GET /api/v1/industries',
      'GET /api/v1/professional-roles',
      'GET /api/v1/rate-limit/stats',
      'GET /api/v1/health',
      'GET /api/v1/metrics',
    ],
  },
  {
    group: 'Analyzer Service',
    endpoints: [
      'POST /api/v1/analyze',
      'POST /api/v1/analyze/async',
      'GET /api/v1/analyze/async/{task_id}/status',
      'GET /api/v1/analyze/async/{task_id}/result',
      'POST /api/v1/analyze/batch',
      'POST /api/v1/analyze/text',
      'GET /api/v1/patterns',
      'GET /api/v1/patterns/{technology}',
      'POST /api/v1/patterns',
      'PUT /api/v1/patterns/{tech_id}',
      'DELETE /api/v1/patterns/{tech_id}',
      'GET /api/v1/patterns/categories',
      'POST /api/v1/patterns/search',
      'GET /api/v1/patterns/stats',
      'GET /api/v1/stats/summary',
      'GET /api/v1/stats/technology/{technology}',
      'GET /api/v1/stats/comparison',
      'GET /api/v1/stats/cache',
      'GET /api/v1/stats/performance',
      'GET /api/v1/health',
      'GET /api/v1/metrics',
    ],
  },
  {
    group: 'Cache Service',
    endpoints: [
      'GET /api/v1/cache/{key}',
      'PUT /api/v1/cache/{key}',
      'DELETE /api/v1/cache/{key}',
      'POST /api/v1/cache/mget',
      'POST /api/v1/cache/mset',
      'GET /api/v1/cache/keys',
      'DELETE /api/v1/cache/clear',
      'POST /api/v1/cache/invalidate/tags',
      'GET /api/v1/admin/stats',
      'GET /api/v1/admin/stats/history',
      'POST /api/v1/admin/flush',
      'POST /api/v1/admin/cleanup/trigger',
      'GET /api/v1/admin/monitor/keys',
      'POST /api/v1/admin/config/update',
      'GET /api/v1/admin/export',
      'GET /api/v1/cluster/info',
      'POST /api/v1/cluster/nodes/join',
      'POST /api/v1/cluster/nodes/leave',
      'GET /api/v1/cluster/nodes/{node_id}/health',
      'POST /api/v1/cluster/rebalance',
      'GET /api/v1/cluster/distribution',
      'GET /api/v1/health',
      'GET /api/v1/metrics',
    ],
  },
  {
    group: 'WebSocket Service',
    endpoints: [
      'WS /api/v1/ws/analyze',
      'WS /api/v1/ws/proxy',
      'WS /api/v1/ws/notifications',
      'WS /api/v1/ws/status',
      'GET /api/v1/ws/sessions',
      'GET /api/v1/ws/sessions/{session_id}',
      'POST /api/v1/ws/sessions/{session_id}/cancel',
      'DELETE /api/v1/ws/sessions/{session_id}',
      'GET /api/v1/ws/connections',
      'POST /api/v1/ws/broadcast',
      'GET /api/v1/admin/connections',
      'GET /api/v1/admin/connections/{connection_id}',
      'DELETE /api/v1/admin/connections/{connection_id}',
      'GET /api/v1/admin/sessions',
      'GET /api/v1/admin/sessions/stats',
      'POST /api/v1/admin/system/cleanup',
      'GET /api/v1/admin/system/info',
      'GET /api/v1/health',
      'GET /api/v1/metrics',
    ],
  },
]

const troubleshootingItems = computed(() => {
  if (isRu.value) {
    return [
      'В Grafana не видно dashboard: проверьте provisioning файлы и перезапустите grafana.',
      'Live Analysis долго стоит на analyzing: проверьте websocket-service, analyzer-service и HH rate limits.',
      'Progress и final total не совпадают: источником истины является final result.',
      'Найдена спорная технология (например React): проверяйте full description и patterns/aliases в Analyzer Lab.',
      'После рестарта monitoring 502/empty UI: подождите 5-15 секунд и обновите страницу.',
    ]
  }

  return [
    'Grafana dashboard is missing: verify provisioning files and restart grafana.',
    'Live Analysis is stuck at analyzing: check websocket-service, analyzer-service, and HH rate limits.',
    'Progress total differs from final total: final result is the source of truth.',
    'Debatable technology match (for example React): inspect full description and patterns/aliases in Analyzer Lab.',
    '502/empty monitoring UI after restart: wait 5-15 seconds and refresh.',
  ]
})

const securityItems = computed(() => {
  if (isRu.value) {
    return [
      'Вынести секреты в .env/secret store.',
      'Ограничить CORS и доступ к admin endpoint-ам.',
      'Сменить default credentials и admin token.',
      'Настроить reverse proxy + TLS.',
      'Зафиксировать версии образов и зависимостей.',
    ]
  }

  return [
    'Move secrets to .env/secret store.',
    'Restrict CORS and admin endpoint exposure.',
    'Replace default credentials and admin token.',
    'Use reverse proxy with TLS.',
    'Pin image and dependency versions.',
  ]
})

const architectureSteps = computed(() => {
  if (isRu.value) {
    return [
      'Frontend открывает `/api/v1/ws/analyze` через API Gateway.',
      'Gateway проксирует поток в websocket-service.',
      'websocket-service получает список/детали вакансий и шлет progress-обновления.',
      'websocket-service вызывает analyzer-service для финального анализа.',
      'Событие completed возвращает `metadata.result` с KPI-полями.',
    ]
  }
  return [
    'Frontend opens `/api/v1/ws/analyze` through API Gateway.',
    'Gateway proxies stream to websocket-service.',
    'websocket-service fetches vacancy list and details, sends progress updates.',
    'websocket-service calls analyzer-service for final analysis.',
    'Completed event returns `metadata.result` with KPI fields.',
  ]
})

const cacheItems = computed(() => {
  if (isRu.value) {
    return [
      'API Gateway cache: короткое proxy-кэширование ответов.',
      'Vacancy/Analyzer cache: повторное использование поиска и результатов анализа.',
      'Cache Service + Redis: централизованное хранение кэша и admin-операции.',
    ]
  }
  return [
    'API Gateway cache: short proxy response caching.',
    'Vacancy/Analyzer cache: search and analysis result reuse.',
    'Cache Service + Redis: centralized cache storage and admin operations.',
  ]
})

const sourceItems = computed(() => {
  if (isRu.value) {
    return [
      'Swagger UI каждого сервиса (`/docs`) и OpenAPI (`/openapi.json`).',
      'Страница API Explorer в этом UI.',
      'Фактический исходный код сервисов и frontend.',
    ]
  }
  return [
    'Swagger UI for each service (`/docs`) and OpenAPI (`/openapi.json`).',
    'API Explorer page in this UI.',
    'Actual service and frontend source code.',
  ]
})

const metricsItems = computed(() => {
  if (isRu.value) {
    return [
      'http_requests_total и http_request_duration_seconds (gateway/services)',
      'analysis_requests_total и analysis_duration_seconds (analyzer)',
      'websocket_active_connections и счетчики сообщений (websocket-service)',
      'cache_hits_total, cache_misses_total, cache_size_items (cache-service)',
    ]
  }
  return [
    'http_requests_total and http_request_duration_seconds (gateway/services)',
    'analysis_requests_total and analysis_duration_seconds (analyzer)',
    'websocket_active_connections and message counters (websocket-service)',
    'cache_hits_total, cache_misses_total, cache_size_items (cache-service)',
  ]
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader :title="ui.title" :subtitle="ui.subtitle" />

    <section class="panel p-4">
      <p class="text-sm text-slate-600">{{ ui.fullDocNote }}</p>
    </section>

    <section id="toc" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.tocTitle }}</h3>
      <div class="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-3">
        <a
          v-for="item in tocItems"
          :key="item.id"
          class="btn-secondary justify-start"
          :href="`#${item.id}`"
        >
          {{ item.label }}
        </a>
      </div>
    </section>

    <section id="quick-start" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.quickTitle }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-3">
        <div>
          <p class="text-sm font-semibold text-slate-700">{{ ui.quickRun }}</p>
          <pre class="code-block mt-2">docker compose up -d --build</pre>
        </div>
        <div>
          <p class="text-sm font-semibold text-slate-700">{{ ui.quickStop }}</p>
          <pre class="code-block mt-2">docker compose down</pre>
        </div>
        <div>
          <p class="text-sm font-semibold text-slate-700">{{ ui.quickRestart }}</p>
          <pre class="code-block mt-2">docker compose restart prometheus grafana</pre>
        </div>
      </div>

      <h4 class="mt-4 text-sm font-semibold text-slate-700">{{ ui.linksTitle }}</h4>
      <div class="table-shell mt-2">
        <table>
          <thead>
            <tr>
              <th>{{ ui.urlHeader }}</th>
              <th>{{ ui.descriptionHeader }}</th>
            </tr>
          </thead>
          <tbody>
            <tr><td class="font-mono text-xs">http://localhost:8088</td><td>Frontend</td></tr>
            <tr><td class="font-mono text-xs">http://localhost:8000</td><td>API Gateway</td></tr>
            <tr><td class="font-mono text-xs">http://localhost:8001</td><td>Vacancy Service</td></tr>
            <tr><td class="font-mono text-xs">http://localhost:8002</td><td>Analyzer Service</td></tr>
            <tr><td class="font-mono text-xs">http://localhost:8003</td><td>Cache Service</td></tr>
            <tr><td class="font-mono text-xs">http://localhost:8004</td><td>WebSocket Service</td></tr>
            <tr><td class="font-mono text-xs">http://localhost:9090</td><td>Prometheus</td></tr>
            <tr><td class="font-mono text-xs">http://localhost:3000</td><td>Grafana (admin/admin)</td></tr>
          </tbody>
        </table>
      </div>

      <h4 class="mt-4 text-sm font-semibold text-slate-700">{{ ui.linksAuth }}</h4>
      <pre class="code-block mt-2">admin / admin
user / user</pre>
    </section>

    <section id="architecture" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.architectureTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.architectureIntro }}</p>
      <div class="table-shell mt-3">
        <table>
          <thead>
            <tr>
              <th>{{ ui.serviceName }}</th>
              <th>{{ ui.servicePort }}</th>
              <th>{{ ui.servicePurpose }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in serviceRows" :key="row.service">
              <td class="font-mono text-xs">{{ row.service }}</td>
              <td class="font-mono text-xs">{{ row.port }}</td>
              <td>{{ row.purpose }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <h4 class="mt-4 text-sm font-semibold text-slate-700">{{ ui.dataFlowTitle }}</h4>
      <ol class="mt-2 list-decimal space-y-1 pl-5 text-sm text-slate-700">
        <li v-for="item in architectureSteps" :key="item">{{ item }}</li>
      </ol>
    </section>

    <section id="roles" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.rolesTitle }}</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <div class="rounded-xl border border-slate-200 p-3">
          <p class="text-sm font-semibold text-slate-800">{{ ui.rolesGuestTitle }}</p>
          <p class="mt-1 text-sm text-slate-600">{{ ui.rolesGuestDesc }}</p>
        </div>
        <div class="rounded-xl border border-slate-200 p-3">
          <p class="text-sm font-semibold text-slate-800">{{ ui.rolesUserTitle }}</p>
          <p class="mt-1 text-sm text-slate-600">{{ ui.rolesUserDesc }}</p>
        </div>
        <div class="rounded-xl border border-slate-200 p-3">
          <p class="text-sm font-semibold text-slate-800">{{ ui.rolesAdminTitle }}</p>
          <p class="mt-1 text-sm text-slate-600">{{ ui.rolesAdminDesc }}</p>
        </div>
      </div>
    </section>

    <section id="ui-guide" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.uiGuideTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.uiGuideHint }}</p>
      <div class="table-shell mt-3">
        <table>
          <thead>
            <tr>
              <th>{{ ui.sectionHeader }}</th>
              <th>{{ ui.descriptionHeader }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in uiSections" :key="item.name">
              <td class="font-semibold">{{ item.name }}</td>
              <td>{{ item.desc }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="live-analysis" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.liveTitle }}</h3>

      <h4 class="mt-3 text-sm font-semibold text-slate-700">{{ ui.liveFieldsTitle }}</h4>
      <div class="table-shell mt-2">
        <table>
          <thead>
            <tr>
              <th>{{ ui.fieldHeader }}</th>
              <th>{{ ui.descriptionHeader }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in liveFields" :key="row.key">
              <td class="font-mono text-xs">{{ row.key }}</td>
              <td>{{ row.desc }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <h4 class="mt-4 text-sm font-semibold text-slate-700">{{ ui.liveStagesTitle }}</h4>
      <div class="mt-2 flex flex-wrap gap-2">
        <span v-for="stage in liveStages" :key="stage" class="badge badge-neutral font-mono">{{ stage }}</span>
      </div>

      <h4 class="mt-4 text-sm font-semibold text-slate-700">{{ ui.liveResultTitle }}</h4>
      <div class="table-shell mt-2">
        <table>
          <thead>
            <tr>
              <th>{{ ui.fieldHeader }}</th>
              <th>{{ ui.descriptionHeader }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in liveResultFields" :key="row.key">
              <td class="font-mono text-xs">{{ row.key }}</td>
              <td>{{ row.desc }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="runtime-settings" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.runtimeTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.runtimeHint }}</p>
      <h4 class="mt-3 text-sm font-semibold text-slate-700">{{ ui.runtimeExamplesTitle }}</h4>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700">
        <li v-for="item in runtimeKeys" :key="item"><code>{{ item }}</code></li>
      </ul>
      <pre class="code-block mt-3">GET /api/v1/runtime-settings/public
GET /api/v1/admin/runtime-settings
PUT /api/v1/admin/runtime-settings
POST /api/v1/admin/runtime-settings/reset</pre>
    </section>

    <section id="api" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.apiTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.apiHint }}</p>

      <div class="table-shell mt-3">
        <table>
          <thead>
            <tr>
              <th>{{ ui.swaggerService }}</th>
              <th>{{ ui.swaggerUrl }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in swaggerRows" :key="item.service">
              <td>{{ item.service }}</td>
              <td><a class="text-brand-700 underline" :href="item.url" target="_blank" rel="noreferrer">{{ item.url }}</a></td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="table-shell mt-3">
        <table>
          <thead>
            <tr>
              <th>{{ ui.endpointGroup }}</th>
              <th>{{ ui.endpointList }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="group in endpointGroups" :key="group.group">
              <td class="font-semibold">{{ group.group }}</td>
              <td>
                <ul class="list-disc space-y-1 pl-4">
                  <li v-for="endpoint in group.endpoints" :key="endpoint"><code>{{ endpoint }}</code></li>
                </ul>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="cache" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.cacheTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.cacheHint }}</p>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700">
        <li v-for="item in cacheItems" :key="item">{{ item }}</li>
      </ul>
      <pre class="code-block mt-3">request_stats:
  real_requests
  cached_requests
  total_requests
  cache_hit_rate
  processing_time</pre>
    </section>

    <section id="metrics" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.metricsTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.metricsHint }}</p>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700">
        <li v-for="item in metricsItems" :key="item">{{ item }}</li>
      </ul>
      <pre class="code-block mt-3">docker compose ps
docker compose logs -f prometheus grafana
curl http://localhost:9090/api/v1/targets</pre>
    </section>

    <section id="troubleshooting" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.troubleshootingTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.troubleshootingHint }}</p>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700">
        <li v-for="item in troubleshootingItems" :key="item">{{ item }}</li>
      </ul>
    </section>

    <section id="security" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.securityTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.securityHint }}</p>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-700">
        <li v-for="item in securityItems" :key="item">{{ item }}</li>
      </ul>
    </section>

    <section id="testing" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.testTitle }}</h3>
      <pre class="code-block mt-2">pytest -q shared/tests
pytest -q api-gateway/tests
pytest -q vacancy-service/tests
pytest -q analyzer-service/tests
pytest -q websocket-service/tests
pytest -q tests/integration/test_gateway_rbac_runtime_integration.py
pytest -q -m integration</pre>
    </section>

    <section id="source-of-truth" class="panel p-4">
      <h3 class="panel-title text-base">{{ ui.sourceTitle }}</h3>
      <p class="mt-2 text-sm text-slate-600">{{ ui.sourceHint }}</p>
      <ol class="mt-2 list-decimal space-y-1 pl-5 text-sm text-slate-700">
        <li v-for="item in sourceItems" :key="item">{{ item }}</li>
      </ol>
    </section>
  </div>
</template>
