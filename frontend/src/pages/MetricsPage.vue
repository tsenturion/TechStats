<script setup>
import { reactive, ref } from 'vue'

import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'
import { useUiPrefs } from '../composables/useUiPrefs'

const { apiRequest } = useApi()
const { language } = useUiPrefs()

const errors = ref([])
const activeKey = ref('gateway')

const metricsTargets = [
  { key: 'gateway', title: 'Gateway /api/v1/metrics', service: 'gateway', path: '/api/v1/metrics' },
  { key: 'vacancy', title: 'Vacancy /api/v1/metrics', service: 'vacancy', path: '/api/v1/metrics' },
  { key: 'analyzer', title: 'Analyzer /api/v1/metrics', service: 'analyzer', path: '/api/v1/metrics' },
  { key: 'cache', title: 'Cache /api/v1/metrics', service: 'cache', path: '/api/v1/metrics' },
  { key: 'websocket', title: 'WebSocket /api/v1/metrics', service: 'websocket', path: '/api/v1/metrics' },
]

const payloads = reactive({
  gateway: null,
  vacancy: null,
  analyzer: null,
  cache: null,
  websocket: null,
  vacancySummary: null,
})

const loading = ref(false)

const messages = {
  ru: {
    subtitle: 'Сбор Prometheus-метрик и JSON summary из всех сервисов.',
    refreshAll: 'Обновить все метрики',
    refreshing: 'Обновление...',
    noMetrics: 'Метрики еще не загружены',
    vacancySummaryTitle: 'Vacancy Metrics Summary (JSON)',
    noSummary: 'Сводка еще не загружена',
    errors: 'Ошибки',
    unknownError: 'неизвестная ошибка',
    vacancySummaryScope: 'Сводка Vacancy',
  },
  en: {
    subtitle: 'Prometheus metrics collection and JSON summary from all services.',
    refreshAll: 'Refresh all metrics',
    refreshing: 'Refreshing...',
    noMetrics: 'No metrics loaded yet',
    vacancySummaryTitle: 'Vacancy Metrics Summary (JSON)',
    noSummary: 'No summary yet',
    errors: 'Errors',
    unknownError: 'unknown error',
    vacancySummaryScope: 'Vacancy summary',
  },
}

function t(key) {
  return messages[language.value]?.[key] || messages.en[key] || key
}

function addError(scope, error) {
  const detail = error?.data?.detail || error?.message || t('unknownError')
  errors.value.unshift(`[${scope}] ${detail}`)
}

async function loadTarget(target) {
  try {
    const response = await apiRequest(target.service, target.path, {
      parseAs: 'text',
    })
    payloads[target.key] = response.raw
  } catch (error) {
    addError(target.title, error)
  }
}

async function refreshMetrics() {
  loading.value = true
  errors.value = []

  await Promise.all(metricsTargets.map(loadTarget))

  try {
    const summary = await apiRequest('vacancy', '/api/v1/metrics/summary')
    payloads.vacancySummary = summary.data
  } catch (error) {
    addError(t('vacancySummaryScope'), error)
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Metrics" :subtitle="t('subtitle')">
      <button class="btn-primary" :disabled="loading" @click="refreshMetrics">{{ loading ? t('refreshing') : t('refreshAll') }}</button>
    </SectionHeader>

    <section class="panel p-4">
      <div class="mb-3 flex flex-wrap gap-2">
        <button
          v-for="target in metricsTargets"
          :key="target.key"
          class="btn-secondary"
          :class="activeKey === target.key ? '!bg-brand-600 !text-white !border-brand-600' : ''"
          @click="activeKey = target.key"
        >
          {{ target.key }}
        </button>
      </div>

      <pre class="code-block h-[28rem]">{{ payloads[activeKey] || t('noMetrics') }}</pre>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('vacancySummaryTitle') }}</h3>
      <pre class="code-block mt-3 max-h-72">{{ payloads.vacancySummary ? JSON.stringify(payloads.vacancySummary, null, 2) : t('noSummary') }}</pre>
    </section>

    <section v-if="errors.length" class="panel border-rose-200 bg-rose-50 p-4">
      <h3 class="panel-title text-base text-rose-700">{{ t('errors') }}</h3>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-rose-700">
        <li v-for="item in errors" :key="item">{{ item }}</li>
      </ul>
    </section>
  </div>
</template>
