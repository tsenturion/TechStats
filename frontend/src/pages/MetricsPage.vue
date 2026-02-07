<script setup>
import { reactive, ref } from 'vue'

import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'

const { apiRequest } = useApi()

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

function addError(scope, error) {
  const detail = error?.data?.detail || error?.message || 'unknown error'
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
    addError('Vacancy summary', error)
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Metrics" subtitle="Сбор Prometheus-метрик и JSON summary из всех сервисов.">
      <button class="btn-primary" :disabled="loading" @click="refreshMetrics">{{ loading ? 'Refreshing...' : 'Refresh all metrics' }}</button>
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

      <pre class="code-block h-[28rem]">{{ payloads[activeKey] || 'No metrics loaded yet' }}</pre>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Vacancy Metrics Summary (JSON)</h3>
      <pre class="code-block mt-3 max-h-72">{{ payloads.vacancySummary ? JSON.stringify(payloads.vacancySummary, null, 2) : 'No summary yet' }}</pre>
    </section>

    <section v-if="errors.length" class="panel border-rose-200 bg-rose-50 p-4">
      <h3 class="panel-title text-base text-rose-700">Errors</h3>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-rose-700">
        <li v-for="item in errors" :key="item">{{ item }}</li>
      </ul>
    </section>
  </div>
</template>
