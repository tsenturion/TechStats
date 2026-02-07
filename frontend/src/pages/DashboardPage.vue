<script setup>
import { onBeforeUnmount, onMounted, reactive, ref } from 'vue'

import SectionHeader from '../components/SectionHeader.vue'
import ServiceCard from '../components/ServiceCard.vue'
import JsonBlock from '../components/JsonBlock.vue'
import { useApi } from '../composables/useApi'

const { config, apiRequest, wsUrl } = useApi()

const loading = ref(false)
const gatewayServices = ref(null)
const dashboardErrors = ref([])
const liveMetrics = ref(null)
const rawHealthPayloads = reactive({})

const services = reactive([
  { key: 'gateway', name: 'API Gateway', path: '/api/v1/health', status: 'unknown', details: '', latency: '' },
  { key: 'vacancy', name: 'Vacancy Service', path: '/api/v1/health', status: 'unknown', details: '', latency: '' },
  { key: 'analyzer', name: 'Analyzer Service', path: '/api/v1/health', status: 'unknown', details: '', latency: '' },
  { key: 'cache', name: 'Cache Service', path: '/api/v1/health', status: 'unknown', details: '', latency: '' },
  { key: 'websocket', name: 'WebSocket Service', path: '/api/v1/health', status: 'unknown', details: '', latency: '' },
])

let metricsSocket = null

async function pingService(target) {
  const started = performance.now()
  try {
    const response = await apiRequest(target.key, target.path)
    const elapsed = performance.now() - started
    const status = response.data?.status || (response.status === 200 ? 'healthy' : 'degraded')
    target.status = status
    target.latency = `${elapsed.toFixed(0)} ms`
    target.details = response.data?.service || 'ok'
    rawHealthPayloads[target.key] = response.data
  } catch (error) {
    target.status = 'error'
    target.details = error?.message || 'Request failed'
    target.latency = '-'
    rawHealthPayloads[target.key] = error?.data || { message: error?.message || 'unknown error' }
    dashboardErrors.value.push(`[${target.name}] ${error?.message || 'request failed'}`)
  }
}

async function refreshDashboard() {
  loading.value = true
  dashboardErrors.value = []

  await Promise.all(services.map((service) => pingService(service)))

  try {
    const response = await apiRequest('gateway', '/api/v1/health/services')
    gatewayServices.value = response.data
  } catch (error) {
    gatewayServices.value = { error: error?.message || 'Failed to load gateway service checks' }
  } finally {
    loading.value = false
  }
}

function startMetricsStream() {
  if (metricsSocket) {
    metricsSocket.close()
  }

  metricsSocket = new WebSocket(wsUrl('gateway', '/api/v1/ws/metrics'))
  metricsSocket.onmessage = (event) => {
    try {
      const parsed = JSON.parse(event.data)
      if (parsed?.type === 'metrics') {
        liveMetrics.value = parsed.data
      }
    } catch {
      // ignore invalid frames
    }
  }

  metricsSocket.onerror = () => {
    dashboardErrors.value.push('Live metrics stream disconnected')
  }
}

function stopMetricsStream() {
  if (metricsSocket) {
    metricsSocket.close()
    metricsSocket = null
  }
}

onMounted(async () => {
  await refreshDashboard()
  startMetricsStream()
})

onBeforeUnmount(() => {
  stopMetricsStream()
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Platform Overview" subtitle="Состояние всех микросервисов, включая live WebSocket telemetry.">
      <button class="btn-primary" :disabled="loading" @click="refreshDashboard">{{ loading ? 'Refreshing...' : 'Refresh' }}</button>
    </SectionHeader>

    <div class="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
      <ServiceCard
        v-for="service in services"
        :key="service.key"
        :name="service.name"
        :base-url="config[service.key]"
        :status="service.status"
        :details="service.details"
        :latency="service.latency"
      />
    </div>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Quick Links</h3>
      <div class="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        <a class="btn-secondary justify-start" :href="`${config.gateway}/docs`" target="_blank" rel="noreferrer">Gateway Docs</a>
        <a class="btn-secondary justify-start" :href="`${config.vacancy}/docs`" target="_blank" rel="noreferrer">Vacancy Docs</a>
        <a class="btn-secondary justify-start" :href="config.prometheus" target="_blank" rel="noreferrer">Prometheus</a>
        <a class="btn-secondary justify-start" :href="config.grafana" target="_blank" rel="noreferrer">Grafana</a>
      </div>
    </section>

    <div class="grid gap-4 lg:grid-cols-2">
      <JsonBlock title="Gateway -> /api/v1/health/services" :value="gatewayServices" max-height="18rem" />
      <JsonBlock title="Live Metrics Stream (/api/v1/ws/metrics)" :value="liveMetrics" max-height="18rem" />
    </div>

    <div class="grid gap-4 lg:grid-cols-2">
      <JsonBlock title="Vacancy Health Payload" :value="rawHealthPayloads.vacancy" max-height="16rem" />
      <JsonBlock title="Analyzer Health Payload" :value="rawHealthPayloads.analyzer" max-height="16rem" />
    </div>

    <div class="grid gap-4 lg:grid-cols-2">
      <JsonBlock title="Cache Health Payload" :value="rawHealthPayloads.cache" max-height="16rem" />
      <JsonBlock title="WebSocket Health Payload" :value="rawHealthPayloads.websocket" max-height="16rem" />
    </div>

    <section v-if="dashboardErrors.length" class="panel border-rose-200 bg-rose-50 p-4">
      <h3 class="panel-title text-base text-rose-700">Errors</h3>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-rose-700">
        <li v-for="item in dashboardErrors" :key="item">{{ item }}</li>
      </ul>
    </section>
  </div>
</template>
