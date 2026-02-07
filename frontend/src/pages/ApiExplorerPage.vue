<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'

import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'

const { apiRequest } = useApi()

const services = [
  { key: 'gateway', title: 'API Gateway' },
  { key: 'vacancy', title: 'Vacancy Service' },
  { key: 'analyzer', title: 'Analyzer Service' },
  { key: 'cache', title: 'Cache Service' },
  { key: 'websocket', title: 'WebSocket Service' },
]

const loadingDocs = ref(false)
const docs = reactive({
  gateway: null,
  vacancy: null,
  analyzer: null,
  cache: null,
  websocket: null,
})

const selectedService = ref('gateway')
const selectedPath = ref('')
const selectedMethod = ref('GET')

const requestEditor = reactive({
  queryJson: '{}',
  bodyJson: '{}',
  headersJson: '{}',
})

const responseState = reactive({
  status: null,
  url: '',
  data: null,
  raw: '',
  error: '',
})

const errors = ref([])

const currentDoc = computed(() => docs[selectedService.value])

const availablePaths = computed(() => {
  const openapi = currentDoc.value
  if (!openapi?.paths) return []
  return Object.keys(openapi.paths).sort()
})

const availableMethods = computed(() => {
  const openapi = currentDoc.value
  if (!openapi?.paths || !selectedPath.value) return []
  const pathSpec = openapi.paths[selectedPath.value]
  if (!pathSpec) return []
  return Object.keys(pathSpec).map((method) => method.toUpperCase())
})

const operationSpec = computed(() => {
  const openapi = currentDoc.value
  if (!openapi?.paths || !selectedPath.value || !selectedMethod.value) return null
  return openapi.paths[selectedPath.value]?.[selectedMethod.value.toLowerCase()] || null
})

function addError(scope, error) {
  const detail = error?.data?.detail || error?.message || String(error)
  errors.value.unshift(`[${scope}] ${detail}`)
}

function safeParseJson(text, fallback = {}) {
  if (!text || !text.trim()) return fallback
  try {
    return JSON.parse(text)
  } catch {
    return fallback
  }
}

async function loadDocs() {
  loadingDocs.value = true
  errors.value = []

  await Promise.all(
    services.map(async (service) => {
      try {
        const response = await apiRequest(service.key, '/openapi.json')
        docs[service.key] = response.data
      } catch (error) {
        docs[service.key] = { error: error?.message || 'Failed to load' }
        addError(`${service.title} openapi`, error)
      }
    }),
  )

  loadingDocs.value = false
}

watch(selectedService, () => {
  const paths = availablePaths.value
  selectedPath.value = paths[0] || ''
})

watch(
  availablePaths,
  (paths) => {
    if (!selectedPath.value && paths.length) {
      selectedPath.value = paths[0]
    }
  },
  { immediate: true },
)

watch(
  selectedPath,
  () => {
    const methods = availableMethods.value
    selectedMethod.value = methods[0] || 'GET'
  },
  { immediate: true },
)

async function sendRequest() {
  responseState.error = ''
  responseState.data = null
  responseState.raw = ''

  try {
    const query = safeParseJson(requestEditor.queryJson, {})
    const headers = safeParseJson(requestEditor.headersJson, {})

    let body
    if (!['GET', 'DELETE'].includes(selectedMethod.value)) {
      body = safeParseJson(requestEditor.bodyJson, {})
    }

    const response = await apiRequest(selectedService.value, selectedPath.value, {
      method: selectedMethod.value,
      query,
      body,
      headers,
    })

    responseState.status = response.status
    responseState.url = response.url
    responseState.data = response.data
    responseState.raw = response.raw
  } catch (error) {
    responseState.status = error?.status || null
    responseState.url = error?.url || ''
    responseState.data = error?.data || null
    responseState.error = error?.message || 'Request failed'
    addError('request', error)
  }
}

onMounted(() => {
  loadDocs()
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="API Explorer" subtitle="OpenAPI-driven inspector for every service. Covers all remaining endpoints without bespoke UI.">
      <button class="btn-primary" :disabled="loadingDocs" @click="loadDocs">{{ loadingDocs ? 'Loading...' : 'Reload OpenAPI' }}</button>
    </SectionHeader>

    <section class="panel p-4">
      <div class="grid gap-3 lg:grid-cols-3">
        <label class="text-sm">
          <span class="mb-1 block">Service</span>
          <select v-model="selectedService" class="form-input">
            <option v-for="service in services" :key="service.key" :value="service.key">{{ service.title }}</option>
          </select>
        </label>
        <label class="text-sm lg:col-span-2">
          <span class="mb-1 block">Path</span>
          <select v-model="selectedPath" class="form-input font-mono">
            <option v-for="path in availablePaths" :key="path" :value="path">{{ path }}</option>
          </select>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">Method</span>
          <select v-model="selectedMethod" class="form-input">
            <option v-for="method in availableMethods" :key="method" :value="method">{{ method }}</option>
          </select>
        </label>
        <label class="text-sm lg:col-span-2">
          <span class="mb-1 block">Operation summary</span>
          <input :value="operationSpec?.summary || operationSpec?.description || '-'" class="form-input" readonly />
        </label>
      </div>

      <div class="mt-4 grid gap-3 lg:grid-cols-3">
        <label class="text-sm">
          <span class="mb-1 block">Query JSON</span>
          <textarea v-model="requestEditor.queryJson" class="form-input h-24 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">Headers JSON</span>
          <textarea v-model="requestEditor.headersJson" class="form-input h-24 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">Body JSON</span>
          <textarea v-model="requestEditor.bodyJson" class="form-input h-24 font-mono"></textarea>
        </label>
      </div>

      <div class="mt-3 flex gap-2">
        <button class="btn-primary" @click="sendRequest">Send</button>
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Response</h3>
      <p class="mt-1 font-mono text-xs text-slate-500">{{ responseState.url || '-' }}</p>
      <p class="mt-1 text-sm text-slate-700">status: {{ responseState.status || '-' }}</p>
      <p v-if="responseState.error" class="mt-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{{ responseState.error }}</p>
      <pre class="code-block mt-3 max-h-[32rem]">{{ responseState.data ? JSON.stringify(responseState.data, null, 2) : responseState.raw || 'No response yet' }}</pre>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Loaded OpenAPI documents</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        <article v-for="service in services" :key="service.key" class="rounded-xl border border-slate-200 bg-white p-3">
          <p class="font-display font-semibold text-slate-900">{{ service.title }}</p>
          <p class="mt-1 text-xs text-slate-500">paths: {{ docs[service.key]?.paths ? Object.keys(docs[service.key].paths).length : 0 }}</p>
          <p class="mt-1 text-xs text-slate-500">version: {{ docs[service.key]?.info?.version || '-' }}</p>
        </article>
      </div>
    </section>

    <section v-if="errors.length" class="panel border-rose-200 bg-rose-50 p-4">
      <h3 class="panel-title text-base text-rose-700">Errors</h3>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-rose-700">
        <li v-for="item in errors" :key="item">{{ item }}</li>
      </ul>
    </section>
  </div>
</template>
