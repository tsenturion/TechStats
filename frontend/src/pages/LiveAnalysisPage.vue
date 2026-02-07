<script setup>
import { computed, onBeforeUnmount, reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { useApi } from '../composables/useApi'

const { apiRequest, wsUrl } = useApi()

const form = reactive({
  vacancy_title: 'Python Developer',
  technology: 'Python',
  exact_search: true,
  area: 113,
  max_pages: 3,
  per_page: 50,
  use_cache: true,
})

const streamState = ref('idle')
const progress = ref(0)
const progressMessage = ref('')
const sessionId = ref('')
const latestPayload = ref(null)
const finalResult = ref(null)
const timeline = ref([])
const requestError = ref('')

let analysisSocket = null

const hasResult = computed(() => Boolean(finalResult.value))

function resetOutput() {
  streamState.value = 'idle'
  progress.value = 0
  progressMessage.value = ''
  sessionId.value = ''
  latestPayload.value = null
  finalResult.value = null
  timeline.value = []
  requestError.value = ''
}

function stopWebsocket() {
  if (analysisSocket) {
    analysisSocket.close()
    analysisSocket = null
  }
}

function pushTimeline(event) {
  timeline.value.unshift({
    at: new Date().toISOString(),
    ...event,
  })
}

function handleStreamMessage(payload) {
  latestPayload.value = payload
  pushTimeline(payload)

  if (payload.session_id) {
    sessionId.value = payload.session_id
  }

  if (payload.type === 'error') {
    streamState.value = 'error'
    requestError.value = payload.message || 'Unknown websocket error'
    stopWebsocket()
    return
  }

  const stage = payload.stage || payload.type
  progress.value = Number(payload.progress ?? progress.value)
  progressMessage.value = payload.message || ''

  if (stage === 'completed') {
    streamState.value = 'completed'
    finalResult.value = payload.metadata?.result || payload.result || latestPayload.value
    stopWebsocket()
  } else {
    streamState.value = 'streaming'
  }
}

function startLiveAnalysis() {
  resetOutput()
  streamState.value = 'connecting'

  analysisSocket = new WebSocket(wsUrl('gateway', '/api/v1/ws/analyze'))

  analysisSocket.onopen = () => {
    streamState.value = 'streaming'
    analysisSocket.send(JSON.stringify(form))
  }

  analysisSocket.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data)
      handleStreamMessage(payload)
    } catch {
      requestError.value = 'Invalid websocket frame received'
      streamState.value = 'error'
    }
  }

  analysisSocket.onerror = () => {
    requestError.value = 'WebSocket connection failed'
    streamState.value = 'error'
  }

  analysisSocket.onclose = () => {
    if (streamState.value === 'streaming' || streamState.value === 'connecting') {
      streamState.value = 'idle'
    }
  }
}

async function runSyncAnalysis() {
  resetOutput()
  streamState.value = 'loading'
  try {
    const response = await apiRequest('gateway', '/api/v1/analyze', {
      method: 'POST',
      body: form,
      query: {
        use_cache: form.use_cache,
      },
    })
    finalResult.value = response.data
    progress.value = 100
    progressMessage.value = 'Synchronous analysis completed'
    streamState.value = 'completed'
  } catch (error) {
    requestError.value = error?.data?.detail || error?.message || 'Request failed'
    streamState.value = 'error'
  }
}

onBeforeUnmount(() => {
  stopWebsocket()
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Live Analysis" subtitle="Realtime pipeline through API Gateway WebSocket + fallback sync REST endpoint." />

    <section class="panel p-4">
      <div class="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Vacancy title</span>
          <input v-model="form.vacancy_title" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Technology</span>
          <input v-model="form.technology" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Area</span>
          <input v-model.number="form.area" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Max pages</span>
          <input v-model.number="form.max_pages" type="number" min="1" max="20" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Per page</span>
          <input v-model.number="form.per_page" type="number" min="1" max="100" class="form-input" />
        </label>
        <div class="grid gap-2 text-sm">
          <label class="inline-flex items-center gap-2">
            <input v-model="form.exact_search" type="checkbox" class="h-4 w-4" /> Exact search
          </label>
          <label class="inline-flex items-center gap-2">
            <input v-model="form.use_cache" type="checkbox" class="h-4 w-4" /> Use cache
          </label>
        </div>
      </div>

      <div class="mt-4 flex flex-wrap gap-2">
        <button class="btn-primary" @click="startLiveAnalysis">Run Live WebSocket</button>
        <button class="btn-secondary" @click="runSyncAnalysis">Run Sync REST</button>
        <button class="btn-danger" @click="stopWebsocket">Stop Stream</button>
      </div>
    </section>

    <section class="panel p-4">
      <div class="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div class="space-x-2">
          <span class="text-sm text-slate-500">state:</span>
          <StatusBadge :status="streamState" />
        </div>
        <p class="font-mono text-xs text-slate-500">session: {{ sessionId || 'n/a' }}</p>
      </div>

      <div class="h-4 overflow-hidden rounded-full bg-slate-200">
        <div class="h-full rounded-full bg-brand-600 transition-all" :style="{ width: `${Math.max(0, Math.min(100, progress))}%` }" />
      </div>
      <p class="mt-2 text-sm text-slate-700">{{ progressMessage || 'No active stage' }}</p>

      <p v-if="requestError" class="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
        {{ requestError }}
      </p>
    </section>

    <section class="panel p-4" v-if="timeline.length">
      <h3 class="panel-title text-base">Stream Timeline</h3>
      <div class="mt-3 max-h-64 overflow-auto">
        <div v-for="(item, idx) in timeline" :key="`${item.at}-${idx}`" class="mb-2 rounded-xl border border-slate-200 p-2">
          <div class="flex items-center justify-between gap-2">
            <p class="text-sm font-semibold text-slate-800">{{ item.stage || item.type }}</p>
            <p class="font-mono text-xs text-slate-500">{{ item.at }}</p>
          </div>
          <p class="text-sm text-slate-600">{{ item.message || '-' }}</p>
        </div>
      </div>
    </section>

    <div class="grid gap-4 xl:grid-cols-2">
      <JsonBlock title="Latest Event" :value="latestPayload" />
      <JsonBlock title="Final Result" :value="finalResult" />
    </div>

    <section v-if="hasResult && finalResult?.vacancies_with_tech" class="panel p-4">
      <h3 class="panel-title text-base">Vacancies With Technology</h3>
      <div class="table-shell mt-3">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>ID</th>
              <th>Title</th>
              <th>Matches</th>
              <th>URL</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(item, index) in finalResult.vacancies_with_tech" :key="item.id || index">
              <td>{{ index + 1 }}</td>
              <td class="font-mono text-xs">{{ item.id }}</td>
              <td>{{ item.name }}</td>
              <td>{{ item.match_count }}</td>
              <td>
                <a class="text-brand-700 underline" :href="item.url" target="_blank" rel="noreferrer">open</a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>
</template>
