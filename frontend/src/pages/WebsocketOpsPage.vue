<script setup>
import { onBeforeUnmount, reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'

const { apiRequest, wsUrl, config } = useApi()

const errors = ref([])
const statusStream = ref(null)
const notificationsLog = ref([])

let statusSocket = null
let notificationsSocket = null

const wsHttpOps = reactive({
  sessions: null,
  sessionDetails: null,
  connections: null,
  broadcast: null,
  cancel: null,
  remove: null,
})

const wsForms = reactive({
  sessionsLimit: 20,
  sessionsOffset: 0,
  sessionId: '',
  broadcastTopic: '',
  broadcastMessage: '{"kind":"notice","text":"hello from UI"}',
  broadcastExclude: '',
})

const adminOps = reactive({
  connections: null,
  connectionDetails: null,
  sessions: null,
  sessionStats: null,
  cleanup: null,
  systemInfo: null,
})

const adminForms = reactive({
  detailedConnections: true,
  connectionId: '',
  adminSessionStatus: '',
  adminCleanupType: 'all',
  includeHistory: false,
  historyLimit: 20,
})

function addError(scope, error) {
  const detail = error?.data?.detail || error?.message || 'unknown error'
  errors.value.unshift(`[${scope}] ${detail}`)
}

function startStatusStream() {
  stopStatusStream()
  statusSocket = new WebSocket(wsUrl('websocket', '/api/v1/ws/status'))
  statusSocket.onmessage = (event) => {
    try {
      statusStream.value = JSON.parse(event.data)
    } catch {
      // ignore
    }
  }
  statusSocket.onerror = () => {
    addError('ws/status', { message: 'status stream disconnected' })
  }
}

function stopStatusStream() {
  if (statusSocket) {
    statusSocket.close()
    statusSocket = null
  }
}

function startNotifications() {
  stopNotifications()
  notificationsSocket = new WebSocket(wsUrl('websocket', '/api/v1/ws/notifications'))
  notificationsSocket.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data)
      notificationsLog.value.unshift({ at: new Date().toISOString(), payload })
    } catch {
      // ignore
    }
  }
  notificationsSocket.onerror = () => {
    addError('ws/notifications', { message: 'notifications stream disconnected' })
  }
}

function unsubscribeNotifications() {
  if (notificationsSocket && notificationsSocket.readyState === WebSocket.OPEN) {
    notificationsSocket.send(JSON.stringify({ type: 'unsubscribe' }))
  }
}

function stopNotifications() {
  if (notificationsSocket) {
    notificationsSocket.close()
    notificationsSocket = null
  }
}

async function loadSessions() {
  try {
    const response = await apiRequest('websocket', '/api/v1/ws/sessions', {
      query: {
        limit: wsForms.sessionsLimit,
        offset: wsForms.sessionsOffset,
      },
    })
    wsHttpOps.sessions = response.data
  } catch (error) {
    addError('ws sessions', error)
  }
}

async function loadSessionDetails() {
  if (!wsForms.sessionId) return
  try {
    const response = await apiRequest('websocket', `/api/v1/ws/sessions/${encodeURIComponent(wsForms.sessionId)}`)
    wsHttpOps.sessionDetails = response.data
  } catch (error) {
    addError('ws session details', error)
  }
}

async function cancelSession() {
  if (!wsForms.sessionId) return
  try {
    const response = await apiRequest('websocket', `/api/v1/ws/sessions/${encodeURIComponent(wsForms.sessionId)}/cancel`, {
      method: 'POST',
    })
    wsHttpOps.cancel = response.data
  } catch (error) {
    addError('ws session cancel', error)
  }
}

async function deleteSession() {
  if (!wsForms.sessionId) return
  try {
    const response = await apiRequest('websocket', `/api/v1/ws/sessions/${encodeURIComponent(wsForms.sessionId)}`, {
      method: 'DELETE',
    })
    wsHttpOps.remove = response.data
  } catch (error) {
    addError('ws session delete', error)
  }
}

async function loadConnections() {
  try {
    const response = await apiRequest('websocket', '/api/v1/ws/connections')
    wsHttpOps.connections = response.data
  } catch (error) {
    addError('ws connections', error)
  }
}

async function broadcastMessage() {
  try {
    const response = await apiRequest('websocket', '/api/v1/ws/broadcast', {
      method: 'POST',
      body: {
        message: (() => {
          try {
            return JSON.parse(wsForms.broadcastMessage)
          } catch {
            return { text: wsForms.broadcastMessage }
          }
        })(),
        topic: wsForms.broadcastTopic || null,
        exclude: wsForms.broadcastExclude
          .split(/[\n,;]+/)
          .map((item) => item.trim())
          .filter(Boolean),
      },
    })
    wsHttpOps.broadcast = response.data
  } catch (error) {
    addError('ws broadcast', error)
  }
}

function adminHeaders() {
  return {
    Authorization: `Bearer ${config.adminToken}`,
  }
}

async function adminLoadConnections() {
  try {
    const response = await apiRequest('websocket', '/api/v1/admin/connections', {
      query: {
        detailed: adminForms.detailedConnections,
      },
      headers: adminHeaders(),
    })
    adminOps.connections = response.data
  } catch (error) {
    addError('admin connections', error)
  }
}

async function adminLoadConnectionDetails() {
  if (!adminForms.connectionId) return
  try {
    const response = await apiRequest('websocket', `/api/v1/admin/connections/${encodeURIComponent(adminForms.connectionId)}`, {
      query: {
        include_history: adminForms.includeHistory,
        history_limit: adminForms.historyLimit,
      },
      headers: adminHeaders(),
    })
    adminOps.connectionDetails = response.data
  } catch (error) {
    addError('admin connection details', error)
  }
}

async function adminDisconnectConnection() {
  if (!adminForms.connectionId) return
  try {
    const response = await apiRequest('websocket', `/api/v1/admin/connections/${encodeURIComponent(adminForms.connectionId)}`, {
      method: 'DELETE',
      headers: adminHeaders(),
    })
    adminOps.connectionDetails = response.data
  } catch (error) {
    addError('admin disconnect', error)
  }
}

async function adminLoadSessions() {
  try {
    const response = await apiRequest('websocket', '/api/v1/admin/sessions', {
      query: {
        status: adminForms.adminSessionStatus || null,
        limit: 50,
        offset: 0,
      },
      headers: adminHeaders(),
    })
    adminOps.sessions = response.data
  } catch (error) {
    addError('admin sessions', error)
  }
}

async function adminLoadSessionStats() {
  try {
    const response = await apiRequest('websocket', '/api/v1/admin/sessions/stats', {
      query: {
        hours: 24,
      },
      headers: adminHeaders(),
    })
    adminOps.sessionStats = response.data
  } catch (error) {
    addError('admin session stats', error)
  }
}

async function adminRunCleanup() {
  try {
    const response = await apiRequest('websocket', '/api/v1/admin/system/cleanup', {
      method: 'POST',
      query: {
        cleanup_type: adminForms.adminCleanupType,
      },
      headers: adminHeaders(),
    })
    adminOps.cleanup = response.data
  } catch (error) {
    addError('admin cleanup', error)
  }
}

async function adminLoadSystemInfo() {
  try {
    const response = await apiRequest('websocket', '/api/v1/admin/system/info', {
      headers: adminHeaders(),
    })
    adminOps.systemInfo = response.data
  } catch (error) {
    addError('admin system info', error)
  }
}

onBeforeUnmount(() => {
  stopStatusStream()
  stopNotifications()
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="WebSocket Ops" subtitle="Realtime channels + HTTP session/connection management + admin endpoints." />

    <section class="panel p-4">
      <h3 class="panel-title text-base">Realtime channels</h3>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="startStatusStream">Start /ws/status</button>
        <button class="btn-secondary" @click="stopStatusStream">Stop status stream</button>
        <button class="btn-primary" @click="startNotifications">Start /ws/notifications</button>
        <button class="btn-secondary" @click="unsubscribeNotifications">Unsubscribe notifications</button>
        <button class="btn-secondary" @click="stopNotifications">Stop notifications</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Status Stream Payload" :value="statusStream" />
        <JsonBlock title="Notifications Log" :value="notificationsLog" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">HTTP WebSocket endpoints</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">Sessions limit</span><input v-model.number="wsForms.sessionsLimit" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Sessions offset</span><input v-model.number="wsForms.sessionsOffset" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Session ID</span><input v-model="wsForms.sessionId" class="form-input" /></label>
      </div>

      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="loadSessions">GET sessions</button>
        <button class="btn-secondary" @click="loadSessionDetails">GET session</button>
        <button class="btn-secondary" @click="cancelSession">POST cancel session</button>
        <button class="btn-danger" @click="deleteSession">DELETE session</button>
        <button class="btn-secondary" @click="loadConnections">GET connections</button>
      </div>

      <div class="mt-3 grid gap-3">
        <label class="text-sm"><span class="mb-1 block">Broadcast topic (optional)</span><input v-model="wsForms.broadcastTopic" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Exclude connection IDs CSV</span><input v-model="wsForms.broadcastExclude" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Broadcast message JSON</span><textarea v-model="wsForms.broadcastMessage" class="form-input h-20 font-mono"></textarea></label>
        <button class="btn-primary w-fit" @click="broadcastMessage">POST broadcast</button>
      </div>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Sessions" :value="wsHttpOps.sessions" />
        <JsonBlock title="Session details" :value="wsHttpOps.sessionDetails" />
        <JsonBlock title="Connections" :value="wsHttpOps.connections" />
        <JsonBlock title="Broadcast" :value="wsHttpOps.broadcast" />
        <JsonBlock title="Cancel session" :value="wsHttpOps.cancel" />
        <JsonBlock title="Delete session" :value="wsHttpOps.remove" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Admin endpoints</h3>
      <p class="mt-1 text-sm text-slate-600">Токен берется из Backend Config. Требуется для `/api/v1/admin/*`.</p>

      <div class="mt-3 grid gap-3 md:grid-cols-2 lg:grid-cols-4">
        <label class="inline-flex items-center gap-2 text-sm lg:col-span-2">
          <input v-model="adminForms.detailedConnections" type="checkbox" class="h-4 w-4" /> Detailed connections
        </label>
        <label class="text-sm">
          <span class="mb-1 block">Connection ID</span>
          <input v-model="adminForms.connectionId" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block">Session status filter</span>
          <input v-model="adminForms.adminSessionStatus" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block">Cleanup type</span>
          <select v-model="adminForms.adminCleanupType" class="form-input">
            <option value="all">all</option>
            <option value="sessions">sessions</option>
            <option value="connections">connections</option>
            <option value="analyses">analyses</option>
          </select>
        </label>
        <label class="inline-flex items-center gap-2 text-sm">
          <input v-model="adminForms.includeHistory" type="checkbox" class="h-4 w-4" /> Include history
        </label>
        <label class="text-sm">
          <span class="mb-1 block">History limit</span>
          <input v-model.number="adminForms.historyLimit" type="number" class="form-input" />
        </label>
      </div>

      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="adminLoadConnections">Admin connections</button>
        <button class="btn-secondary" @click="adminLoadConnectionDetails">Admin connection details</button>
        <button class="btn-danger" @click="adminDisconnectConnection">Admin disconnect</button>
        <button class="btn-secondary" @click="adminLoadSessions">Admin sessions</button>
        <button class="btn-secondary" @click="adminLoadSessionStats">Admin session stats</button>
        <button class="btn-secondary" @click="adminRunCleanup">Admin cleanup</button>
        <button class="btn-secondary" @click="adminLoadSystemInfo">Admin system info</button>
      </div>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Admin connections" :value="adminOps.connections" />
        <JsonBlock title="Admin connection details" :value="adminOps.connectionDetails" />
        <JsonBlock title="Admin sessions" :value="adminOps.sessions" />
        <JsonBlock title="Admin session stats" :value="adminOps.sessionStats" />
        <JsonBlock title="Admin cleanup" :value="adminOps.cleanup" />
        <JsonBlock title="Admin system info" :value="adminOps.systemInfo" />
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
