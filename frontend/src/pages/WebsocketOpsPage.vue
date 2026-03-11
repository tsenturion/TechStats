<script setup>
import { computed, onBeforeUnmount, reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'
import { useAuth } from '../composables/useAuth'
import { useUiPrefs } from '../composables/useUiPrefs'

const { apiRequest, wsUrl, config } = useApi()
const { isUserOrAdmin, isAdmin } = useAuth()
const { language } = useUiPrefs()

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
const userLocked = computed(() => !isUserOrAdmin.value)
const adminLocked = computed(() => !isAdmin.value)

const messages = {
  ru: {
    subtitle: 'Realtime-каналы + HTTP управление сессиями/соединениями + admin endpoint-ы.',
    userRequired: 'Требуется вход с ролью user/admin',
    adminRequired: 'Требуется роль admin',
    userLockedHint:
      'В гостевом режиме доступны только пассивные realtime-каналы. Управляющие HTTP операции требуют роль user/admin.',
    adminLockedHint: "Админские WebSocket endpoint'ы доступны только admin.",
    adminTokenHint: 'Токен берется из Backend Config. Требуется для `/api/v1/admin/*`.',
    realtimeChannels: 'Realtime-каналы',
    startStatus: 'Запустить /ws/status',
    stopStatus: 'Остановить status stream',
    startNotifications: 'Запустить /ws/notifications',
    unsubscribeNotifications: 'Отписаться от notifications',
    stopNotifications: 'Остановить notifications',
    statusPayload: 'Payload потока статуса',
    notificationsLog: 'Лог notifications',
    httpWsEndpoints: 'HTTP endpoint-ы WebSocket Service',
    sessionsLimit: 'Лимит сессий',
    sessionsOffset: 'Смещение сессий',
    sessionId: 'Session ID',
    getSessions: 'GET sessions',
    getSession: 'GET session',
    postCancelSession: 'POST cancel session',
    deleteSession: 'DELETE session',
    getConnections: 'GET connections',
    broadcastTopic: 'Broadcast topic (опционально)',
    excludeIds: 'Исключить connection ID (CSV)',
    broadcastMessageJson: 'Broadcast message JSON',
    postBroadcast: 'POST broadcast',
    sessions: 'Сессии',
    sessionDetails: 'Детали сессии',
    connections: 'Соединения',
    broadcast: 'Broadcast',
    cancelSession: 'Отмена сессии',
    deleteSessionResult: 'Удаление сессии',
    adminEndpoints: 'Admin endpoint-ы',
    detailedConnections: 'Детализированные соединения',
    connectionId: 'Connection ID',
    sessionStatusFilter: 'Фильтр статуса сессий',
    cleanupType: 'Тип cleanup',
    includeHistory: 'Включить историю',
    historyLimit: 'Лимит истории',
    adminConnections: 'Admin connections',
    adminConnectionDetails: 'Admin connection details',
    adminDisconnect: 'Admin disconnect',
    adminSessions: 'Admin sessions',
    adminSessionStats: 'Admin session stats',
    adminCleanup: 'Admin cleanup',
    adminSystemInfo: 'Admin system info',
    errors: 'Ошибки',
    statusDisconnected: 'status stream отключен',
    notificationsDisconnected: 'notifications stream отключен',
    unknownError: 'неизвестная ошибка',
  },
  en: {
    subtitle: 'Realtime channels + HTTP session/connection management + admin endpoints.',
    userRequired: 'Login with user/admin role is required',
    adminRequired: 'Admin role is required',
    userLockedHint:
      'In guest mode only passive realtime channels are available. HTTP control operations require user/admin role.',
    adminLockedHint: "Admin WebSocket endpoints are available only for admin role.",
    adminTokenHint: 'Token is taken from Backend Config. Required for `/api/v1/admin/*`.',
    realtimeChannels: 'Realtime channels',
    startStatus: 'Start /ws/status',
    stopStatus: 'Stop status stream',
    startNotifications: 'Start /ws/notifications',
    unsubscribeNotifications: 'Unsubscribe notifications',
    stopNotifications: 'Stop notifications',
    statusPayload: 'Status Stream Payload',
    notificationsLog: 'Notifications Log',
    httpWsEndpoints: 'HTTP WebSocket endpoints',
    sessionsLimit: 'Sessions limit',
    sessionsOffset: 'Sessions offset',
    sessionId: 'Session ID',
    getSessions: 'GET sessions',
    getSession: 'GET session',
    postCancelSession: 'POST cancel session',
    deleteSession: 'DELETE session',
    getConnections: 'GET connections',
    broadcastTopic: 'Broadcast topic (optional)',
    excludeIds: 'Exclude connection IDs CSV',
    broadcastMessageJson: 'Broadcast message JSON',
    postBroadcast: 'POST broadcast',
    sessions: 'Sessions',
    sessionDetails: 'Session details',
    connections: 'Connections',
    broadcast: 'Broadcast',
    cancelSession: 'Cancel session',
    deleteSessionResult: 'Delete session',
    adminEndpoints: 'Admin endpoints',
    detailedConnections: 'Detailed connections',
    connectionId: 'Connection ID',
    sessionStatusFilter: 'Session status filter',
    cleanupType: 'Cleanup type',
    includeHistory: 'Include history',
    historyLimit: 'History limit',
    adminConnections: 'Admin connections',
    adminConnectionDetails: 'Admin connection details',
    adminDisconnect: 'Admin disconnect',
    adminSessions: 'Admin sessions',
    adminSessionStats: 'Admin session stats',
    adminCleanup: 'Admin cleanup',
    adminSystemInfo: 'Admin system info',
    errors: 'Errors',
    statusDisconnected: 'status stream disconnected',
    notificationsDisconnected: 'notifications stream disconnected',
    unknownError: 'unknown error',
  },
}

function t(key) {
  return messages[language.value]?.[key] || messages.en[key] || key
}

function addError(scope, error) {
  const detail = error?.data?.detail || error?.message || t('unknownError')
  errors.value.unshift(`[${scope}] ${detail}`)
}

function requireUserPermission(scope) {
  if (!userLocked.value) return true
  addError(scope, { message: t('userRequired') })
  return false
}

function requireAdminPermission(scope) {
  if (!adminLocked.value) return true
  addError(scope, { message: t('adminRequired') })
  return false
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
    addError('ws/status', { message: t('statusDisconnected') })
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
    addError('ws/notifications', { message: t('notificationsDisconnected') })
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
  if (!requireUserPermission('ws sessions')) return
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
  if (!requireUserPermission('ws session details')) return
  if (!wsForms.sessionId) return
  try {
    const response = await apiRequest('websocket', `/api/v1/ws/sessions/${encodeURIComponent(wsForms.sessionId)}`)
    wsHttpOps.sessionDetails = response.data
  } catch (error) {
    addError('ws session details', error)
  }
}

async function cancelSession() {
  if (!requireUserPermission('ws session cancel')) return
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
  if (!requireUserPermission('ws session delete')) return
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
  if (!requireUserPermission('ws connections')) return
  try {
    const response = await apiRequest('websocket', '/api/v1/ws/connections')
    wsHttpOps.connections = response.data
  } catch (error) {
    addError('ws connections', error)
  }
}

async function broadcastMessage() {
  if (!requireUserPermission('ws broadcast')) return
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
  if (!requireAdminPermission('admin connections')) return
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
  if (!requireAdminPermission('admin connection details')) return
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
  if (!requireAdminPermission('admin disconnect')) return
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
  if (!requireAdminPermission('admin sessions')) return
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
  if (!requireAdminPermission('admin session stats')) return
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
  if (!requireAdminPermission('admin cleanup')) return
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
  if (!requireAdminPermission('admin system info')) return
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
    <SectionHeader title="WebSocket Ops" :subtitle="t('subtitle')" />

    <section v-if="userLocked" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('userLockedHint') }}
      </p>
    </section>
    <section v-if="adminLocked" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('adminLockedHint') }}
      </p>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('realtimeChannels') }}</h3>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="startStatusStream">{{ t('startStatus') }}</button>
        <button class="btn-secondary" @click="stopStatusStream">{{ t('stopStatus') }}</button>
        <button class="btn-primary" @click="startNotifications">{{ t('startNotifications') }}</button>
        <button class="btn-secondary" @click="unsubscribeNotifications">{{ t('unsubscribeNotifications') }}</button>
        <button class="btn-secondary" @click="stopNotifications">{{ t('stopNotifications') }}</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('statusPayload')" :value="statusStream" />
        <JsonBlock :title="t('notificationsLog')" :value="notificationsLog" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('httpWsEndpoints') }}</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">{{ t('sessionsLimit') }}</span><input v-model.number="wsForms.sessionsLimit" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('sessionsOffset') }}</span><input v-model.number="wsForms.sessionsOffset" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('sessionId') }}</span><input v-model="wsForms.sessionId" class="form-input" /></label>
      </div>

      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" :disabled="userLocked" @click="loadSessions">{{ t('getSessions') }}</button>
        <button class="btn-secondary" :disabled="userLocked" @click="loadSessionDetails">{{ t('getSession') }}</button>
        <button class="btn-secondary" :disabled="userLocked" @click="cancelSession">{{ t('postCancelSession') }}</button>
        <button class="btn-danger" :disabled="userLocked" @click="deleteSession">{{ t('deleteSession') }}</button>
        <button class="btn-secondary" :disabled="userLocked" @click="loadConnections">{{ t('getConnections') }}</button>
      </div>

      <div class="mt-3 grid gap-3">
        <label class="text-sm"><span class="mb-1 block">{{ t('broadcastTopic') }}</span><input v-model="wsForms.broadcastTopic" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('excludeIds') }}</span><input v-model="wsForms.broadcastExclude" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('broadcastMessageJson') }}</span><textarea v-model="wsForms.broadcastMessage" class="form-input h-20 font-mono"></textarea></label>
        <button class="btn-primary w-fit" :disabled="userLocked" @click="broadcastMessage">{{ t('postBroadcast') }}</button>
      </div>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('sessions')" :value="wsHttpOps.sessions" />
        <JsonBlock :title="t('sessionDetails')" :value="wsHttpOps.sessionDetails" />
        <JsonBlock :title="t('connections')" :value="wsHttpOps.connections" />
        <JsonBlock :title="t('broadcast')" :value="wsHttpOps.broadcast" />
        <JsonBlock :title="t('cancelSession')" :value="wsHttpOps.cancel" />
        <JsonBlock :title="t('deleteSessionResult')" :value="wsHttpOps.remove" />
      </div>
    </section>

    <section v-if="!adminLocked" class="panel p-4">
      <h3 class="panel-title text-base">{{ t('adminEndpoints') }}</h3>
      <p class="mt-1 text-sm text-slate-600">{{ t('adminTokenHint') }}</p>

      <div class="mt-3 grid gap-3 md:grid-cols-2 lg:grid-cols-4">
        <label class="inline-flex items-center gap-2 text-sm lg:col-span-2">
          <input v-model="adminForms.detailedConnections" type="checkbox" class="h-4 w-4" /> {{ t('detailedConnections') }}
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('connectionId') }}</span>
          <input v-model="adminForms.connectionId" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('sessionStatusFilter') }}</span>
          <input v-model="adminForms.adminSessionStatus" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('cleanupType') }}</span>
          <select v-model="adminForms.adminCleanupType" class="form-input">
            <option value="all">all</option>
            <option value="sessions">sessions</option>
            <option value="connections">connections</option>
            <option value="analyses">analyses</option>
          </select>
        </label>
        <label class="inline-flex items-center gap-2 text-sm">
          <input v-model="adminForms.includeHistory" type="checkbox" class="h-4 w-4" /> {{ t('includeHistory') }}
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('historyLimit') }}</span>
          <input v-model.number="adminForms.historyLimit" type="number" class="form-input" />
        </label>
      </div>

      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="adminLoadConnections">{{ t('adminConnections') }}</button>
        <button class="btn-secondary" @click="adminLoadConnectionDetails">{{ t('adminConnectionDetails') }}</button>
        <button class="btn-danger" @click="adminDisconnectConnection">{{ t('adminDisconnect') }}</button>
        <button class="btn-secondary" @click="adminLoadSessions">{{ t('adminSessions') }}</button>
        <button class="btn-secondary" @click="adminLoadSessionStats">{{ t('adminSessionStats') }}</button>
        <button class="btn-secondary" @click="adminRunCleanup">{{ t('adminCleanup') }}</button>
        <button class="btn-secondary" @click="adminLoadSystemInfo">{{ t('adminSystemInfo') }}</button>
      </div>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('adminConnections')" :value="adminOps.connections" />
        <JsonBlock :title="t('adminConnectionDetails')" :value="adminOps.connectionDetails" />
        <JsonBlock :title="t('adminSessions')" :value="adminOps.sessions" />
        <JsonBlock :title="t('adminSessionStats')" :value="adminOps.sessionStats" />
        <JsonBlock :title="t('adminCleanup')" :value="adminOps.cleanup" />
        <JsonBlock :title="t('adminSystemInfo')" :value="adminOps.systemInfo" />
      </div>
    </section>

    <section v-if="errors.length" class="panel border-rose-200 bg-rose-50 p-4">
      <h3 class="panel-title text-base text-rose-700">{{ t('errors') }}</h3>
      <ul class="mt-2 list-disc space-y-1 pl-5 text-sm text-rose-700">
        <li v-for="item in errors" :key="item">{{ item }}</li>
      </ul>
    </section>
  </div>
</template>
