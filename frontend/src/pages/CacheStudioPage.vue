<script setup>
import { computed, reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'
import { useAuth } from '../composables/useAuth'
import { useUiPrefs } from '../composables/useUiPrefs'

const { apiRequest } = useApi()
const { isAdmin } = useAuth()
const { language } = useUiPrefs()

const errors = ref([])

const keyForm = reactive({
  key: 'demo:key',
  value: '{"hello":"world"}',
  ttl: 3600,
  tags: 'demo,ui',
})
const keyOps = reactive({
  get: null,
  set: null,
  del: null,
})

const bulkForm = reactive({
  mgetKeys: 'demo:key,another:key',
  msetItems: '{"demo:key":{"from":"ui"},"another:key":[1,2,3]}',
  msetTtl: 3600,
  msetTags: '{"demo:key":["demo"],"another:key":["list"]}',
})
const bulkOps = reactive({
  mget: null,
  mset: null,
})

const maintenanceForm = reactive({
  pattern: '*',
  invalidateTags: 'demo,ui',
})
const maintenanceOps = reactive({
  keys: null,
  clear: null,
  invalidate: null,
})

const adminForm = reactive({
  historyHours: 24,
  historyInterval: 1,
  monitorLimit: 50,
  monitorOffset: 0,
  monitorSort: 'key',
  monitorOrder: 'asc',
  configPatch: '{"default_ttl_seconds": 1800}',
})
const adminOps = reactive({
  stats: null,
  history: null,
  monitor: null,
  exportData: null,
  cleanup: null,
  flush: null,
  config: null,
})

const clusterForm = reactive({
  distributionKey: 'demo:key',
  nodeId: 'cache-node-2',
  joinId: 'cache-node-new',
  joinUrl: 'http://cache-node-new:8003',
})
const clusterOps = reactive({
  info: null,
  distribution: null,
  rebalance: null,
  nodeHealth: null,
  join: null,
  leave: null,
})
const adminLocked = computed(() => !isAdmin.value)

const messages = {
  ru: {
    subtitle: 'Низкоуровневые cache-операции, admin-контроль и endpoint-ы управления кластером.',
    adminRequired: 'Требуется роль admin',
    adminLockedHint:
      'В гостевом/пользовательском режиме доступны только операции чтения кэша. Изменение и админ/cluster операции доступны только admin.',
    singleKeyOperations: 'Операции с одиночным ключом',
    key: 'Key',
    valueJsonPlain: 'Value (JSON/plain)',
    ttlSeconds: 'TTL (секунды)',
    tagsCsv: 'Tags CSV',
    bulkOperations: 'Массовые операции',
    mgetKeysCsv: 'MGET keys CSV',
    msetItemsJson: 'MSET items JSON',
    msetTagsJson: 'MSET tags JSON',
    msetTtl: 'MSET TTL',
    maintenance: 'Maintenance',
    pattern: 'Pattern',
    invalidateTagsCsv: 'Invalidate tags CSV',
    adminEndpoints: 'Admin endpoint-ы',
    stats: 'Статистика',
    history: 'История',
    monitor: 'Мониторинг',
    cleanupTrigger: 'Запустить cleanup',
    flushConfirm: 'Flush (confirm)',
    export: 'Экспорт',
    configUpdate: 'Обновить конфиг',
    historyHours: 'Hours истории',
    historyInterval: 'Интервал истории',
    monitorLimit: 'Лимит мониторинга',
    monitorOffset: 'Смещение мониторинга',
    monitorSort: 'Сортировка мониторинга',
    monitorOrder: 'Порядок мониторинга',
    configPatchJson: 'Config patch JSON',
    clusterEndpoints: 'Cluster endpoint-ы',
    distributionKey: 'Distribution key',
    nodeId: 'Node ID',
    joinId: 'Join ID',
    joinUrl: 'Join URL',
    info: 'Информация',
    distribution: 'Распределение',
    rebalance: 'Rebalance',
    nodeHealth: 'Здоровье узла',
    joinNode: 'Добавить узел',
    leaveNode: 'Убрать узел',
    keys: 'Ключи',
    clear: 'Очистка',
    invalidate: 'Инвалидация',
    adminStats: 'Admin статистика',
    adminHistory: 'Admin история',
    adminMonitor: 'Admin мониторинг',
    adminExport: 'Admin экспорт',
    adminCleanup: 'Admin cleanup',
    adminFlush: 'Admin flush',
    adminConfig: 'Admin конфиг',
    clusterInfo: 'Cluster информация',
    join: 'Добавление',
    leave: 'Удаление',
    errors: 'Ошибки',
    unknownError: 'неизвестная ошибка',
  },
  en: {
    subtitle: 'Low-level cache operations, admin controls and cluster management endpoints.',
    adminRequired: 'Admin role is required',
    adminLockedHint:
      'In guest/user mode only cache read operations are available. Mutations and admin/cluster operations are available only for admin role.',
    singleKeyOperations: 'Single Key Operations',
    key: 'Key',
    valueJsonPlain: 'Value (JSON/plain)',
    ttlSeconds: 'TTL seconds',
    tagsCsv: 'Tags CSV',
    bulkOperations: 'Bulk Operations',
    mgetKeysCsv: 'MGET keys CSV',
    msetItemsJson: 'MSET items JSON',
    msetTagsJson: 'MSET tags JSON',
    msetTtl: 'MSET TTL',
    maintenance: 'Maintenance',
    pattern: 'Pattern',
    invalidateTagsCsv: 'Invalidate tags CSV',
    adminEndpoints: 'Admin Endpoints',
    stats: 'Stats',
    history: 'History',
    monitor: 'Monitor',
    cleanupTrigger: 'Cleanup Trigger',
    flushConfirm: 'Flush (confirm)',
    export: 'Export',
    configUpdate: 'Config Update',
    historyHours: 'History hours',
    historyInterval: 'History interval',
    monitorLimit: 'Monitor limit',
    monitorOffset: 'Monitor offset',
    monitorSort: 'Monitor sort',
    monitorOrder: 'Monitor order',
    configPatchJson: 'Config patch JSON',
    clusterEndpoints: 'Cluster Endpoints',
    distributionKey: 'Distribution key',
    nodeId: 'Node ID',
    joinId: 'Join ID',
    joinUrl: 'Join URL',
    info: 'Info',
    distribution: 'Distribution',
    rebalance: 'Rebalance',
    nodeHealth: 'Node Health',
    joinNode: 'Join Node',
    leaveNode: 'Leave Node',
    keys: 'Keys',
    clear: 'Clear',
    invalidate: 'Invalidate',
    adminStats: 'Admin stats',
    adminHistory: 'Admin history',
    adminMonitor: 'Admin monitor',
    adminExport: 'Admin export',
    adminCleanup: 'Admin cleanup',
    adminFlush: 'Admin flush',
    adminConfig: 'Admin config',
    clusterInfo: 'Cluster info',
    join: 'Join',
    leave: 'Leave',
    errors: 'Errors',
    unknownError: 'unknown error',
  },
}

function t(key) {
  return messages[language.value]?.[key] || messages.en[key] || key
}

function parseCsv(value) {
  return value
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

function tryJson(input, fallback = null) {
  try {
    return JSON.parse(input)
  } catch {
    return fallback
  }
}

function addError(scope, error) {
  const detail = error?.data?.detail || error?.message || t('unknownError')
  errors.value.unshift(`[${scope}] ${detail}`)
}

function requireAdminPermission(scope) {
  if (!adminLocked.value) return true
  addError(scope, { message: t('adminRequired') })
  return false
}

async function getKey() {
  try {
    const response = await apiRequest('cache', `/api/v1/cache/${encodeURIComponent(keyForm.key)}`)
    keyOps.get = response.data
  } catch (error) {
    addError('cache get', error)
  }
}

async function setKey() {
  if (!requireAdminPermission('cache set')) return
  try {
    const response = await apiRequest('cache', `/api/v1/cache/${encodeURIComponent(keyForm.key)}`, {
      method: 'PUT',
      body: {
        value: tryJson(keyForm.value, keyForm.value),
        ttl: Number(keyForm.ttl),
        tags: parseCsv(keyForm.tags),
      },
    })
    keyOps.set = response.data
  } catch (error) {
    addError('cache set', error)
  }
}

async function deleteKey() {
  if (!requireAdminPermission('cache delete')) return
  try {
    const response = await apiRequest('cache', `/api/v1/cache/${encodeURIComponent(keyForm.key)}`, {
      method: 'DELETE',
    })
    keyOps.del = response.data
  } catch (error) {
    addError('cache delete', error)
  }
}

async function runMget() {
  try {
    const response = await apiRequest('cache', '/api/v1/cache/mget', {
      method: 'POST',
      body: {
        keys: parseCsv(bulkForm.mgetKeys),
      },
    })
    bulkOps.mget = response.data
  } catch (error) {
    addError('cache mget', error)
  }
}

async function runMset() {
  if (!requireAdminPermission('cache mset')) return
  try {
    const items = tryJson(bulkForm.msetItems, {})
    const tags = tryJson(bulkForm.msetTags, {})

    const response = await apiRequest('cache', '/api/v1/cache/mset', {
      method: 'POST',
      body: {
        items,
        ttl: Number(bulkForm.msetTtl),
        tags,
      },
    })
    bulkOps.mset = response.data
  } catch (error) {
    addError('cache mset', error)
  }
}

async function listKeys() {
  try {
    const response = await apiRequest('cache', '/api/v1/cache/keys', {
      query: {
        pattern: maintenanceForm.pattern,
      },
    })
    maintenanceOps.keys = response.data
  } catch (error) {
    addError('cache keys', error)
  }
}

async function clearByPattern() {
  if (!requireAdminPermission('cache clear')) return
  try {
    const response = await apiRequest('cache', '/api/v1/cache/clear', {
      method: 'DELETE',
      query: {
        pattern: maintenanceForm.pattern,
      },
    })
    maintenanceOps.clear = response.data
  } catch (error) {
    addError('cache clear', error)
  }
}

async function invalidateByTags() {
  if (!requireAdminPermission('cache invalidate')) return
  try {
    const response = await apiRequest('cache', '/api/v1/cache/invalidate/tags', {
      method: 'POST',
      body: {
        tags: parseCsv(maintenanceForm.invalidateTags),
      },
    })
    maintenanceOps.invalidate = response.data
  } catch (error) {
    addError('cache invalidate', error)
  }
}

async function loadAdminStats() {
  if (!requireAdminPermission('admin stats')) return
  try {
    const response = await apiRequest('cache', '/api/v1/admin/stats')
    adminOps.stats = response.data
  } catch (error) {
    addError('admin stats', error)
  }
}

async function loadAdminHistory() {
  if (!requireAdminPermission('admin history')) return
  try {
    const response = await apiRequest('cache', '/api/v1/admin/stats/history', {
      query: {
        hours: adminForm.historyHours,
        interval: adminForm.historyInterval,
      },
    })
    adminOps.history = response.data
  } catch (error) {
    addError('admin history', error)
  }
}

async function loadMonitor() {
  if (!requireAdminPermission('admin monitor')) return
  try {
    const response = await apiRequest('cache', '/api/v1/admin/monitor/keys', {
      query: {
        limit: adminForm.monitorLimit,
        offset: adminForm.monitorOffset,
        sort_by: adminForm.monitorSort,
        order: adminForm.monitorOrder,
      },
    })
    adminOps.monitor = response.data
  } catch (error) {
    addError('admin monitor', error)
  }
}

async function runAdminCleanup() {
  if (!requireAdminPermission('admin cleanup')) return
  try {
    const response = await apiRequest('cache', '/api/v1/admin/cleanup/trigger', {
      method: 'POST',
    })
    adminOps.cleanup = response.data
  } catch (error) {
    addError('admin cleanup', error)
  }
}

async function runAdminFlush() {
  if (!requireAdminPermission('admin flush')) return
  try {
    const response = await apiRequest('cache', '/api/v1/admin/flush', {
      method: 'POST',
      body: {
        confirm: true,
      },
    })
    adminOps.flush = response.data
  } catch (error) {
    addError('admin flush', error)
  }
}

async function runAdminConfigPatch() {
  if (!requireAdminPermission('admin config')) return
  try {
    const response = await apiRequest('cache', '/api/v1/admin/config/update', {
      method: 'POST',
      body: tryJson(adminForm.configPatch, {}),
    })
    adminOps.config = response.data
  } catch (error) {
    addError('admin config', error)
  }
}

async function loadAdminExport() {
  if (!requireAdminPermission('admin export')) return
  try {
    const response = await apiRequest('cache', '/api/v1/admin/export', {
      query: {
        format: 'json',
        limit: 200,
      },
    })
    adminOps.exportData = response.data
  } catch (error) {
    addError('admin export', error)
  }
}

async function loadClusterInfo() {
  if (!requireAdminPermission('cluster info')) return
  try {
    const response = await apiRequest('cache', '/api/v1/cluster/info')
    clusterOps.info = response.data
  } catch (error) {
    addError('cluster info', error)
  }
}

async function loadDistribution() {
  if (!requireAdminPermission('cluster distribution')) return
  try {
    const response = await apiRequest('cache', '/api/v1/cluster/distribution', {
      query: {
        key: clusterForm.distributionKey,
      },
    })
    clusterOps.distribution = response.data
  } catch (error) {
    addError('cluster distribution', error)
  }
}

async function runRebalance() {
  if (!requireAdminPermission('cluster rebalance')) return
  try {
    const response = await apiRequest('cache', '/api/v1/cluster/rebalance', {
      method: 'POST',
    })
    clusterOps.rebalance = response.data
  } catch (error) {
    addError('cluster rebalance', error)
  }
}

async function loadNodeHealth() {
  if (!requireAdminPermission('cluster node health')) return
  try {
    const response = await apiRequest('cache', `/api/v1/cluster/nodes/${encodeURIComponent(clusterForm.nodeId)}/health`)
    clusterOps.nodeHealth = response.data
  } catch (error) {
    addError('cluster node health', error)
  }
}

async function joinNode() {
  if (!requireAdminPermission('cluster join')) return
  try {
    const response = await apiRequest('cache', '/api/v1/cluster/nodes/join', {
      method: 'POST',
      body: {
        id: clusterForm.joinId,
        url: clusterForm.joinUrl,
        version: '1.0.0',
      },
    })
    clusterOps.join = response.data
  } catch (error) {
    addError('cluster join', error)
  }
}

async function leaveNode() {
  if (!requireAdminPermission('cluster leave')) return
  try {
    const response = await apiRequest('cache', '/api/v1/cluster/nodes/leave', {
      method: 'POST',
      body: {
        id: clusterForm.nodeId,
      },
    })
    clusterOps.leave = response.data
  } catch (error) {
    addError('cluster leave', error)
  }
}
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Cache Studio" :subtitle="t('subtitle')" />

    <section v-if="adminLocked" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('adminLockedHint') }}
      </p>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('singleKeyOperations') }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <label class="text-sm lg:col-span-2"><span class="mb-1 block">{{ t('key') }}</span><input v-model="keyForm.key" class="form-input font-mono" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('valueJsonPlain') }}</span><textarea v-model="keyForm.value" class="form-input h-24 font-mono"></textarea></label>
        <div class="space-y-3">
          <label class="text-sm"><span class="mb-1 block">{{ t('ttlSeconds') }}</span><input v-model.number="keyForm.ttl" type="number" class="form-input" /></label>
          <label class="text-sm"><span class="mb-1 block">{{ t('tagsCsv') }}</span><input v-model="keyForm.tags" class="form-input" /></label>
        </div>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" :disabled="adminLocked" @click="setKey">PUT /cache/{key}</button>
        <button class="btn-secondary" @click="getKey">GET /cache/{key}</button>
        <button class="btn-danger" :disabled="adminLocked" @click="deleteKey">DELETE /cache/{key}</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-3">
        <JsonBlock title="GET" :value="keyOps.get" />
        <JsonBlock title="PUT" :value="keyOps.set" />
        <JsonBlock title="DELETE" :value="keyOps.del" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('bulkOperations') }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <label class="text-sm">
          <span class="mb-1 block">{{ t('mgetKeysCsv') }}</span>
          <textarea v-model="bulkForm.mgetKeys" class="form-input h-20 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('msetItemsJson') }}</span>
          <textarea v-model="bulkForm.msetItems" class="form-input h-20 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('msetTagsJson') }}</span>
          <textarea v-model="bulkForm.msetTags" class="form-input h-20 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('msetTtl') }}</span>
          <input v-model.number="bulkForm.msetTtl" type="number" class="form-input" />
        </label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="runMget">POST /cache/mget</button>
        <button class="btn-secondary" :disabled="adminLocked" @click="runMset">POST /cache/mset</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="MGET" :value="bulkOps.mget" />
        <JsonBlock title="MSET" :value="bulkOps.mset" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('maintenance') }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <label class="text-sm"><span class="mb-1 block">{{ t('pattern') }}</span><input v-model="maintenanceForm.pattern" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('invalidateTagsCsv') }}</span><input v-model="maintenanceForm.invalidateTags" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="listKeys">GET /cache/keys</button>
        <button class="btn-secondary" :disabled="adminLocked" @click="clearByPattern">DELETE /cache/clear</button>
        <button class="btn-secondary" :disabled="adminLocked" @click="invalidateByTags">POST /cache/invalidate/tags</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-3">
        <JsonBlock :title="t('keys')" :value="maintenanceOps.keys" />
        <JsonBlock :title="t('clear')" :value="maintenanceOps.clear" />
        <JsonBlock :title="t('invalidate')" :value="maintenanceOps.invalidate" />
      </div>
    </section>

    <section v-if="!adminLocked" class="panel p-4">
      <h3 class="panel-title text-base">{{ t('adminEndpoints') }}</h3>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="loadAdminStats">{{ t('stats') }}</button>
        <button class="btn-secondary" @click="loadAdminHistory">{{ t('history') }}</button>
        <button class="btn-secondary" @click="loadMonitor">{{ t('monitor') }}</button>
        <button class="btn-secondary" @click="runAdminCleanup">{{ t('cleanupTrigger') }}</button>
        <button class="btn-danger" @click="runAdminFlush">{{ t('flushConfirm') }}</button>
        <button class="btn-secondary" @click="loadAdminExport">{{ t('export') }}</button>
        <button class="btn-secondary" @click="runAdminConfigPatch">{{ t('configUpdate') }}</button>
      </div>

      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">{{ t('historyHours') }}</span><input v-model.number="adminForm.historyHours" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('historyInterval') }}</span><input v-model.number="adminForm.historyInterval" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('monitorLimit') }}</span><input v-model.number="adminForm.monitorLimit" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('monitorOffset') }}</span><input v-model.number="adminForm.monitorOffset" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('monitorSort') }}</span><input v-model="adminForm.monitorSort" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('monitorOrder') }}</span><input v-model="adminForm.monitorOrder" class="form-input" /></label>
      </div>
      <label class="mt-3 block text-sm">
        <span class="mb-1 block">{{ t('configPatchJson') }}</span>
        <textarea v-model="adminForm.configPatch" class="form-input h-20 font-mono"></textarea>
      </label>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('adminStats')" :value="adminOps.stats" />
        <JsonBlock :title="t('adminHistory')" :value="adminOps.history" />
        <JsonBlock :title="t('adminMonitor')" :value="adminOps.monitor" />
        <JsonBlock :title="t('adminExport')" :value="adminOps.exportData" />
        <JsonBlock :title="t('adminCleanup')" :value="adminOps.cleanup" />
        <JsonBlock :title="t('adminFlush')" :value="adminOps.flush" />
        <JsonBlock :title="t('adminConfig')" :value="adminOps.config" />
      </div>
    </section>

    <section v-if="!adminLocked" class="panel p-4">
      <h3 class="panel-title text-base">{{ t('clusterEndpoints') }}</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">{{ t('distributionKey') }}</span><input v-model="clusterForm.distributionKey" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('nodeId') }}</span><input v-model="clusterForm.nodeId" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('joinId') }}</span><input v-model="clusterForm.joinId" class="form-input" /></label>
        <label class="text-sm md:col-span-2"><span class="mb-1 block">{{ t('joinUrl') }}</span><input v-model="clusterForm.joinUrl" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="loadClusterInfo">{{ t('info') }}</button>
        <button class="btn-secondary" @click="loadDistribution">{{ t('distribution') }}</button>
        <button class="btn-secondary" @click="runRebalance">{{ t('rebalance') }}</button>
        <button class="btn-secondary" @click="loadNodeHealth">{{ t('nodeHealth') }}</button>
        <button class="btn-secondary" @click="joinNode">{{ t('joinNode') }}</button>
        <button class="btn-danger" @click="leaveNode">{{ t('leaveNode') }}</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('clusterInfo')" :value="clusterOps.info" />
        <JsonBlock :title="t('distribution')" :value="clusterOps.distribution" />
        <JsonBlock :title="t('rebalance')" :value="clusterOps.rebalance" />
        <JsonBlock :title="t('nodeHealth')" :value="clusterOps.nodeHealth" />
        <JsonBlock :title="t('join')" :value="clusterOps.join" />
        <JsonBlock :title="t('leave')" :value="clusterOps.leave" />
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
