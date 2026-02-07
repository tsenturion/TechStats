<script setup>
import { reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'

const { apiRequest } = useApi()

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
  const detail = error?.data?.detail || error?.message || 'unknown error'
  errors.value.unshift(`[${scope}] ${detail}`)
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
  try {
    const response = await apiRequest('cache', '/api/v1/admin/stats')
    adminOps.stats = response.data
  } catch (error) {
    addError('admin stats', error)
  }
}

async function loadAdminHistory() {
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
  try {
    const response = await apiRequest('cache', '/api/v1/cluster/info')
    clusterOps.info = response.data
  } catch (error) {
    addError('cluster info', error)
  }
}

async function loadDistribution() {
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
  try {
    const response = await apiRequest('cache', `/api/v1/cluster/nodes/${encodeURIComponent(clusterForm.nodeId)}/health`)
    clusterOps.nodeHealth = response.data
  } catch (error) {
    addError('cluster node health', error)
  }
}

async function joinNode() {
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
    <SectionHeader title="Cache Studio" subtitle="Low-level cache operations, admin controls and cluster management endpoints." />

    <section class="panel p-4">
      <h3 class="panel-title text-base">Single Key Operations</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <label class="text-sm lg:col-span-2"><span class="mb-1 block">Key</span><input v-model="keyForm.key" class="form-input font-mono" /></label>
        <label class="text-sm"><span class="mb-1 block">Value (JSON/plain)</span><textarea v-model="keyForm.value" class="form-input h-24 font-mono"></textarea></label>
        <div class="space-y-3">
          <label class="text-sm"><span class="mb-1 block">TTL seconds</span><input v-model.number="keyForm.ttl" type="number" class="form-input" /></label>
          <label class="text-sm"><span class="mb-1 block">Tags CSV</span><input v-model="keyForm.tags" class="form-input" /></label>
        </div>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="setKey">PUT /cache/{key}</button>
        <button class="btn-secondary" @click="getKey">GET /cache/{key}</button>
        <button class="btn-danger" @click="deleteKey">DELETE /cache/{key}</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-3">
        <JsonBlock title="GET" :value="keyOps.get" />
        <JsonBlock title="PUT" :value="keyOps.set" />
        <JsonBlock title="DELETE" :value="keyOps.del" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Bulk Operations</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <label class="text-sm">
          <span class="mb-1 block">MGET keys CSV</span>
          <textarea v-model="bulkForm.mgetKeys" class="form-input h-20 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">MSET items JSON</span>
          <textarea v-model="bulkForm.msetItems" class="form-input h-20 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">MSET tags JSON</span>
          <textarea v-model="bulkForm.msetTags" class="form-input h-20 font-mono"></textarea>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">MSET TTL</span>
          <input v-model.number="bulkForm.msetTtl" type="number" class="form-input" />
        </label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="runMget">POST /cache/mget</button>
        <button class="btn-secondary" @click="runMset">POST /cache/mset</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="MGET" :value="bulkOps.mget" />
        <JsonBlock title="MSET" :value="bulkOps.mset" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Maintenance</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <label class="text-sm"><span class="mb-1 block">Pattern</span><input v-model="maintenanceForm.pattern" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Invalidate tags CSV</span><input v-model="maintenanceForm.invalidateTags" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="listKeys">GET /cache/keys</button>
        <button class="btn-secondary" @click="clearByPattern">DELETE /cache/clear</button>
        <button class="btn-secondary" @click="invalidateByTags">POST /cache/invalidate/tags</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-3">
        <JsonBlock title="Keys" :value="maintenanceOps.keys" />
        <JsonBlock title="Clear" :value="maintenanceOps.clear" />
        <JsonBlock title="Invalidate" :value="maintenanceOps.invalidate" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Admin Endpoints</h3>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="loadAdminStats">Stats</button>
        <button class="btn-secondary" @click="loadAdminHistory">History</button>
        <button class="btn-secondary" @click="loadMonitor">Monitor</button>
        <button class="btn-secondary" @click="runAdminCleanup">Cleanup Trigger</button>
        <button class="btn-danger" @click="runAdminFlush">Flush (confirm)</button>
        <button class="btn-secondary" @click="loadAdminExport">Export</button>
        <button class="btn-secondary" @click="runAdminConfigPatch">Config Update</button>
      </div>

      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">History hours</span><input v-model.number="adminForm.historyHours" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">History interval</span><input v-model.number="adminForm.historyInterval" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Monitor limit</span><input v-model.number="adminForm.monitorLimit" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Monitor offset</span><input v-model.number="adminForm.monitorOffset" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Monitor sort</span><input v-model="adminForm.monitorSort" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Monitor order</span><input v-model="adminForm.monitorOrder" class="form-input" /></label>
      </div>
      <label class="mt-3 block text-sm">
        <span class="mb-1 block">Config patch JSON</span>
        <textarea v-model="adminForm.configPatch" class="form-input h-20 font-mono"></textarea>
      </label>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Admin stats" :value="adminOps.stats" />
        <JsonBlock title="Admin history" :value="adminOps.history" />
        <JsonBlock title="Admin monitor" :value="adminOps.monitor" />
        <JsonBlock title="Admin export" :value="adminOps.exportData" />
        <JsonBlock title="Admin cleanup" :value="adminOps.cleanup" />
        <JsonBlock title="Admin flush" :value="adminOps.flush" />
        <JsonBlock title="Admin config" :value="adminOps.config" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Cluster Endpoints</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">Distribution key</span><input v-model="clusterForm.distributionKey" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Node ID</span><input v-model="clusterForm.nodeId" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Join ID</span><input v-model="clusterForm.joinId" class="form-input" /></label>
        <label class="text-sm md:col-span-2"><span class="mb-1 block">Join URL</span><input v-model="clusterForm.joinUrl" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="loadClusterInfo">Info</button>
        <button class="btn-secondary" @click="loadDistribution">Distribution</button>
        <button class="btn-secondary" @click="runRebalance">Rebalance</button>
        <button class="btn-secondary" @click="loadNodeHealth">Node Health</button>
        <button class="btn-secondary" @click="joinNode">Join Node</button>
        <button class="btn-danger" @click="leaveNode">Leave Node</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Cluster info" :value="clusterOps.info" />
        <JsonBlock title="Distribution" :value="clusterOps.distribution" />
        <JsonBlock title="Rebalance" :value="clusterOps.rebalance" />
        <JsonBlock title="Node health" :value="clusterOps.nodeHealth" />
        <JsonBlock title="Join" :value="clusterOps.join" />
        <JsonBlock title="Leave" :value="clusterOps.leave" />
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
