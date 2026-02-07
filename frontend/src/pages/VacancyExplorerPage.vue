<script setup>
import { reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'

const { apiRequest } = useApi()

const searchForm = reactive({
  query: 'Python developer',
  area: 113,
  page: 0,
  per_page: 20,
  exact_search: true,
})

const searchResult = ref(null)
const selectedVacancyId = ref('')
const vacancyDetails = ref(null)
const batchIdsInput = ref('')
const batchResult = ref(null)
const referenceData = reactive({
  areas: null,
  industries: null,
  professionalRoles: null,
  metro: null,
  rateLimit: null,
})
const metroCityId = ref(1)
const errors = ref([])
const loading = reactive({
  search: false,
  details: false,
  batch: false,
  ref: false,
})

function captureError(scope, error) {
  const detail = error?.data?.detail || error?.message || 'unknown error'
  errors.value.unshift(`[${scope}] ${detail}`)
}

async function runSearch() {
  loading.search = true
  try {
    const response = await apiRequest('gateway', '/api/v1/vacancies/search', {
      query: searchForm,
    })
    searchResult.value = response.data
  } catch (error) {
    captureError('search', error)
  } finally {
    loading.search = false
  }
}

async function loadVacancyDetails(id = selectedVacancyId.value) {
  if (!id) return
  loading.details = true
  try {
    const response = await apiRequest('gateway', `/api/v1/vacancies/${id}`)
    vacancyDetails.value = response.data
  } catch (error) {
    captureError('vacancy details', error)
  } finally {
    loading.details = false
  }
}

async function loadBatch() {
  const ids = batchIdsInput.value
    .split(/[\s,;]+/)
    .map((item) => item.trim())
    .filter(Boolean)

  if (!ids.length) return

  loading.batch = true
  try {
    const response = await apiRequest('gateway', '/api/v1/vacancies/batch', {
      query: {
        vacancy_ids: ids,
      },
    })
    batchResult.value = response.data
  } catch (error) {
    captureError('batch', error)
  } finally {
    loading.batch = false
  }
}

async function fetchReference(endpoint, key, query = null) {
  loading.ref = true
  try {
    const response = await apiRequest('vacancy', endpoint, { query })
    referenceData[key] = response.data
  } catch (error) {
    captureError(`reference:${key}`, error)
  } finally {
    loading.ref = false
  }
}

async function loadAllReference() {
  await Promise.all([
    fetchReference('/api/v1/areas', 'areas'),
    fetchReference('/api/v1/industries', 'industries'),
    fetchReference('/api/v1/professional-roles', 'professionalRoles'),
    fetchReference('/api/v1/rate-limit/stats', 'rateLimit'),
  ])
}

async function loadMetro() {
  await fetchReference(`/api/v1/metro/${metroCityId.value}`, 'metro')
}
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Vacancy Explorer" subtitle="Gateway proxy + direct vacancy-service endpoints (areas, metro, industries, rate limits)." />

    <section class="panel p-4">
      <h3 class="panel-title text-base">Search via API Gateway</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <label class="text-sm md:col-span-3">
          <span class="mb-1 block text-slate-700">Query</span>
          <input v-model="searchForm.query" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Area</span>
          <input v-model.number="searchForm.area" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Page</span>
          <input v-model.number="searchForm.page" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Per Page</span>
          <input v-model.number="searchForm.per_page" type="number" min="1" max="100" class="form-input" />
        </label>
      </div>
      <label class="mt-2 inline-flex items-center gap-2 text-sm text-slate-700">
        <input v-model="searchForm.exact_search" type="checkbox" class="h-4 w-4" /> Exact search
      </label>
      <div class="mt-3 flex gap-2">
        <button class="btn-primary" :disabled="loading.search" @click="runSearch">{{ loading.search ? 'Searching...' : 'Search' }}</button>
      </div>

      <div v-if="searchResult?.items?.length" class="table-shell mt-4">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Title</th>
              <th>Company</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in searchResult.items" :key="item.id">
              <td class="font-mono text-xs">{{ item.id }}</td>
              <td>{{ item.name }}</td>
              <td>{{ item.employer?.name || '-' }}</td>
              <td>
                <button class="btn-secondary px-2 py-1 text-xs" @click="selectedVacancyId = item.id; loadVacancyDetails(item.id)">Details</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Vacancy Details / Batch</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <div>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Vacancy ID</span>
            <input v-model="selectedVacancyId" class="form-input font-mono" />
          </label>
          <button class="btn-primary mt-2" :disabled="loading.details" @click="loadVacancyDetails()">{{ loading.details ? 'Loading...' : 'Load Details' }}</button>
        </div>
        <div>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Batch IDs (comma/newline separated)</span>
            <textarea v-model="batchIdsInput" class="form-input h-24 font-mono"></textarea>
          </label>
          <button class="btn-secondary mt-2" :disabled="loading.batch" @click="loadBatch">{{ loading.batch ? 'Loading...' : 'Load Batch' }}</button>
        </div>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Vacancy Details" :value="vacancyDetails" />
        <JsonBlock title="Batch Result" :value="batchResult" />
      </div>
    </section>

    <section class="panel p-4">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <h3 class="panel-title text-base">Reference Data from Vacancy Service</h3>
        <button class="btn-primary" :disabled="loading.ref" @click="loadAllReference">Load All</button>
      </div>
      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <button class="btn-secondary" @click="fetchReference('/api/v1/areas', 'areas')">Areas</button>
        <button class="btn-secondary" @click="fetchReference('/api/v1/industries', 'industries')">Industries</button>
        <button class="btn-secondary" @click="fetchReference('/api/v1/professional-roles', 'professionalRoles')">Professional Roles</button>
      </div>
      <div class="mt-3 grid gap-2 md:grid-cols-[220px_1fr]">
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Metro city ID</span>
          <input v-model.number="metroCityId" type="number" class="form-input" />
        </label>
        <button class="btn-secondary self-end" @click="loadMetro">Load Metro</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Areas" :value="referenceData.areas" />
        <JsonBlock title="Industries" :value="referenceData.industries" />
        <JsonBlock title="Professional Roles" :value="referenceData.professionalRoles" />
        <JsonBlock title="Metro" :value="referenceData.metro" />
      </div>
      <div class="mt-4">
        <button class="btn-secondary" @click="fetchReference('/api/v1/rate-limit/stats', 'rateLimit')">Load Rate Limit Stats</button>
        <div class="mt-3">
          <JsonBlock title="Rate Limit Stats" :value="referenceData.rateLimit" max-height="14rem" />
        </div>
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
