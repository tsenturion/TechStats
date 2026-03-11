<script setup>
import { computed, onMounted, reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'
import { useAuth } from '../composables/useAuth'
import { useRuntimeSettings } from '../composables/useRuntimeSettings'
import { useUiPrefs } from '../composables/useUiPrefs'

const { apiRequest } = useApi()
const { isUserOrAdmin } = useAuth()
const { loadRuntimeSettings, getSettingValue } = useRuntimeSettings()
const { language } = useUiPrefs()

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
const runLocked = computed(() => !isUserOrAdmin.value)

const messages = {
  ru: {
    subtitle: 'Gateway proxy + прямые endpoint-ы vacancy-service (areas, metro, industries, rate limits).',
    userRequired: 'Требуется вход с ролью user/admin',
    guestHint:
      'В гостевом режиме доступны справочники и статистика. Поиск и детали вакансий доступны только user/admin.',
    searchViaGateway: 'Поиск через API Gateway',
    query: 'Query',
    area: 'Area',
    page: 'Page',
    perPage: 'Per page',
    exactSearch: 'Exact search',
    searching: 'Поиск...',
    search: 'Поиск',
    title: 'Название',
    id: 'ID',
    company: 'Компания',
    action: 'Действие',
    details: 'Details',
    detailsBatch: 'Детали вакансии / Batch',
    vacancyId: 'ID вакансии',
    loading: 'Загрузка...',
    loadDetails: 'Загрузить детали',
    batchIds: 'Batch ID (через запятую или новую строку)',
    loadBatch: 'Загрузить batch',
    vacancyDetails: 'Детали вакансии',
    batchResult: 'Результат batch',
    referenceData: 'Справочники из Vacancy Service',
    loadAll: 'Загрузить всё',
    industries: 'Industries',
    professionalRoles: 'Professional Roles',
    metroCityId: 'Metro city ID',
    loadMetro: 'Загрузить Metro',
    areas: 'Areas',
    metro: 'Metro',
    loadRateLimitStats: 'Загрузить статистику лимитов',
    rateLimitStats: 'Статистика лимитов',
    errors: 'Ошибки',
    unknownError: 'неизвестная ошибка',
    searchScope: 'search',
    vacancyDetailsScope: 'vacancy details',
    batchScope: 'batch',
  },
  en: {
    subtitle: 'Gateway proxy + direct vacancy-service endpoints (areas, metro, industries, rate limits).',
    userRequired: 'Login with user/admin role is required',
    guestHint:
      'In guest mode reference data and stats are available. Vacancy search and details require user/admin role.',
    searchViaGateway: 'Search via API Gateway',
    query: 'Query',
    area: 'Area',
    page: 'Page',
    perPage: 'Per page',
    exactSearch: 'Exact search',
    searching: 'Searching...',
    search: 'Search',
    title: 'Title',
    id: 'ID',
    company: 'Company',
    action: 'Action',
    details: 'Details',
    detailsBatch: 'Vacancy Details / Batch',
    vacancyId: 'Vacancy ID',
    loading: 'Loading...',
    loadDetails: 'Load Details',
    batchIds: 'Batch IDs (comma/newline separated)',
    loadBatch: 'Load Batch',
    vacancyDetails: 'Vacancy Details',
    batchResult: 'Batch Result',
    referenceData: 'Reference Data from Vacancy Service',
    loadAll: 'Load All',
    industries: 'Industries',
    professionalRoles: 'Professional Roles',
    metroCityId: 'Metro city ID',
    loadMetro: 'Load Metro',
    areas: 'Areas',
    metro: 'Metro',
    loadRateLimitStats: 'Load Rate Limit Stats',
    rateLimitStats: 'Rate Limit Stats',
    errors: 'Errors',
    unknownError: 'unknown error',
    searchScope: 'search',
    vacancyDetailsScope: 'vacancy details',
    batchScope: 'batch',
  },
}

function t(key) {
  return messages[language.value]?.[key] || messages.en[key] || key
}

function captureError(scope, error) {
  const detail = error?.data?.detail || error?.message || t('unknownError')
  errors.value.unshift(`[${scope}] ${detail}`)
}

async function runSearch() {
  if (runLocked.value) {
    captureError(t('searchScope'), { message: t('userRequired') })
    return
  }
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
  if (runLocked.value) {
    captureError(t('vacancyDetailsScope'), { message: t('userRequired') })
    return
  }
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
  if (runLocked.value) {
    captureError(t('batchScope'), { message: t('userRequired') })
    return
  }

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

onMounted(async () => {
  await loadRuntimeSettings()
  searchForm.area = Number(getSettingValue('search_default_area', searchForm.area))
  searchForm.per_page = Number(getSettingValue('search_default_per_page', searchForm.per_page))
  searchForm.exact_search = Boolean(getSettingValue('search_default_exact', searchForm.exact_search))
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Vacancy Explorer" :subtitle="t('subtitle')" />

    <section v-if="runLocked" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('guestHint') }}
      </p>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('searchViaGateway') }}</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <label class="text-sm md:col-span-3">
          <span class="mb-1 block text-slate-700">{{ t('query') }}</span>
          <input v-model="searchForm.query" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('area') }}</span>
          <input v-model.number="searchForm.area" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('page') }}</span>
          <input v-model.number="searchForm.page" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('perPage') }}</span>
          <input v-model.number="searchForm.per_page" type="number" min="1" max="100" class="form-input" />
        </label>
      </div>
      <label class="mt-2 inline-flex items-center gap-2 text-sm text-slate-700">
        <input v-model="searchForm.exact_search" type="checkbox" class="h-4 w-4" /> {{ t('exactSearch') }}
      </label>
      <div class="mt-3 flex gap-2">
        <button class="btn-primary" :disabled="loading.search || runLocked" @click="runSearch">{{ loading.search ? t('searching') : t('search') }}</button>
      </div>

      <div v-if="searchResult?.items?.length" class="table-shell mt-4">
        <table>
          <thead>
            <tr>
              <th>{{ t('id') }}</th>
              <th>{{ t('title') }}</th>
              <th>{{ t('company') }}</th>
              <th>{{ t('action') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in searchResult.items" :key="item.id">
              <td class="font-mono text-xs">{{ item.id }}</td>
              <td>{{ item.name }}</td>
              <td>{{ item.employer?.name || '-' }}</td>
              <td>
                <button class="btn-secondary px-2 py-1 text-xs" :disabled="runLocked" @click="selectedVacancyId = item.id; loadVacancyDetails(item.id)">{{ t('details') }}</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('detailsBatch') }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <div>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('vacancyId') }}</span>
            <input v-model="selectedVacancyId" class="form-input font-mono" />
          </label>
          <button class="btn-primary mt-2" :disabled="loading.details || runLocked" @click="loadVacancyDetails()">{{ loading.details ? t('loading') : t('loadDetails') }}</button>
        </div>
        <div>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('batchIds') }}</span>
            <textarea v-model="batchIdsInput" class="form-input h-24 font-mono"></textarea>
          </label>
          <button class="btn-secondary mt-2" :disabled="loading.batch || runLocked" @click="loadBatch">{{ loading.batch ? t('loading') : t('loadBatch') }}</button>
        </div>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('vacancyDetails')" :value="vacancyDetails" />
        <JsonBlock :title="t('batchResult')" :value="batchResult" />
      </div>
    </section>

    <section class="panel p-4">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <h3 class="panel-title text-base">{{ t('referenceData') }}</h3>
        <button class="btn-primary" :disabled="loading.ref" @click="loadAllReference">{{ t('loadAll') }}</button>
      </div>
      <div class="mt-3 grid gap-3 md:grid-cols-3">
        <button class="btn-secondary" @click="fetchReference('/api/v1/areas', 'areas')">{{ t('areas') }}</button>
        <button class="btn-secondary" @click="fetchReference('/api/v1/industries', 'industries')">{{ t('industries') }}</button>
        <button class="btn-secondary" @click="fetchReference('/api/v1/professional-roles', 'professionalRoles')">{{ t('professionalRoles') }}</button>
      </div>
      <div class="mt-3 grid gap-2 md:grid-cols-[220px_1fr]">
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('metroCityId') }}</span>
          <input v-model.number="metroCityId" type="number" class="form-input" />
        </label>
        <button class="btn-secondary self-end" @click="loadMetro">{{ t('loadMetro') }}</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('areas')" :value="referenceData.areas" />
        <JsonBlock :title="t('industries')" :value="referenceData.industries" />
        <JsonBlock :title="t('professionalRoles')" :value="referenceData.professionalRoles" />
        <JsonBlock :title="t('metro')" :value="referenceData.metro" />
      </div>
      <div class="mt-4">
        <button class="btn-secondary" @click="fetchReference('/api/v1/rate-limit/stats', 'rateLimit')">{{ t('loadRateLimitStats') }}</button>
        <div class="mt-3">
          <JsonBlock :title="t('rateLimitStats')" :value="referenceData.rateLimit" max-height="14rem" />
        </div>
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
