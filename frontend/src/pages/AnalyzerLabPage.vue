<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'
import { useAuth } from '../composables/useAuth'
import { useRuntimeSettings } from '../composables/useRuntimeSettings'
import { useUiPrefs } from '../composables/useUiPrefs'

const { apiRequest } = useApi()
const { isUserOrAdmin, isAdmin } = useAuth()
const { loadRuntimeSettings, getSettingValue } = useRuntimeSettings()
const { language } = useUiPrefs()

const errors = ref([])

const analyzeForm = reactive({
  vacancy_title: 'Data Engineer',
  technology: 'Python',
  exact_search: true,
  area: 113,
  max_pages: 2,
  per_page: 50,
  use_cache: true,
})

const syncResult = ref(null)
const asyncTask = reactive({
  id: '',
  status: null,
  result: null,
})
let asyncPollTimer = null

const batchForm = reactive({
  vacancy_ids: '',
  technologies: 'Python,SQL,Docker',
  exact_search: true,
})
const batchResult = ref(null)

const textForm = reactive({
  text: 'Need a Python developer with FastAPI, Docker, and PostgreSQL.',
  technology: 'Python',
  technologies: 'Python,Docker,PostgreSQL',
  mode: 'single',
})
const textResult = ref(null)

const patternsState = reactive({
  categoryFilter: '',
  patterns: null,
  patternDetails: null,
  categories: null,
  stats: null,
  searchResult: null,
})

const patternForm = reactive({
  id: 'go',
  name: 'Go',
  category: 'programming_language',
  patterns: 'go\\b\n\\bgolang\\b',
  aliases: 'golang,go1.22',
  weight: 1,
  description: 'Go language',
})

const updatePatternId = ref('')
const deletePatternId = ref('')

const statsForm = reactive({
  summaryHours: 24,
  technology: 'Python',
  techDays: 7,
  comparisonTechs: 'Python,JavaScript,Go',
  comparisonDays: 30,
  performanceHours: 24,
})

const analyzerStats = reactive({
  summary: null,
  technology: null,
  comparison: null,
  performance: null,
  cache: null,
})
const runLocked = computed(() => !isUserOrAdmin.value)
const patternManageLocked = computed(() => !isAdmin.value)

const messages = {
  ru: {
    subtitle: 'Полный цикл analyzer-service: sync/async/batch/text + patterns CRUD + stats.',
    userRequired: 'Требуется вход с ролью user/admin',
    adminRequired: 'Требуется роль admin',
    guestHint:
      'В гостевом режиме доступны статистика и просмотр patterns. Запуск анализа доступен только user/admin.',
    adminHint: 'Управление patterns (create/update/delete) доступно только admin.',
    analyzeSection: 'Анализ (Sync + Async)',
    vacancyTitle: 'Vacancy title',
    technology: 'Technology',
    area: 'Area',
    maxPages: 'Max pages',
    perPage: 'Per page',
    exactSearch: 'Exact search',
    useCache: 'Use cache',
    runSync: 'Run Sync',
    runAsync: 'Run Async',
    fetchAsyncResult: 'Fetch Async Result',
    syncResult: 'Sync Result',
    asyncTask: 'Async Task',
    batchTextSection: 'Batch + Text Analysis',
    vacancyIds: 'Vacancy ID',
    technologies: 'Technologies',
    runBatchAnalysis: 'Run Batch Analysis',
    text: 'Text',
    mode: 'Mode',
    singleTechnology: 'Single Technology',
    multipleTechnologies: 'Multiple Technologies',
    technologiesCsv: 'Technologies CSV',
    runTextAnalysis: 'Run Text Analysis',
    batchResult: 'Batch Result',
    textAnalysis: 'Text Analysis',
    patternsCrud: 'Patterns CRUD',
    id: 'ID',
    name: 'Name',
    category: 'Category',
    regexPatterns: 'Regex patterns (comma/newline)',
    aliases: 'Aliases',
    weight: 'Weight',
    description: 'Description',
    create: 'Create',
    searchByName: 'Search by Name',
    loadPatterns: 'Load Patterns',
    loadCategoriesStats: 'Load Categories/Stats',
    patternIdGetUpdate: 'Pattern ID for get/update',
    get: 'Get',
    update: 'Update',
    patternIdDelete: 'Pattern ID for delete',
    delete: 'Delete',
    patternsList: 'Patterns list',
    patternDetails: 'Pattern details',
    patternSearch: 'Pattern search',
    patternCategories: 'Pattern categories',
    patternStats: 'Pattern stats',
    analyzerStats: 'Analyzer Stats',
    summaryHours: 'Summary hours',
    techDays: 'Tech days',
    comparisonDays: 'Comparison days',
    comparisonTechCsv: 'Comparison technologies CSV',
    performanceHours: 'Performance hours',
    summary: 'Summary',
    comparison: 'Comparison',
    performanceCache: 'Performance + Cache',
    technologyStats: 'Technology stats',
    performance: 'Performance',
    cacheStats: 'Cache stats',
    errors: 'Ошибки',
    unknownError: 'неизвестная ошибка',
  },
  en: {
    subtitle: 'Full analyzer-service cycle: sync/async/batch/text + patterns CRUD + stats.',
    userRequired: 'Login with user/admin role is required',
    adminRequired: 'Admin role is required',
    guestHint:
      'In guest mode stats and patterns view are available. Starting analysis requires user/admin role.',
    adminHint: 'Patterns management (create/update/delete) is available only for admin role.',
    analyzeSection: 'Analyze (Sync + Async)',
    vacancyTitle: 'Vacancy title',
    technology: 'Technology',
    area: 'Area',
    maxPages: 'Max pages',
    perPage: 'Per page',
    exactSearch: 'Exact search',
    useCache: 'Use cache',
    runSync: 'Run Sync',
    runAsync: 'Run Async',
    fetchAsyncResult: 'Fetch Async Result',
    syncResult: 'Sync Result',
    asyncTask: 'Async Task',
    batchTextSection: 'Batch + Text Analysis',
    vacancyIds: 'Vacancy IDs',
    technologies: 'Technologies',
    runBatchAnalysis: 'Run Batch Analysis',
    text: 'Text',
    mode: 'Mode',
    singleTechnology: 'Single Technology',
    multipleTechnologies: 'Multiple Technologies',
    technologiesCsv: 'Technologies CSV',
    runTextAnalysis: 'Run Text Analysis',
    batchResult: 'Batch Result',
    textAnalysis: 'Text Analysis',
    patternsCrud: 'Patterns CRUD',
    id: 'ID',
    name: 'Name',
    category: 'Category',
    regexPatterns: 'Regex patterns (comma/newline)',
    aliases: 'Aliases',
    weight: 'Weight',
    description: 'Description',
    create: 'Create',
    searchByName: 'Search by Name',
    loadPatterns: 'Load Patterns',
    loadCategoriesStats: 'Load Categories/Stats',
    patternIdGetUpdate: 'Pattern ID for get/update',
    get: 'Get',
    update: 'Update',
    patternIdDelete: 'Pattern ID for delete',
    delete: 'Delete',
    patternsList: 'Patterns list',
    patternDetails: 'Pattern details',
    patternSearch: 'Pattern search',
    patternCategories: 'Pattern categories',
    patternStats: 'Pattern stats',
    analyzerStats: 'Analyzer Stats',
    summaryHours: 'Summary hours',
    techDays: 'Tech days',
    comparisonDays: 'Comparison days',
    comparisonTechCsv: 'Comparison technologies CSV',
    performanceHours: 'Performance hours',
    summary: 'Summary',
    comparison: 'Comparison',
    performanceCache: 'Performance + Cache',
    technologyStats: 'Technology stats',
    performance: 'Performance',
    cacheStats: 'Cache stats',
    errors: 'Errors',
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
  if (!runLocked.value) return true
  addError(scope, { message: t('userRequired') })
  return false
}

function requireAdminPermission(scope) {
  if (!patternManageLocked.value) return true
  addError(scope, { message: t('adminRequired') })
  return false
}

function parseCsv(value) {
  return value
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

async function runSyncAnalyze() {
  if (!requireUserPermission('analyze sync')) return
  try {
    const response = await apiRequest('analyzer', '/api/v1/analyze', {
      method: 'POST',
      query: {
        use_cache: analyzeForm.use_cache,
      },
      body: {
        vacancy_title: analyzeForm.vacancy_title,
        technology: analyzeForm.technology,
        exact_search: analyzeForm.exact_search,
        area: analyzeForm.area,
        max_pages: analyzeForm.max_pages,
        per_page: analyzeForm.per_page,
      },
    })
    syncResult.value = response.data
  } catch (error) {
    addError('analyze sync', error)
  }
}

async function startAsyncAnalyze() {
  if (!requireUserPermission('analyze async')) return
  stopAsyncPolling()
  asyncTask.id = ''
  asyncTask.status = null
  asyncTask.result = null

  try {
    const response = await apiRequest('analyzer', '/api/v1/analyze/async', {
      method: 'POST',
      body: {
        vacancy_title: analyzeForm.vacancy_title,
        technology: analyzeForm.technology,
        exact_search: analyzeForm.exact_search,
        area: analyzeForm.area,
        max_pages: analyzeForm.max_pages,
        per_page: analyzeForm.per_page,
        use_cache: analyzeForm.use_cache,
      },
    })

    asyncTask.id = response.data.task_id
    asyncTask.status = response.data
    pollAsyncStatus()
  } catch (error) {
    addError('analyze async', error)
  }
}

function stopAsyncPolling() {
  if (asyncPollTimer) {
    clearTimeout(asyncPollTimer)
    asyncPollTimer = null
  }
}

async function pollAsyncStatus() {
  if (!asyncTask.id) return

  try {
    const status = await apiRequest('analyzer', `/api/v1/analyze/async/${asyncTask.id}/status`)
    asyncTask.status = status.data

    if (['completed', 'failed'].includes(status.data.status)) {
      if (status.data.status === 'completed') {
        await loadAsyncResult()
      }
      stopAsyncPolling()
      return
    }
  } catch (error) {
    addError('async status', error)
    stopAsyncPolling()
    return
  }

  asyncPollTimer = setTimeout(pollAsyncStatus, 1200)
}

async function loadAsyncResult() {
  if (!requireUserPermission('async result')) return
  if (!asyncTask.id) return
  try {
    const result = await apiRequest('analyzer', `/api/v1/analyze/async/${asyncTask.id}/result`)
    asyncTask.result = result.data
  } catch (error) {
    addError('async result', error)
  }
}

async function runBatchAnalyze() {
  if (!requireUserPermission('analyze batch')) return
  try {
    const response = await apiRequest('analyzer', '/api/v1/analyze/batch', {
      method: 'POST',
      body: {
        vacancy_ids: parseCsv(batchForm.vacancy_ids),
        technologies: parseCsv(batchForm.technologies),
        exact_search: batchForm.exact_search,
      },
    })
    batchResult.value = response.data
  } catch (error) {
    addError('analyze batch', error)
  }
}

async function runTextAnalyze() {
  if (!requireUserPermission('analyze text')) return
  try {
    const payload = {
      text: textForm.text,
    }
    if (textForm.mode === 'single') {
      payload.technology = textForm.technology
    } else {
      payload.technologies = parseCsv(textForm.technologies)
    }

    const response = await apiRequest('analyzer', '/api/v1/analyze/text', {
      method: 'POST',
      body: payload,
    })
    textResult.value = response.data
  } catch (error) {
    addError('analyze text', error)
  }
}

async function loadPatterns() {
  try {
    const response = await apiRequest('analyzer', '/api/v1/patterns', {
      query: {
        category: patternsState.categoryFilter || null,
      },
    })
    patternsState.patterns = response.data
  } catch (error) {
    addError('patterns list', error)
  }
}

async function getPatternById() {
  if (!updatePatternId.value) return
  try {
    const response = await apiRequest('analyzer', `/api/v1/patterns/${updatePatternId.value}`)
    patternsState.patternDetails = response.data
  } catch (error) {
    addError('pattern details', error)
  }
}

async function createPattern() {
  if (!requireAdminPermission('pattern create')) return
  try {
    const response = await apiRequest('analyzer', '/api/v1/patterns', {
      method: 'POST',
      body: {
        id: patternForm.id,
        name: patternForm.name,
        category: patternForm.category,
        patterns: parseCsv(patternForm.patterns),
        aliases: parseCsv(patternForm.aliases),
        weight: Number(patternForm.weight || 1),
        description: patternForm.description,
      },
    })
    patternsState.patternDetails = response.data
    await loadPatterns()
  } catch (error) {
    addError('pattern create', error)
  }
}

async function updatePattern() {
  if (!requireAdminPermission('pattern update')) return
  if (!updatePatternId.value) return
  try {
    const response = await apiRequest('analyzer', `/api/v1/patterns/${updatePatternId.value}`, {
      method: 'PUT',
      body: {
        id: patternForm.id,
        name: patternForm.name,
        category: patternForm.category,
        patterns: parseCsv(patternForm.patterns),
        aliases: parseCsv(patternForm.aliases),
        weight: Number(patternForm.weight || 1),
        description: patternForm.description,
      },
    })
    patternsState.patternDetails = response.data
    await loadPatterns()
  } catch (error) {
    addError('pattern update', error)
  }
}

async function deletePattern() {
  if (!requireAdminPermission('pattern delete')) return
  if (!deletePatternId.value) return
  try {
    const response = await apiRequest('analyzer', `/api/v1/patterns/${deletePatternId.value}`, {
      method: 'DELETE',
    })
    patternsState.patternDetails = response.data
    await loadPatterns()
  } catch (error) {
    addError('pattern delete', error)
  }
}

async function searchPatterns() {
  try {
    const response = await apiRequest('analyzer', '/api/v1/patterns/search', {
      method: 'POST',
      body: {
        query: patternForm.name,
        category: patternForm.category || null,
        limit: 25,
      },
    })
    patternsState.searchResult = response.data
  } catch (error) {
    addError('pattern search', error)
  }
}

async function loadPatternMeta() {
  try {
    const [categories, stats] = await Promise.all([
      apiRequest('analyzer', '/api/v1/patterns/categories'),
      apiRequest('analyzer', '/api/v1/patterns/stats'),
    ])
    patternsState.categories = categories.data
    patternsState.stats = stats.data
  } catch (error) {
    addError('pattern meta', error)
  }
}

async function loadSummaryStats() {
  try {
    const response = await apiRequest('analyzer', '/api/v1/stats/summary', {
      query: {
        hours: statsForm.summaryHours,
      },
    })
    analyzerStats.summary = response.data
  } catch (error) {
    addError('stats summary', error)
  }
}

async function loadTechnologyStats() {
  try {
    const response = await apiRequest('analyzer', `/api/v1/stats/technology/${encodeURIComponent(statsForm.technology)}`, {
      query: {
        days: statsForm.techDays,
      },
    })
    analyzerStats.technology = response.data
  } catch (error) {
    addError('stats technology', error)
  }
}

async function loadComparisonStats() {
  try {
    const response = await apiRequest('analyzer', '/api/v1/stats/comparison', {
      query: {
        technologies: parseCsv(statsForm.comparisonTechs),
        days: statsForm.comparisonDays,
      },
    })
    analyzerStats.comparison = response.data
  } catch (error) {
    addError('stats comparison', error)
  }
}

async function loadPerformanceStats() {
  try {
    const [performance, cache] = await Promise.all([
      apiRequest('analyzer', '/api/v1/stats/performance', {
        query: {
          hours: statsForm.performanceHours,
        },
      }),
      apiRequest('analyzer', '/api/v1/stats/cache'),
    ])

    analyzerStats.performance = performance.data
    analyzerStats.cache = cache.data
  } catch (error) {
    addError('stats performance/cache', error)
  }
}

onBeforeUnmount(() => {
  stopAsyncPolling()
})

onMounted(async () => {
  await loadRuntimeSettings()
  analyzeForm.area = Number(getSettingValue('search_default_area', analyzeForm.area))
  analyzeForm.exact_search = Boolean(getSettingValue('search_default_exact', analyzeForm.exact_search))
  analyzeForm.max_pages = Number(getSettingValue('search_default_max_pages', analyzeForm.max_pages))
  analyzeForm.per_page = Number(getSettingValue('search_default_per_page', analyzeForm.per_page))
  analyzeForm.use_cache = Boolean(getSettingValue('search_default_use_cache', analyzeForm.use_cache))
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Analyzer Lab" :subtitle="t('subtitle')" />

    <section v-if="runLocked" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('guestHint') }}
      </p>
    </section>
    <section v-if="patternManageLocked" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('adminHint') }}
      </p>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('analyzeSection') }}</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('vacancyTitle') }}</span>
          <input v-model="analyzeForm.vacancy_title" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('technology') }}</span>
          <input v-model="analyzeForm.technology" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('area') }}</span>
          <input v-model.number="analyzeForm.area" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('maxPages') }}</span>
          <input v-model.number="analyzeForm.max_pages" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('perPage') }}</span>
          <input v-model.number="analyzeForm.per_page" type="number" class="form-input" />
        </label>
        <div class="space-y-2 text-sm">
          <label class="inline-flex items-center gap-2"><input v-model="analyzeForm.exact_search" type="checkbox" class="h-4 w-4" />{{ t('exactSearch') }}</label>
          <label class="inline-flex items-center gap-2"><input v-model="analyzeForm.use_cache" type="checkbox" class="h-4 w-4" />{{ t('useCache') }}</label>
        </div>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" :disabled="runLocked" @click="runSyncAnalyze">{{ t('runSync') }}</button>
        <button class="btn-secondary" :disabled="runLocked" @click="startAsyncAnalyze">{{ t('runAsync') }}</button>
        <button class="btn-secondary" :disabled="runLocked" @click="loadAsyncResult">{{ t('fetchAsyncResult') }}</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('syncResult')" :value="syncResult" />
        <JsonBlock :title="t('asyncTask')" :value="asyncTask" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('batchTextSection') }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <div class="space-y-2">
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('vacancyIds') }}</span>
            <textarea v-model="batchForm.vacancy_ids" class="form-input h-24 font-mono" placeholder="id1,id2,id3"></textarea>
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('technologies') }}</span>
            <input v-model="batchForm.technologies" class="form-input" />
          </label>
          <label class="inline-flex items-center gap-2 text-sm"><input v-model="batchForm.exact_search" type="checkbox" class="h-4 w-4" />{{ t('exactSearch') }}</label>
          <button class="btn-primary" :disabled="runLocked" @click="runBatchAnalyze">{{ t('runBatchAnalysis') }}</button>
        </div>

        <div class="space-y-2">
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('text') }}</span>
            <textarea v-model="textForm.text" class="form-input h-24"></textarea>
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('mode') }}</span>
            <select v-model="textForm.mode" class="form-input">
              <option value="single">{{ t('singleTechnology') }}</option>
              <option value="multiple">{{ t('multipleTechnologies') }}</option>
            </select>
          </label>
          <label v-if="textForm.mode === 'single'" class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('technology') }}</span>
            <input v-model="textForm.technology" class="form-input" />
          </label>
          <label v-else class="text-sm">
            <span class="mb-1 block text-slate-700">{{ t('technologiesCsv') }}</span>
            <input v-model="textForm.technologies" class="form-input" />
          </label>
          <button class="btn-secondary" :disabled="runLocked" @click="runTextAnalyze">{{ t('runTextAnalysis') }}</button>
        </div>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('batchResult')" :value="batchResult" />
        <JsonBlock :title="t('textAnalysis')" :value="textResult" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('patternsCrud') }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">{{ t('id') }}</span><input v-model="patternForm.id" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('name') }}</span><input v-model="patternForm.name" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('category') }}</span><input v-model="patternForm.category" class="form-input" /></label>
        <label class="text-sm lg:col-span-3"><span class="mb-1 block">{{ t('regexPatterns') }}</span><textarea v-model="patternForm.patterns" class="form-input h-20 font-mono"></textarea></label>
        <label class="text-sm lg:col-span-2"><span class="mb-1 block">{{ t('aliases') }}</span><input v-model="patternForm.aliases" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('weight') }}</span><input v-model.number="patternForm.weight" type="number" step="0.1" class="form-input" /></label>
        <label class="text-sm lg:col-span-3"><span class="mb-1 block">{{ t('description') }}</span><input v-model="patternForm.description" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" :disabled="patternManageLocked" @click="createPattern">{{ t('create') }}</button>
        <button class="btn-secondary" @click="searchPatterns">{{ t('searchByName') }}</button>
        <button class="btn-secondary" @click="loadPatterns">{{ t('loadPatterns') }}</button>
        <button class="btn-secondary" @click="loadPatternMeta">{{ t('loadCategoriesStats') }}</button>
      </div>

      <div class="mt-4 grid gap-3 md:grid-cols-2">
        <label class="text-sm">
          <span class="mb-1 block">{{ t('patternIdGetUpdate') }}</span>
          <input v-model="updatePatternId" class="form-input" />
          <div class="mt-2 flex gap-2">
            <button class="btn-secondary" @click="getPatternById">{{ t('get') }}</button>
            <button class="btn-secondary" :disabled="patternManageLocked" @click="updatePattern">{{ t('update') }}</button>
          </div>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">{{ t('patternIdDelete') }}</span>
          <input v-model="deletePatternId" class="form-input" />
          <div class="mt-2 flex gap-2">
            <button class="btn-danger" :disabled="patternManageLocked" @click="deletePattern">{{ t('delete') }}</button>
          </div>
        </label>
      </div>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('patternsList')" :value="patternsState.patterns" />
        <JsonBlock :title="t('patternDetails')" :value="patternsState.patternDetails" />
        <JsonBlock :title="t('patternSearch')" :value="patternsState.searchResult" />
        <JsonBlock :title="t('patternCategories')" :value="patternsState.categories" />
        <JsonBlock :title="t('patternStats')" :value="patternsState.stats" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">{{ t('analyzerStats') }}</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-4">
        <label class="text-sm"><span class="mb-1 block">{{ t('summaryHours') }}</span><input v-model.number="statsForm.summaryHours" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('technology') }}</span><input v-model="statsForm.technology" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('techDays') }}</span><input v-model.number="statsForm.techDays" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('comparisonDays') }}</span><input v-model.number="statsForm.comparisonDays" type="number" class="form-input" /></label>
        <label class="text-sm lg:col-span-3"><span class="mb-1 block">{{ t('comparisonTechCsv') }}</span><input v-model="statsForm.comparisonTechs" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">{{ t('performanceHours') }}</span><input v-model.number="statsForm.performanceHours" type="number" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="loadSummaryStats">{{ t('summary') }}</button>
        <button class="btn-secondary" @click="loadTechnologyStats">{{ t('technology') }}</button>
        <button class="btn-secondary" @click="loadComparisonStats">{{ t('comparison') }}</button>
        <button class="btn-secondary" @click="loadPerformanceStats">{{ t('performanceCache') }}</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock :title="t('summary')" :value="analyzerStats.summary" />
        <JsonBlock :title="t('technologyStats')" :value="analyzerStats.technology" />
        <JsonBlock :title="t('comparison')" :value="analyzerStats.comparison" />
        <JsonBlock :title="t('performance')" :value="analyzerStats.performance" />
        <JsonBlock :title="t('cacheStats')" :value="analyzerStats.cache" />
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
