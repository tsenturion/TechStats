<script setup>
import { onBeforeUnmount, reactive, ref } from 'vue'

import JsonBlock from '../components/JsonBlock.vue'
import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'

const { apiRequest } = useApi()

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
  text: 'Нужен Python разработчик со знанием FastAPI, Docker и PostgreSQL.',
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

function addError(scope, error) {
  const detail = error?.data?.detail || error?.message || 'unknown error'
  errors.value.unshift(`[${scope}] ${detail}`)
}

function parseCsv(value) {
  return value
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean)
}

async function runSyncAnalyze() {
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
  if (!asyncTask.id) return
  try {
    const result = await apiRequest('analyzer', `/api/v1/analyze/async/${asyncTask.id}/result`)
    asyncTask.result = result.data
  } catch (error) {
    addError('async result', error)
  }
}

async function runBatchAnalyze() {
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
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Analyzer Lab" subtitle="Полный цикл analyzer-service: sync/async/batch/text + patterns CRUD + stats." />

    <section class="panel p-4">
      <h3 class="panel-title text-base">Analyze (Sync + Async)</h3>
      <div class="mt-3 grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Vacancy title</span>
          <input v-model="analyzeForm.vacancy_title" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Technology</span>
          <input v-model="analyzeForm.technology" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Area</span>
          <input v-model.number="analyzeForm.area" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Max pages</span>
          <input v-model.number="analyzeForm.max_pages" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">Per page</span>
          <input v-model.number="analyzeForm.per_page" type="number" class="form-input" />
        </label>
        <div class="space-y-2 text-sm">
          <label class="inline-flex items-center gap-2"><input v-model="analyzeForm.exact_search" type="checkbox" class="h-4 w-4" />Exact search</label>
          <label class="inline-flex items-center gap-2"><input v-model="analyzeForm.use_cache" type="checkbox" class="h-4 w-4" />Use cache</label>
        </div>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="runSyncAnalyze">Run Sync</button>
        <button class="btn-secondary" @click="startAsyncAnalyze">Run Async</button>
        <button class="btn-secondary" @click="loadAsyncResult">Fetch Async Result</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Sync Result" :value="syncResult" />
        <JsonBlock title="Async Task" :value="asyncTask" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Batch + Text Analysis</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-2">
        <div class="space-y-2">
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Vacancy IDs</span>
            <textarea v-model="batchForm.vacancy_ids" class="form-input h-24 font-mono" placeholder="id1,id2,id3"></textarea>
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Technologies</span>
            <input v-model="batchForm.technologies" class="form-input" />
          </label>
          <label class="inline-flex items-center gap-2 text-sm"><input v-model="batchForm.exact_search" type="checkbox" class="h-4 w-4" />Exact search</label>
          <button class="btn-primary" @click="runBatchAnalyze">Run Batch Analysis</button>
        </div>

        <div class="space-y-2">
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Text</span>
            <textarea v-model="textForm.text" class="form-input h-24"></textarea>
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Mode</span>
            <select v-model="textForm.mode" class="form-input">
              <option value="single">Single Technology</option>
              <option value="multiple">Multiple Technologies</option>
            </select>
          </label>
          <label v-if="textForm.mode === 'single'" class="text-sm">
            <span class="mb-1 block text-slate-700">Technology</span>
            <input v-model="textForm.technology" class="form-input" />
          </label>
          <label v-else class="text-sm">
            <span class="mb-1 block text-slate-700">Technologies CSV</span>
            <input v-model="textForm.technologies" class="form-input" />
          </label>
          <button class="btn-secondary" @click="runTextAnalyze">Run Text Analysis</button>
        </div>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Batch Result" :value="batchResult" />
        <JsonBlock title="Text Analysis" :value="textResult" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Patterns CRUD</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-3">
        <label class="text-sm"><span class="mb-1 block">ID</span><input v-model="patternForm.id" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Name</span><input v-model="patternForm.name" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Category</span><input v-model="patternForm.category" class="form-input" /></label>
        <label class="text-sm lg:col-span-3"><span class="mb-1 block">Regex patterns (comma/newline)</span><textarea v-model="patternForm.patterns" class="form-input h-20 font-mono"></textarea></label>
        <label class="text-sm lg:col-span-2"><span class="mb-1 block">Aliases</span><input v-model="patternForm.aliases" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Weight</span><input v-model.number="patternForm.weight" type="number" step="0.1" class="form-input" /></label>
        <label class="text-sm lg:col-span-3"><span class="mb-1 block">Description</span><input v-model="patternForm.description" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="createPattern">Create</button>
        <button class="btn-secondary" @click="searchPatterns">Search by Name</button>
        <button class="btn-secondary" @click="loadPatterns">Load Patterns</button>
        <button class="btn-secondary" @click="loadPatternMeta">Load Categories/Stats</button>
      </div>

      <div class="mt-4 grid gap-3 md:grid-cols-2">
        <label class="text-sm">
          <span class="mb-1 block">Pattern ID for get/update</span>
          <input v-model="updatePatternId" class="form-input" />
          <div class="mt-2 flex gap-2">
            <button class="btn-secondary" @click="getPatternById">Get</button>
            <button class="btn-secondary" @click="updatePattern">Update</button>
          </div>
        </label>
        <label class="text-sm">
          <span class="mb-1 block">Pattern ID for delete</span>
          <input v-model="deletePatternId" class="form-input" />
          <div class="mt-2 flex gap-2">
            <button class="btn-danger" @click="deletePattern">Delete</button>
          </div>
        </label>
      </div>

      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Patterns list" :value="patternsState.patterns" />
        <JsonBlock title="Pattern details" :value="patternsState.patternDetails" />
        <JsonBlock title="Pattern search" :value="patternsState.searchResult" />
        <JsonBlock title="Pattern categories" :value="patternsState.categories" />
        <JsonBlock title="Pattern stats" :value="patternsState.stats" />
      </div>
    </section>

    <section class="panel p-4">
      <h3 class="panel-title text-base">Analyzer Stats</h3>
      <div class="mt-3 grid gap-3 lg:grid-cols-4">
        <label class="text-sm"><span class="mb-1 block">Summary hours</span><input v-model.number="statsForm.summaryHours" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Technology</span><input v-model="statsForm.technology" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Tech days</span><input v-model.number="statsForm.techDays" type="number" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Comparison days</span><input v-model.number="statsForm.comparisonDays" type="number" class="form-input" /></label>
        <label class="text-sm lg:col-span-3"><span class="mb-1 block">Comparison technologies CSV</span><input v-model="statsForm.comparisonTechs" class="form-input" /></label>
        <label class="text-sm"><span class="mb-1 block">Performance hours</span><input v-model.number="statsForm.performanceHours" type="number" class="form-input" /></label>
      </div>
      <div class="mt-3 flex flex-wrap gap-2">
        <button class="btn-primary" @click="loadSummaryStats">Summary</button>
        <button class="btn-secondary" @click="loadTechnologyStats">Technology</button>
        <button class="btn-secondary" @click="loadComparisonStats">Comparison</button>
        <button class="btn-secondary" @click="loadPerformanceStats">Performance + Cache</button>
      </div>
      <div class="mt-4 grid gap-4 xl:grid-cols-2">
        <JsonBlock title="Summary" :value="analyzerStats.summary" />
        <JsonBlock title="Technology stats" :value="analyzerStats.technology" />
        <JsonBlock title="Comparison" :value="analyzerStats.comparison" />
        <JsonBlock title="Performance" :value="analyzerStats.performance" />
        <JsonBlock title="Cache stats" :value="analyzerStats.cache" />
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
