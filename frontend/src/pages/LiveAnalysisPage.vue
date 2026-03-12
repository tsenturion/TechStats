<script setup>
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { useWebSocket } from '@vueuse/core'

import SectionHeader from '../components/SectionHeader.vue'
import StatusBadge from '../components/StatusBadge.vue'
import { useApi } from '../composables/useApi'
import { useAuth } from '../composables/useAuth'
import { useRuntimeSettings } from '../composables/useRuntimeSettings'
import { useUiPrefs } from '../composables/useUiPrefs'

const { apiRequest, wsUrl } = useApi()
const { isUserOrAdmin } = useAuth()
const { loadRuntimeSettings, getSettingValue } = useRuntimeSettings()
const { language } = useUiPrefs()

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
const allVacancies = ref([])
const timeline = ref([])
const requestError = ref('')
const vacanciesLoadError = ref('')
const lastRunParams = ref(null)
const socketUrl = ref('')
const pendingWsPayload = ref(null)
const hasReceivedStreamFrame = ref(false)
const stopRequestedByUser = ref(false)
const withTechSort = reactive({
  key: 'title',
  direction: 'asc',
})
const withoutTechSort = reactive({
  key: 'title',
  direction: 'asc',
})

const messages = {
  ru: {
    subtitle: 'Поток в реальном времени через API Gateway WebSocket + fallback sync REST endpoint.',
    accessRequired: 'Требуется вход с ролью user/admin',
    accessHint: 'Для запуска нового анализа нужен вход с ролью user или admin.',
    vacancyTitle: 'Vacancy title',
    technology: 'Technology',
    area: 'Area',
    maxPages: 'Max pages',
    perPage: 'Per page',
    exactSearch: 'Exact search',
    useCache: 'Use cache',
    runLiveWebSocket: 'Run Live WebSocket',
    runSyncRest: 'Run Sync REST',
    stopStream: 'Stop Stream',
    state: 'state',
    session: 'session',
    noActiveStage: 'Нет активного этапа',
    kpi: 'KPI',
    technologyShare: 'Доля технологии в вакансиях',
    duplicatesSuffix: 'одинаковых',
    forTechnology: 'для',
    processedCoverage: 'Обработано',
    streamTimeline: 'Stream Timeline',
    withTechnology: 'Vacancies With Technology',
    withoutTechnology: 'Vacancies Without Technology',
    textMatches: 'Совпадения в тексте',
    keySkillsMatches: 'Совпадения в "Ключевые навыки"',
    sortHint: 'Нажмите по заголовку колонки для сортировки',
    title: 'Название',
    id: 'ID',
    url: 'URL',
    open: 'open',
    noVacancyName: '-',
    noVacancyUrl: '#',
    syncCompleted: 'Синхронный анализ завершен',
    stoppedByUser: 'Остановлено пользователем',
    stopCancelFailed: 'Не удалось отменить анализ на сервере',
    processedVacanciesPrefix: 'Обработано вакансий:',
    requestFailed: 'Запрос завершился ошибкой',
    failedToLoadVacancyList: 'Не удалось загрузить список вакансий',
    unknownWsError: 'Неизвестная ошибка websocket',
    wsFailed: 'WebSocket connection failed',
    invalidWsFrame: 'Получен некорректный websocket-кадр',
    vacanciesListLoadError:
      'Не удалось загрузить полный список вакансий для блока "Without Technology"',
  },
  en: {
    subtitle: 'Realtime pipeline through API Gateway WebSocket + fallback sync REST endpoint.',
    accessRequired: 'Login with user/admin role is required',
    accessHint: 'Login with user or admin role is required to run a new analysis.',
    vacancyTitle: 'Vacancy title',
    technology: 'Technology',
    area: 'Area',
    maxPages: 'Max pages',
    perPage: 'Per page',
    exactSearch: 'Exact search',
    useCache: 'Use cache',
    runLiveWebSocket: 'Run Live WebSocket',
    runSyncRest: 'Run Sync REST',
    stopStream: 'Stop Stream',
    state: 'state',
    session: 'session',
    noActiveStage: 'No active stage',
    kpi: 'KPI',
    technologyShare: 'Technology share in vacancies',
    duplicatesSuffix: 'duplicates',
    forTechnology: 'for',
    processedCoverage: 'Processed',
    streamTimeline: 'Stream Timeline',
    withTechnology: 'Vacancies With Technology',
    withoutTechnology: 'Vacancies Without Technology',
    textMatches: 'Text matches',
    keySkillsMatches: 'Key skills matches',
    sortHint: 'Click column header to sort',
    title: 'Title',
    id: 'ID',
    url: 'URL',
    open: 'open',
    noVacancyName: '-',
    noVacancyUrl: '#',
    syncCompleted: 'Synchronous analysis completed',
    stoppedByUser: 'Stopped by user',
    stopCancelFailed: 'Failed to cancel analysis on server',
    processedVacanciesPrefix: 'Processed vacancies:',
    requestFailed: 'Request failed',
    failedToLoadVacancyList: 'Failed to load vacancies list',
    unknownWsError: 'Unknown websocket error',
    wsFailed: 'WebSocket connection failed',
    invalidWsFrame: 'Invalid websocket frame received',
    vacanciesListLoadError:
      'Failed to load the complete vacancies list for the "Without Technology" section',
  },
}

function t(key) {
  return messages[language.value]?.[key] || messages.en[key] || key
}

const hasResult = computed(() => Boolean(finalResult.value))
const resultPayload = computed(() => {
  if (finalResult.value) return finalResult.value
  return latestPayload.value?.metadata?.result || null
})
const technologySharePercent = computed(() => {
  const candidate =
    resultPayload.value?.tech_percentage ??
    resultPayload.value?.data?.tech_percentage ??
    resultPayload.value?.result?.tech_percentage

  const value = Number(candidate)
  return Number.isFinite(value) ? value : null
})
const techVacanciesCount = computed(() => {
  const candidate = resultPayload.value?.tech_vacancies
  const value = Number(candidate)
  return Number.isFinite(value) ? value : null
})
const requestedVacanciesCount = computed(() => {
  const requested = Number(resultPayload.value?.requested_vacancies)
  if (Number.isFinite(requested) && requested > 0) {
    return requested
  }
  const total = Number(resultPayload.value?.total_vacancies)
  return Number.isFinite(total) ? total : null
})
const totalVacanciesCount = computed(() => {
  const candidate = resultPayload.value?.total_vacancies
  const value = Number(candidate)
  return Number.isFinite(value) ? value : null
})
const duplicateVacanciesCount = computed(() => {
  const topLevelCandidate = Number(resultPayload.value?.duplicate_vacancies_count)
  if (Number.isFinite(topLevelCandidate)) {
    return Math.max(0, topLevelCandidate)
  }

  const withList = Array.isArray(resultPayload.value?.vacancies_with_tech)
    ? resultPayload.value.vacancies_with_tech
    : []
  const withoutList = Array.isArray(resultPayload.value?.vacancies_without_tech)
    ? resultPayload.value.vacancies_without_tech
    : []
  const combined = [...withList, ...withoutList]

  if (!combined.length || !combined.some((item) => Object.prototype.hasOwnProperty.call(item || {}, 'is_duplicate'))) {
    return null
  }

  return combined.reduce((count, item) => {
    return count + (item?.is_duplicate ? 1 : 0)
  }, 0)
})
const hasKpi = computed(() => technologySharePercent.value !== null || (techVacanciesCount.value !== null && requestedVacanciesCount.value !== null))
const withTechVacancies = computed(() => {
  const list = resultPayload.value?.vacancies_with_tech
  return Array.isArray(list) ? list : []
})
const sortedWithTechVacancies = computed(() => {
  const rows = [...withTechVacancies.value]
  const multiplier = withTechSort.direction === 'asc' ? 1 : -1

  rows.sort((left, right) => {
    if (withTechSort.key === 'title') {
      const leftName = String(left?.name || '').trim()
      const rightName = String(right?.name || '').trim()
      const cmp = leftName.localeCompare(rightName, undefined, { sensitivity: 'base', numeric: true })
      return cmp * multiplier
    }

    if (withTechSort.key === 'text_matches') {
      const leftValue = Number(left?.text_match_count ?? left?.match_count ?? 0)
      const rightValue = Number(right?.text_match_count ?? right?.match_count ?? 0)
      return (leftValue - rightValue) * multiplier
    }

    const leftValue = Number(left?.key_skills_match_count ?? 0)
    const rightValue = Number(right?.key_skills_match_count ?? 0)
    return (leftValue - rightValue) * multiplier
  })

  return rows
})
const withoutTechVacanciesFromResult = computed(() => {
  const list = resultPayload.value?.vacancies_without_tech
  return Array.isArray(list) ? list : null
})
const vacanciesWithoutTech = computed(() => {
  if (Array.isArray(withoutTechVacanciesFromResult.value)) {
    return withoutTechVacanciesFromResult.value
  }

  if (!Array.isArray(allVacancies.value) || allVacancies.value.length === 0) {
    return []
  }

  const withTechIds = new Set(withTechVacancies.value.map((item) => String(item.id)))
  return allVacancies.value.filter((vacancy) => !withTechIds.has(String(vacancy.id)))
})
const sortedVacanciesWithoutTech = computed(() => {
  const rows = [...vacanciesWithoutTech.value]
  const multiplier = withoutTechSort.direction === 'asc' ? 1 : -1

  rows.sort((left, right) => {
    const leftName = String(left?.name || '').trim()
    const rightName = String(right?.name || '').trim()
    const cmp = leftName.localeCompare(rightName, undefined, { sensitivity: 'base', numeric: true })
    return cmp * multiplier
  })

  return rows
})
const runLocked = computed(() => !isUserOrAdmin.value)

function setWithTechSort(key) {
  if (withTechSort.key === key) {
    withTechSort.direction = withTechSort.direction === 'asc' ? 'desc' : 'asc'
    return
  }

  withTechSort.key = key
  withTechSort.direction = key === 'title' ? 'asc' : 'desc'
}

function getWithTechSortIndicator(key) {
  if (withTechSort.key !== key) {
    return '↕'
  }
  return withTechSort.direction === 'asc' ? '↑' : '↓'
}

function setWithoutTechSort(key) {
  if (withoutTechSort.key === key) {
    withoutTechSort.direction = withoutTechSort.direction === 'asc' ? 'desc' : 'asc'
    return
  }

  withoutTechSort.key = key
  withoutTechSort.direction = 'asc'
}

function getWithoutTechSortIndicator(key) {
  if (withoutTechSort.key !== key) {
    return '↕'
  }
  return withoutTechSort.direction === 'asc' ? '↑' : '↓'
}

function vacancyLinkText(item) {
  const name = String(item?.name || t('noVacancyName')).trim()
  const id = String(item?.id || '').trim()
  if (!id) {
    return name
  }
  return `${name} (${id})`
}

function vacancyTitleOnly(item) {
  return String(item?.name || t('noVacancyName')).trim()
}

function resetOutput() {
  streamState.value = 'idle'
  progress.value = 0
  progressMessage.value = ''
  sessionId.value = ''
  latestPayload.value = null
  finalResult.value = null
  allVacancies.value = []
  timeline.value = []
  requestError.value = ''
  vacanciesLoadError.value = ''
  hasReceivedStreamFrame.value = false
}

function closeWebsocket() {
  wsClose()
}

async function stopWebsocket() {
  stopRequestedByUser.value = true
  pendingWsPayload.value = null
  hasReceivedStreamFrame.value = false

  const currentSessionId = String(sessionId.value || '').trim()
  const canCancelRemote = currentSessionId.length > 0 && streamState.value !== 'completed'

  requestError.value = ''

  if (canCancelRemote) {
    try {
      await apiRequest('websocket', `/api/v1/ws/sessions/${encodeURIComponent(currentSessionId)}/cancel`, {
        method: 'POST',
      })
    } catch (error) {
      requestError.value = `${t('stopCancelFailed')}: ${error?.data?.detail || error?.message || t('requestFailed')}`
    }
  }

  if (currentSessionId) {
    pushTimeline({
      type: 'progress',
      stage: 'cancelled',
      message: t('stoppedByUser'),
      progress: 0,
      session_id: currentSessionId,
      timestamp: Date.now() / 1000,
    })
  }

  streamState.value = 'idle'
  progress.value = 0
  progressMessage.value = t('stoppedByUser')
  sessionId.value = ''
  closeWebsocket()
}

function pushTimeline(event) {
  const sourceTimestamp = Number(event?.timestamp)
  const normalizedTimestamp = Number.isFinite(sourceTimestamp) ? sourceTimestamp * 1000 : Date.now()
  timeline.value.unshift({
    at: normalizedTimestamp,
    ...event,
  })
}

function withProgressFraction(message, payload) {
  const baseMessage = String(message || '').trim()
  const processed = Number(payload?.metadata?.processed)
  const total = Number(payload?.metadata?.total)

  if (!Number.isFinite(processed) || !Number.isFinite(total) || total <= 0) {
    return baseMessage
  }

  const normalizedProcessed = Math.max(0, Math.min(total, processed))
  const fraction = `${normalizedProcessed}/${total}`

  if (baseMessage.includes('/')) {
    return baseMessage
  }

  if (baseMessage) {
    return `${baseMessage} ${fraction}`
  }

  return `${t('processedVacanciesPrefix')} ${fraction}`
}

function formatLocalDate(value) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return '-'
  }

  return new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(date)
}

async function loadAllVacancies(paramsSnapshot) {
  if (!paramsSnapshot?.vacancy_title) {
    return
  }

  vacanciesLoadError.value = ''
  allVacancies.value = []

  const baseQuery = {
    query: paramsSnapshot.vacancy_title,
    area: paramsSnapshot.area,
    per_page: paramsSnapshot.per_page,
    exact_search: paramsSnapshot.exact_search,
  }

  try {
    const firstPage = await apiRequest('gateway', '/api/v1/vacancies/search', {
      query: {
        ...baseQuery,
        page: 0,
      },
    })

    const pagesFromApi = Number(firstPage.data?.pages ?? 1)
    const safePagesFromApi = Number.isFinite(pagesFromApi) && pagesFromApi > 0 ? pagesFromApi : 1
    const safeMaxPages = Math.max(1, Number(paramsSnapshot.max_pages) || 1)
    const pagesToLoad = Math.min(safePagesFromApi, safeMaxPages)

    const allItems = Array.isArray(firstPage.data?.items) ? [...firstPage.data.items] : []

    if (pagesToLoad > 1) {
      const pageRequests = []
      for (let page = 1; page < pagesToLoad; page += 1) {
        pageRequests.push(
          apiRequest('gateway', '/api/v1/vacancies/search', {
            query: {
              ...baseQuery,
              page,
            },
          }),
        )
      }

      const pageResults = await Promise.all(pageRequests)
      pageResults.forEach((pageResponse) => {
        const pageItems = Array.isArray(pageResponse.data?.items) ? pageResponse.data.items : []
        allItems.push(...pageItems)
      })
    }

    const deduped = new Map()
    allItems.forEach((item) => {
      const id = item?.id
      if (!id || deduped.has(String(id))) {
        return
      }
      deduped.set(String(id), {
        id,
        name: item.name || t('noVacancyName'),
        url: item.alternate_url || t('noVacancyUrl'),
      })
    })

    const totalFromResult = Number(resultPayload.value?.requested_vacancies ?? resultPayload.value?.total_vacancies)
    const hasTotalCap = Number.isFinite(totalFromResult) && totalFromResult > 0
    const fallbackList = Array.from(deduped.values())
    allVacancies.value = hasTotalCap ? fallbackList.slice(0, totalFromResult) : fallbackList
  } catch (error) {
    vacanciesLoadError.value = error?.data?.detail || error?.message || t('failedToLoadVacancyList')
  }
}

function handleStreamMessage(payload) {
  latestPayload.value = payload
  pushTimeline(payload)
  hasReceivedStreamFrame.value = true

  if (payload.session_id) {
    sessionId.value = payload.session_id
  }

  if (payload.type === 'error') {
    streamState.value = 'error'
    requestError.value = payload.message || t('unknownWsError')
    closeWebsocket()
    return
  }

  requestError.value = ''

  const stage = payload.stage || payload.type
  progress.value = Number(payload.progress ?? progress.value)
  progressMessage.value = withProgressFraction(payload.message || '', payload)

  if (stage === 'completed') {
    streamState.value = 'completed'
    finalResult.value = payload.metadata?.result || payload.result || latestPayload.value
    if (!Array.isArray(finalResult.value?.vacancies_without_tech)) {
      void loadAllVacancies(lastRunParams.value)
    }
    closeWebsocket()
  } else {
    streamState.value = 'streaming'
  }
}

function startLiveAnalysis() {
  if (runLocked.value) {
    requestError.value = t('accessRequired')
    return
  }

  resetOutput()
  lastRunParams.value = { ...form }
  pendingWsPayload.value = { ...lastRunParams.value }
  streamState.value = 'connecting'
  socketUrl.value = wsUrl('gateway', '/api/v1/ws/analyze', { includeAuth: true })
  wsOpen()
}

async function runSyncAnalysis() {
  if (runLocked.value) {
    requestError.value = t('accessRequired')
    return
  }

  resetOutput()
  lastRunParams.value = { ...form }
  streamState.value = 'loading'
  try {
    const response = await apiRequest('gateway', '/api/v1/analyze', {
      method: 'POST',
      body: lastRunParams.value,
      query: {
        use_cache: lastRunParams.value.use_cache,
      },
    })
    finalResult.value = response.data
    progress.value = 100
    progressMessage.value = t('syncCompleted')
    streamState.value = 'completed'
    if (!Array.isArray(finalResult.value?.vacancies_without_tech)) {
      await loadAllVacancies(lastRunParams.value)
    }
  } catch (error) {
    requestError.value = error?.data?.detail || error?.message || t('requestFailed')
    streamState.value = 'error'
  }
}

onBeforeUnmount(() => {
  closeWebsocket()
})

const { data: wsData, open: wsOpen, close: wsClose, send: wsSend } = useWebSocket(socketUrl, {
  immediate: false,
  autoReconnect: {
    retries: 2,
    delay: 1000,
  },
  onConnected() {
    stopRequestedByUser.value = false
    streamState.value = 'streaming'
    requestError.value = ''
    if (pendingWsPayload.value) {
      wsSend(JSON.stringify(pendingWsPayload.value))
      pendingWsPayload.value = null
    }
  },
  onDisconnected() {
    if (stopRequestedByUser.value) {
      stopRequestedByUser.value = false
      return
    }

    if (streamState.value === 'streaming' || streamState.value === 'connecting') {
      streamState.value = 'idle'
    }
  },
  onError() {
    if (stopRequestedByUser.value) {
      return
    }

    if (streamState.value === 'completed') {
      return
    }
    if (streamState.value === 'connecting' && !hasReceivedStreamFrame.value) {
      return
    }
    requestError.value = t('wsFailed')
    streamState.value = 'error'
  },
})

watch(wsData, (frame) => {
  if (!frame) return
  try {
    const payload = JSON.parse(frame)
    handleStreamMessage(payload)
  } catch {
    requestError.value = t('invalidWsFrame')
    streamState.value = 'error'
  }
})

onMounted(async () => {
  await loadRuntimeSettings()
  form.area = Number(getSettingValue('search_default_area', form.area))
  form.exact_search = Boolean(getSettingValue('search_default_exact', form.exact_search))
  form.max_pages = Number(getSettingValue('search_default_max_pages', form.max_pages))
  form.per_page = Number(getSettingValue('search_default_per_page', form.per_page))
  form.use_cache = Boolean(getSettingValue('search_default_use_cache', form.use_cache))
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader title="Live Analysis" :subtitle="t('subtitle')" />

    <section v-if="runLocked" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('accessHint') }}
      </p>
    </section>

    <section class="panel p-4">
      <div class="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('vacancyTitle') }}</span>
          <input v-model="form.vacancy_title" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('technology') }}</span>
          <input v-model="form.technology" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('area') }}</span>
          <input v-model.number="form.area" type="number" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('maxPages') }}</span>
          <input v-model.number="form.max_pages" type="number" min="1" max="20" class="form-input" />
        </label>
        <label class="text-sm">
          <span class="mb-1 block text-slate-700">{{ t('perPage') }}</span>
          <input v-model.number="form.per_page" type="number" min="1" max="100" class="form-input" />
        </label>
        <div class="grid gap-2 text-sm">
          <label class="inline-flex items-center gap-2">
            <input v-model="form.exact_search" type="checkbox" class="h-4 w-4" /> {{ t('exactSearch') }}
          </label>
          <label class="inline-flex items-center gap-2">
            <input v-model="form.use_cache" type="checkbox" class="h-4 w-4" /> {{ t('useCache') }}
          </label>
        </div>
      </div>

      <div class="mt-4 flex flex-wrap gap-2">
        <button class="btn-primary" :disabled="runLocked" @click="startLiveAnalysis">{{ t('runLiveWebSocket') }}</button>
        <button class="btn-secondary" :disabled="runLocked" @click="runSyncAnalysis">{{ t('runSyncRest') }}</button>
        <button class="btn-danger" @click="stopWebsocket">{{ t('stopStream') }}</button>
      </div>
    </section>

    <section class="panel p-4">
      <div class="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div class="space-x-2">
          <span class="text-sm text-slate-500">{{ t('state') }}:</span>
          <StatusBadge :status="streamState" />
        </div>
        <p class="font-mono text-xs text-slate-500">{{ t('session') }}: {{ sessionId || 'n/a' }}</p>
      </div>

      <div class="h-4 overflow-hidden rounded-full bg-slate-200">
        <div class="h-full rounded-full bg-brand-600 transition-all" :style="{ width: `${Math.max(0, Math.min(100, progress))}%` }" />
      </div>
      <p class="mt-2 text-sm text-slate-700">{{ progressMessage || t('noActiveStage') }}</p>

      <p v-if="requestError" class="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
        {{ requestError }}
      </p>
    </section>

    <section v-if="hasKpi" class="panel p-4">
      <p class="text-xs font-semibold uppercase tracking-wide text-slate-500">{{ t('kpi') }}</p>
      <div class="mt-2 flex items-end justify-between gap-3">
        <div>
          <p class="text-sm text-slate-600">{{ t('technologyShare') }}</p>
          <div class="flex items-end gap-3">
            <p v-if="technologySharePercent !== null" class="text-4xl font-extrabold leading-none text-brand-700">
              {{ technologySharePercent.toFixed(1) }}%
            </p>
            <p v-if="techVacanciesCount !== null && requestedVacanciesCount !== null" class="pb-1 text-lg font-semibold text-slate-700">
              {{ techVacanciesCount }}/{{ requestedVacanciesCount }}
              <span v-if="duplicateVacanciesCount !== null"> ({{ duplicateVacanciesCount }} {{ t('duplicatesSuffix') }})</span>
            </p>
          </div>
          <p
            v-if="totalVacanciesCount !== null && requestedVacanciesCount !== null && totalVacanciesCount < requestedVacanciesCount"
            class="mt-2 text-xs text-amber-700"
          >
            {{ t('processedCoverage') }} {{ totalVacanciesCount }}/{{ requestedVacanciesCount }}
          </p>
        </div>
        <p class="text-xs text-slate-500">{{ t('forTechnology') }} "{{ form.technology }}"</p>
      </div>
    </section>

    <section class="panel p-4" v-if="timeline.length">
      <h3 class="panel-title text-base">{{ t('streamTimeline') }}</h3>
      <div class="mt-3 max-h-64 overflow-auto">
        <div v-for="(item, idx) in timeline" :key="`${item.at}-${idx}`" class="mb-2 rounded-xl border border-slate-200 p-2">
          <div class="flex items-center justify-between gap-2">
            <p class="text-sm font-semibold text-slate-800">{{ item.stage || item.type }}</p>
            <p class="font-mono text-xs text-slate-500">{{ formatLocalDate(item.at) }}</p>
          </div>
          <p class="text-sm text-slate-600">{{ item.message || '-' }}</p>
        </div>
      </div>
    </section>

    <section v-if="hasResult && withTechVacancies.length" class="panel p-4">
      <h3 class="panel-title text-base">{{ t('withTechnology') }} ({{ withTechVacancies.length }})</h3>
      <p class="mt-1 text-xs text-slate-500">{{ t('sortHint') }}</p>
      <div class="table-shell mt-3">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>
                <button
                  class="inline-flex items-center gap-1 bg-transparent p-0 text-left font-semibold text-inherit hover:text-brand-700"
                  @click="setWithTechSort('title')"
                >
                  {{ t('title') }}
                  <span class="font-mono text-xs">{{ getWithTechSortIndicator('title') }}</span>
                </button>
              </th>
              <th>
                <button
                  class="inline-flex items-center gap-1 bg-transparent p-0 text-left font-semibold text-inherit hover:text-brand-700"
                  @click="setWithTechSort('text_matches')"
                >
                  {{ t('textMatches') }}
                  <span class="font-mono text-xs">{{ getWithTechSortIndicator('text_matches') }}</span>
                </button>
              </th>
              <th>
                <button
                  class="inline-flex items-center gap-1 bg-transparent p-0 text-left font-semibold text-inherit hover:text-brand-700"
                  @click="setWithTechSort('key_skills_matches')"
                >
                  {{ t('keySkillsMatches') }}
                  <span class="font-mono text-xs">{{ getWithTechSortIndicator('key_skills_matches') }}</span>
                </button>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(item, index) in sortedWithTechVacancies" :key="item.id || index">
              <td>{{ index + 1 }}</td>
              <td>
                <a class="text-brand-700 underline" :href="item.url" target="_blank" rel="noreferrer">{{ vacancyTitleOnly(item) }}</a>
              </td>
              <td>{{ item.text_match_count ?? item.match_count ?? 0 }}</td>
              <td>{{ item.key_skills_match_count ?? 0 }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-if="hasResult && vacanciesWithoutTech.length" class="panel p-4">
      <h3 class="panel-title text-base">{{ t('withoutTechnology') }} ({{ vacanciesWithoutTech.length }})</h3>
      <p class="mt-1 text-xs text-slate-500">{{ t('sortHint') }}</p>
      <div class="table-shell mt-3">
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>
                <button
                  class="inline-flex items-center gap-1 bg-transparent p-0 text-left font-semibold text-inherit hover:text-brand-700"
                  @click="setWithoutTechSort('title')"
                >
                  {{ t('title') }}
                  <span class="font-mono text-xs">{{ getWithoutTechSortIndicator('title') }}</span>
                </button>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(item, index) in sortedVacanciesWithoutTech" :key="`${item.id || 'vacancy'}-${index}`">
              <td>{{ index + 1 }}</td>
              <td>
                <a class="text-brand-700 underline" :href="item.url" target="_blank" rel="noreferrer">{{ vacancyTitleOnly(item) }}</a>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-if="hasResult && vacanciesLoadError" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('vacanciesListLoadError') }}: {{ vacanciesLoadError }}
      </p>
    </section>
  </div>
</template>
