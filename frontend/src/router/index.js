import { createRouter, createWebHistory } from 'vue-router'

import DashboardPage from '../pages/DashboardPage.vue'
import DocumentationPage from '../pages/DocumentationPage.vue'
import LiveAnalysisPage from '../pages/LiveAnalysisPage.vue'
import VacancyExplorerPage from '../pages/VacancyExplorerPage.vue'
import AnalyzerLabPage from '../pages/AnalyzerLabPage.vue'
import CacheStudioPage from '../pages/CacheStudioPage.vue'
import WebsocketOpsPage from '../pages/WebsocketOpsPage.vue'
import MetricsPage from '../pages/MetricsPage.vue'
import ApiExplorerPage from '../pages/ApiExplorerPage.vue'
import AdminSettingsPage from '../pages/AdminSettingsPage.vue'
import { useAuth } from '../composables/useAuth'
import { useUiPrefsStore } from '../stores/uiPrefs'

const routeTitles = {
  dashboard: { ru: 'Обзор', en: 'Overview' },
  documentation: { ru: 'Documentation', en: 'Documentation' },
  analysis: { ru: 'Live Analysis', en: 'Live Analysis' },
  vacancies: { ru: 'Вакансии', en: 'Vacancies' },
  analyzer: { ru: 'Анализатор', en: 'Analyzer' },
  cache: { ru: 'Кэш', en: 'Cache' },
  websocket: { ru: 'WebSocket', en: 'WebSocket' },
  metrics: { ru: 'Метрики', en: 'Metrics' },
  explorer: { ru: 'Explorer', en: 'Explorer' },
  'admin-settings': { ru: 'Админ Константы', en: 'Admin Runtime' },
}

const routes = [
  { path: '/', name: 'dashboard', component: DashboardPage, meta: { title: 'Overview', minRole: 'admin' } },
  { path: '/documentation', name: 'documentation', component: DocumentationPage, meta: { title: 'Documentation', minRole: 'guest' } },
  { path: '/analysis', name: 'analysis', component: LiveAnalysisPage, meta: { title: 'Live Analysis', minRole: 'user' } },
  { path: '/vacancies', name: 'vacancies', component: VacancyExplorerPage, meta: { title: 'Vacancy Explorer', minRole: 'user' } },
  { path: '/analyzer', name: 'analyzer', component: AnalyzerLabPage, meta: { title: 'Analyzer Lab', minRole: 'guest' } },
  { path: '/cache', name: 'cache', component: CacheStudioPage, meta: { title: 'Cache Studio', minRole: 'guest' } },
  { path: '/websocket', name: 'websocket', component: WebsocketOpsPage, meta: { title: 'WebSocket Ops', minRole: 'user' } },
  { path: '/metrics', name: 'metrics', component: MetricsPage, meta: { title: 'Metrics', minRole: 'guest' } },
  { path: '/explorer', name: 'explorer', component: ApiExplorerPage, meta: { title: 'API Explorer', minRole: 'admin' } },
  { path: '/admin-settings', name: 'admin-settings', component: AdminSettingsPage, meta: { title: 'Admin Settings', minRole: 'admin' } },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

router.beforeEach((to) => {
  const minRole = to.meta?.minRole || 'guest'
  const { hasRole } = useAuth()
  if (hasRole(minRole)) {
    return true
  }
  return { path: '/documentation' }
})

router.afterEach((to) => {
  const language = useUiPrefsStore().language || 'ru'
  const localizedTitle = routeTitles[to.name]?.[language] || to.meta?.title || 'Dashboard'
  document.title = `TechStats UI | ${localizedTitle}`
})

export default router
