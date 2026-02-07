import { createRouter, createWebHistory } from 'vue-router'

import DashboardPage from '../pages/DashboardPage.vue'
import LiveAnalysisPage from '../pages/LiveAnalysisPage.vue'
import VacancyExplorerPage from '../pages/VacancyExplorerPage.vue'
import AnalyzerLabPage from '../pages/AnalyzerLabPage.vue'
import CacheStudioPage from '../pages/CacheStudioPage.vue'
import WebsocketOpsPage from '../pages/WebsocketOpsPage.vue'
import MetricsPage from '../pages/MetricsPage.vue'
import ApiExplorerPage from '../pages/ApiExplorerPage.vue'

const routes = [
  { path: '/', name: 'dashboard', component: DashboardPage, meta: { title: 'Обзор' } },
  { path: '/analysis', name: 'analysis', component: LiveAnalysisPage, meta: { title: 'Live Analysis' } },
  { path: '/vacancies', name: 'vacancies', component: VacancyExplorerPage, meta: { title: 'Vacancy Explorer' } },
  { path: '/analyzer', name: 'analyzer', component: AnalyzerLabPage, meta: { title: 'Analyzer Lab' } },
  { path: '/cache', name: 'cache', component: CacheStudioPage, meta: { title: 'Cache Studio' } },
  { path: '/websocket', name: 'websocket', component: WebsocketOpsPage, meta: { title: 'WebSocket Ops' } },
  { path: '/metrics', name: 'metrics', component: MetricsPage, meta: { title: 'Metrics' } },
  { path: '/explorer', name: 'explorer', component: ApiExplorerPage, meta: { title: 'API Explorer' } },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

router.afterEach((to) => {
  document.title = `TechStats UI | ${to.meta?.title || 'Dashboard'}`
})

export default router
