<script setup>
import { computed, ref } from 'vue'
import { RouterLink, RouterView, useRoute } from 'vue-router'

import { useApi } from './composables/useApi'

const route = useRoute()
const configOpened = ref(false)
const { config, persistConfig, resetConfig } = useApi()

const navItems = [
  { to: '/', label: 'Overview' },
  { to: '/analysis', label: 'Live Analysis' },
  { to: '/vacancies', label: 'Vacancies' },
  { to: '/analyzer', label: 'Analyzer' },
  { to: '/cache', label: 'Cache' },
  { to: '/websocket', label: 'WebSocket' },
  { to: '/metrics', label: 'Metrics' },
  { to: '/explorer', label: 'Explorer' },
]

const currentTitle = computed(() => {
  const matched = navItems.find((item) => item.to === route.path)
  return matched?.label || 'TechStats'
})

function closeConfig() {
  configOpened.value = false
}

function saveConfig() {
  persistConfig()
  closeConfig()
}

function restoreDefaults() {
  resetConfig()
}
</script>

<template>
  <div class="min-h-screen">
    <header class="sticky top-0 z-30 border-b border-slate-200 bg-white/85 backdrop-blur">
      <div class="mx-auto flex max-w-[1600px] items-center justify-between gap-4 px-4 py-3 lg:px-6">
        <div class="flex items-center gap-3">
          <div class="h-9 w-9 rounded-xl bg-brand-600 p-2 text-white shadow">
            <svg viewBox="0 0 24 24" fill="none" class="h-full w-full">
              <path d="M4 19h16" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
              <path d="M7 15l3-4 3 2 4-6" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </div>
          <div>
            <p class="font-display text-sm font-bold tracking-wide text-brand-700">TECHSTATS CONTROL</p>
            <h1 class="font-display text-lg font-semibold leading-tight text-slate-900">{{ currentTitle }}</h1>
          </div>
        </div>
        <button class="btn-secondary" @click="configOpened = true">Backend Config</button>
      </div>
    </header>

    <div class="mx-auto grid max-w-[1600px] grid-cols-1 gap-4 px-4 py-4 lg:grid-cols-[250px_minmax(0,1fr)] lg:gap-6 lg:px-6">
      <aside class="panel h-fit p-2 lg:sticky lg:top-24">
        <nav class="grid gap-1">
          <RouterLink
            v-for="item in navItems"
            :key="item.to"
            :to="item.to"
            class="rounded-xl px-3 py-2 text-sm font-semibold transition"
            :class="route.path === item.to ? 'bg-brand-600 text-white' : 'text-slate-700 hover:bg-slate-100'"
          >
            {{ item.label }}
          </RouterLink>
        </nav>
      </aside>

      <main class="min-w-0">
        <RouterView />
      </main>
    </div>

    <div v-if="configOpened" class="fixed inset-0 z-40 flex items-end justify-center bg-slate-900/45 p-4 lg:items-center" @click.self="closeConfig">
      <section class="panel w-full max-w-2xl p-5">
        <div class="mb-4 flex items-start justify-between gap-3">
          <div>
            <h2 class="panel-title">Backend Endpoints</h2>
            <p class="mt-1 text-sm text-slate-600">Изменения сохраняются в localStorage браузера.</p>
          </div>
          <button class="btn-secondary px-3 py-1.5 text-xs" @click="closeConfig">Close</button>
        </div>

        <div class="grid gap-3 md:grid-cols-2">
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Gateway</span>
            <input v-model="config.gateway" class="form-input font-mono" />
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Vacancy</span>
            <input v-model="config.vacancy" class="form-input font-mono" />
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Analyzer</span>
            <input v-model="config.analyzer" class="form-input font-mono" />
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Cache</span>
            <input v-model="config.cache" class="form-input font-mono" />
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">WebSocket Service</span>
            <input v-model="config.websocket" class="form-input font-mono" />
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Prometheus</span>
            <input v-model="config.prometheus" class="form-input font-mono" />
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">Grafana</span>
            <input v-model="config.grafana" class="form-input font-mono" />
          </label>
          <label class="text-sm">
            <span class="mb-1 block text-slate-700">WebSocket Admin Token</span>
            <input v-model="config.adminToken" class="form-input font-mono" />
          </label>
        </div>

        <div class="mt-5 flex flex-wrap justify-end gap-2">
          <button class="btn-secondary" @click="restoreDefaults">Reset Defaults</button>
          <button class="btn-primary" @click="saveConfig">Save</button>
        </div>
      </section>
    </div>
  </div>
</template>
