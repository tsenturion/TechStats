<script setup>
import { computed, ref, watch, onMounted } from 'vue'
import { RouterLink, RouterView, useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { useForm } from 'vee-validate'
import { toTypedSchema } from '@vee-validate/zod'
import { z } from 'zod'

import { useApi } from './composables/useApi'
import { useAuth } from './composables/useAuth'
import { useUiPrefs } from './composables/useUiPrefs'

const route = useRoute()
const router = useRouter()
const configOpened = ref(false)
const { apiRequest, config, persistConfig, resetConfig } = useApi()
const { authState, hasRole, isAuthenticated, isAdmin, setAuth, clearAuth } = useAuth()
const { theme, language, setTheme, setLanguage } = useUiPrefs()
const { t: i18nT, locale } = useI18n({ useScope: 'global' })

const authSchema = toTypedSchema(
  z.object({
    username: z.string().min(1),
    password: z.string().min(1),
  }),
)
const { defineField, validate: validateAuthForm, setFieldValue } = useForm({
  validationSchema: authSchema,
  initialValues: {
    username: '',
    password: '',
  },
})
const [authUsername] = defineField('username')
const [authPassword] = defineField('password')

const authLoading = ref(false)
const authError = ref('')
const authNotice = ref('')

const navDefinitions = [
  { to: '/', key: 'overview', minRole: 'admin' },
  { to: '/documentation', key: 'documentation', minRole: 'guest' },
  { to: '/analysis', key: 'liveAnalysis', minRole: 'user' },
  { to: '/vacancies', key: 'vacancies', minRole: 'user' },
  { to: '/analyzer', key: 'analyzer', minRole: 'guest' },
  { to: '/cache', key: 'cache', minRole: 'guest' },
  { to: '/websocket', key: 'websocket', minRole: 'user' },
  { to: '/metrics', key: 'metrics', minRole: 'guest' },
  { to: '/explorer', key: 'explorer', minRole: 'admin' },
  { to: '/admin-settings', key: 'adminSettings', minRole: 'admin' },
]

const endpointFields = [
  { key: 'gateway', labelKey: 'gateway' },
  { key: 'vacancy', labelKey: 'vacancy' },
  { key: 'analyzer', label: 'Analyzer' },
  { key: 'cache', label: 'Cache' },
  { key: 'websocket', labelKey: 'websocketService' },
  { key: 'prometheus', label: 'Prometheus' },
  { key: 'grafana', label: 'Grafana' },
  { key: 'adminToken', labelKey: 'websocketAdminToken' },
]

function t(key) {
  return i18nT(`app.${key}`)
}

function applyTheme() {
  document.body.classList.toggle('theme-dark', theme.value === 'dark')
}
watch(theme, applyTheme, { immediate: true })
watch(
  language,
  (value) => {
    locale.value = value === 'en' ? 'en' : 'ru'
  },
  { immediate: true },
)

watch(
  () => authState.role,
  () => {
    const minRole = route.meta?.minRole || 'guest'
    if (!hasRole(minRole)) {
      router.push('/documentation')
    }
    if (!isAdmin.value) {
      configOpened.value = false
    }
  },
)

const navItems = computed(() => {
  return navDefinitions
    .filter((item) => hasRole(item.minRole))
    .map((item) => ({ to: item.to, label: t(item.key) }))
})

const currentTitle = computed(() => {
  const matched = navDefinitions.find((item) => item.to === route.path)
  return matched ? t(matched.key) : 'TechStats'
})

const roleLabel = computed(() => {
  if (authState.role === 'admin') return t('roleAdmin')
  if (authState.role === 'user') return t('roleUser')
  return t('roleGuest')
})

function closeConfig() {
  configOpened.value = false
}

function openConfig() {
  if (!isAdmin.value) {
    return
  }
  configOpened.value = true
}

function saveConfig() {
  if (!isAdmin.value) {
    return
  }
  persistConfig()
  closeConfig()
}

function restoreDefaults() {
  if (!isAdmin.value) {
    return
  }
  resetConfig()
}

function changeTheme(value) {
  setTheme(value)
}

function changeLanguage(value) {
  setLanguage(value)
}

async function verifyCurrentToken() {
  if (!authState.accessToken) {
    return
  }
  try {
    const response = await apiRequest('gateway', '/api/v1/auth/me')
    setAuth({
      accessToken: authState.accessToken,
      role: response.data?.role || authState.role,
      username: response.data?.username || authState.username,
    })
  } catch {
    clearAuth()
  }
}

async function login() {
  const validation = await validateAuthForm()
  if (!validation.valid) {
    authError.value = t('authError')
    return
  }

  authLoading.value = true
  authError.value = ''
  authNotice.value = ''

  try {
    const response = await apiRequest('gateway', '/api/v1/auth/login', {
      method: 'POST',
      auth: false,
      body: {
        username: authUsername.value,
        password: authPassword.value,
      },
    })

    setAuth({
      accessToken: response.data?.access_token,
      refreshToken: response.data?.refresh_token,
      role: response.data?.role,
      username: response.data?.username,
    })

    setFieldValue('password', '')
  } catch (error) {
    authError.value = error?.data?.detail || t('authError')
  } finally {
    authLoading.value = false
  }
}

async function register() {
  const validation = await validateAuthForm()
  if (!validation.valid) {
    authError.value = t('registerError')
    return
  }

  authLoading.value = true
  authError.value = ''
  authNotice.value = ''

  try {
    const response = await apiRequest('gateway', '/api/v1/auth/register', {
      method: 'POST',
      auth: false,
      body: {
        username: authUsername.value,
        password: authPassword.value,
      },
    })

    setAuth({
      accessToken: response.data?.access_token,
      refreshToken: response.data?.refresh_token,
      role: response.data?.role,
      username: response.data?.username,
    })

    setFieldValue('password', '')
    authNotice.value = t('registerSuccess')
  } catch (error) {
    authError.value = error?.data?.detail || t('registerError')
  } finally {
    authLoading.value = false
  }
}

function logout() {
  clearAuth()
  setFieldValue('password', '')
  authNotice.value = ''
}

onMounted(() => {
  void verifyCurrentToken()
})
</script>

<template>
  <div class="min-h-screen">
    <header class="app-header sticky top-0 z-30 backdrop-blur">
      <div class="mx-auto flex max-w-[1600px] flex-wrap items-center justify-between gap-4 px-4 py-3 lg:px-6">
        <div class="flex items-center gap-3">
          <div class="h-9 w-9 rounded-xl bg-brand-600 p-2 text-white shadow">
            <svg viewBox="0 0 24 24" fill="none" class="h-full w-full">
              <path d="M4 19h16" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
              <path d="M7 15l3-4 3 2 4-6" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
          </div>
          <div>
            <p class="font-display text-sm font-bold tracking-wide text-brand-700">{{ t('title') }}</p>
            <h1 class="font-display text-lg font-semibold leading-tight text-slate-900">{{ currentTitle }}</h1>
            <p class="text-xs text-slate-500">
              <span class="font-semibold">{{ roleLabel }}</span>
              <span v-if="isAuthenticated"> · {{ authState.username }}</span>
            </p>
          </div>
        </div>

        <div class="flex flex-wrap items-center justify-end gap-2">
          <div class="toggle-group">
            <span class="toggle-label">{{ t('theme') }}</span>
            <button class="toggle-btn" :class="{ 'toggle-btn-active': theme === 'light' }" @click="changeTheme('light')">{{ t('light') }}</button>
            <button class="toggle-btn" :class="{ 'toggle-btn-active': theme === 'dark' }" @click="changeTheme('dark')">{{ t('dark') }}</button>
          </div>

          <div class="toggle-group">
            <span class="toggle-label">{{ t('language') }}</span>
            <button class="toggle-btn" :class="{ 'toggle-btn-active': language === 'ru' }" @click="changeLanguage('ru')">RU</button>
            <button class="toggle-btn" :class="{ 'toggle-btn-active': language === 'en' }" @click="changeLanguage('en')">EN</button>
          </div>

          <div v-if="!isAuthenticated" class="auth-strip">
            <input v-model="authUsername" :placeholder="t('username')" class="auth-input" />
            <input v-model="authPassword" :placeholder="t('password')" type="password" class="auth-input" @keyup.enter="login" />
            <button class="btn-primary px-3 py-1.5 text-xs" :disabled="authLoading" @click="login">
              {{ authLoading ? '...' : t('login') }}
            </button>
            <button class="btn-secondary px-3 py-1.5 text-xs" :disabled="authLoading" @click="register">
              {{ authLoading ? '...' : t('register') }}
            </button>
          </div>
          <button v-else class="btn-secondary px-3 py-1.5 text-xs" @click="logout">{{ t('logout') }}</button>

        </div>
      </div>
      <p v-if="!isAuthenticated" class="mx-auto max-w-[1600px] px-4 pb-2 text-xs text-amber-700 lg:px-6">
        {{ t('guestHint') }}
      </p>
      <p v-if="authError" class="mx-auto max-w-[1600px] px-4 pb-2 text-xs text-rose-700 lg:px-6">{{ authError }}</p>
      <p v-if="authNotice" class="mx-auto max-w-[1600px] px-4 pb-2 text-xs text-emerald-700 lg:px-6">{{ authNotice }}</p>
    </header>

    <div class="mx-auto grid max-w-[1600px] grid-cols-1 gap-4 px-4 py-4 lg:grid-cols-[250px_minmax(0,1fr)] lg:gap-6 lg:px-6">
      <aside class="panel h-fit p-2 lg:sticky lg:top-24">
        <nav class="grid gap-1">
          <RouterLink
            v-for="item in navItems"
            :key="item.to"
            :to="item.to"
            class="app-nav-link"
            :class="route.path === item.to ? 'app-nav-link-active' : 'app-nav-link-idle'"
          >
            {{ item.label }}
          </RouterLink>
          <button
            v-if="isAdmin"
            class="app-nav-link w-full text-left"
            :class="configOpened ? 'app-nav-link-active' : 'app-nav-link-idle'"
            @click="openConfig"
          >
            {{ t('backendConfig') }}
          </button>
        </nav>
      </aside>

      <main class="min-w-0">
        <RouterView v-slot="{ Component }">
          <KeepAlive>
            <component :is="Component" />
          </KeepAlive>
        </RouterView>
      </main>
    </div>

    <div v-if="configOpened && isAdmin" class="fixed inset-0 z-40 flex items-end justify-center bg-slate-900/45 p-4 lg:items-center" @click.self="closeConfig">
      <section class="panel w-full max-w-2xl p-5">
        <div class="mb-4 flex items-start justify-between gap-3">
          <div>
            <h2 class="panel-title">{{ t('modalTitle') }}</h2>
            <p class="mt-1 text-sm text-slate-600">{{ t('modalSubtitle') }}</p>
          </div>
          <button class="btn-secondary px-3 py-1.5 text-xs" @click="closeConfig">{{ t('close') }}</button>
        </div>

        <div class="grid gap-3 md:grid-cols-2">
          <label v-for="field in endpointFields" :key="field.key" class="text-sm">
            <span class="mb-1 block text-slate-700">{{ field.label || t(field.labelKey) }}</span>
            <input v-model="config[field.key]" class="form-input font-mono" />
          </label>
        </div>

        <div class="mt-5 flex flex-wrap justify-end gap-2">
          <button class="btn-secondary" @click="restoreDefaults">{{ t('resetDefaults') }}</button>
          <button class="btn-primary" @click="saveConfig">{{ t('save') }}</button>
        </div>
      </section>
    </div>
  </div>
</template>
