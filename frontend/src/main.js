import { createApp } from 'vue'
import { VueQueryPlugin, QueryClient } from '@tanstack/vue-query'
import { createPinia } from 'pinia'
import piniaPluginPersistedstate from 'pinia-plugin-persistedstate'

import App from './App.vue'
import router from './router'
import { createI18nInstance } from './i18n'
import { useUiPrefsStore } from './stores/uiPrefs'
import './style.css'

const app = createApp(App)

const pinia = createPinia()
pinia.use(piniaPluginPersistedstate)
app.use(pinia)

const uiPrefs = useUiPrefsStore(pinia)
const i18n = createI18nInstance(uiPrefs.language || 'ru')
app.use(i18n)

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
      staleTime: 30_000,
    },
  },
})
app.use(VueQueryPlugin, { queryClient })

app.use(router)
app.mount('#app')
