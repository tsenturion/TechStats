import { computed, reactive, watch } from 'vue'

import { useApi } from './useApi'

const runtimeState = reactive({
  loading: false,
  loaded: false,
  error: '',
  settings: {},
  schema: {},
})

function getSettingValue(key, fallback = null) {
  if (Object.prototype.hasOwnProperty.call(runtimeState.settings, key)) {
    return runtimeState.settings[key]
  }
  return fallback
}

export function useRuntimeSettings() {
  const { useApiQuery } = useApi()
  const query = useApiQuery({
    queryKey: ['runtime-settings-public'],
    service: 'gateway',
    path: '/api/v1/runtime-settings/public',
    options: { auth: false },
    enabled: false,
  })

  watch(
    () => query.data.value,
    (data) => {
      if (!data) return
      runtimeState.settings = data?.settings || {}
      runtimeState.schema = data?.schema || {}
      runtimeState.loaded = true
      runtimeState.error = ''
    },
  )

  watch(
    () => query.isPending.value,
    (isPending) => {
      runtimeState.loading = Boolean(isPending)
    },
    { immediate: true },
  )

  watch(
    () => query.error.value,
    (error) => {
      if (!error) return
      runtimeState.error = error?.data?.detail || error?.message || 'Failed to load runtime settings'
    },
  )

  async function loadRuntimeSettings(force = false) {
    if (runtimeState.loading) return
    if (runtimeState.loaded && !force) return
    await query.refetch()
  }

  return {
    runtimeState,
    runtimeLoaded: computed(() => runtimeState.loaded),
    loadRuntimeSettings,
    getSettingValue,
    runtimeQuery: query,
  }
}
