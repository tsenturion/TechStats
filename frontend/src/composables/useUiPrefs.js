import { computed } from 'vue'

import { useUiPrefsStore } from '../stores/uiPrefs'

export function useUiPrefs() {
  const store = useUiPrefsStore()

  return {
    theme: computed(() => store.theme),
    language: computed(() => store.language),
    setTheme: (value) => store.setTheme(value),
    setLanguage: (value) => store.setLanguage(value),
    resetUiPrefs: () => store.reset(),
    prefsKey: 'techstats_ui_prefs_v1',
  }
}
