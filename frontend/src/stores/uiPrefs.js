import { defineStore } from 'pinia'

export const useUiPrefsStore = defineStore('uiPrefs', {
  state: () => ({
    theme: 'light',
    language: 'ru',
  }),
  actions: {
    setTheme(value) {
      this.theme = value === 'dark' ? 'dark' : 'light'
    },
    setLanguage(value) {
      this.language = value === 'en' ? 'en' : 'ru'
    },
    reset() {
      this.theme = 'light'
      this.language = 'ru'
    },
  },
  persist: true,
})
