import { defineStore } from 'pinia'

export const useAuthStore = defineStore('auth', {
  state: () => ({
    accessToken: '',
    refreshToken: '',
    role: 'guest',
    username: '',
  }),
  actions: {
    setAuth(payload = {}) {
      this.accessToken = payload.accessToken || ''
      this.refreshToken = payload.refreshToken || ''
      this.role = ['user', 'admin'].includes(payload.role) ? payload.role : 'guest'
      this.username = payload.username || ''
    },
    clearAuth() {
      this.accessToken = ''
      this.refreshToken = ''
      this.role = 'guest'
      this.username = ''
    },
  },
  getters: {
    isAuthenticated: (state) => Boolean(state.accessToken),
    isAdmin: (state) => state.role === 'admin',
    isUserOrAdmin: (state) => state.role === 'user' || state.role === 'admin',
  },
  persist: true,
})
