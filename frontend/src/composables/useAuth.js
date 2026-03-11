import { computed } from 'vue'

import { useAuthStore } from '../stores/auth'

export function useAuth() {
  const store = useAuthStore()

  function hasRole(requiredRole) {
    if (requiredRole === 'guest') return true
    if (requiredRole === 'user') return store.role === 'user' || store.role === 'admin'
    if (requiredRole === 'admin') return store.role === 'admin'
    return false
  }

  return {
    authState: store,
    isAuthenticated: computed(() => store.isAuthenticated),
    isAdmin: computed(() => store.isAdmin),
    isUserOrAdmin: computed(() => store.isUserOrAdmin),
    setAuth: (payload) => store.setAuth(payload),
    clearAuth: () => store.clearAuth(),
    hasRole,
  }
}
