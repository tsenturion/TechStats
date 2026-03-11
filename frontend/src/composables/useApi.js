import axios from 'axios'
import { reactive } from 'vue'
import { useMutation, useQuery } from '@tanstack/vue-query'

import { useAuth } from './useAuth'

const STORAGE_KEY = 'techstats_frontend_config_v1'

const defaults = {
  gateway: 'http://localhost:8000',
  vacancy: 'http://localhost:8001',
  analyzer: 'http://localhost:8002',
  cache: 'http://localhost:8003',
  websocket: 'http://localhost:8004',
  prometheus: 'http://localhost:9090',
  grafana: 'http://localhost:3000',
  adminToken: 'admin_secret_token',
}

function readPersistedConfig() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return { ...defaults }
    const parsed = JSON.parse(raw)
    return { ...defaults, ...parsed }
  } catch {
    return { ...defaults }
  }
}

const config = reactive(readPersistedConfig())

function persistConfig() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(config))
}

function resetConfig() {
  Object.assign(config, defaults)
  persistConfig()
}

function buildUrl(service, path = '', query = null) {
  const base = (config[service] || '').replace(/\/+$/, '')
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  const url = new URL(`${base}${normalizedPath}`)
  if (query && typeof query === 'object') {
    Object.entries(query).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return
      if (Array.isArray(value)) {
        value.forEach((item) => {
          if (item !== undefined && item !== null && item !== '') {
            url.searchParams.append(key, String(item))
          }
        })
        return
      }
      url.searchParams.set(key, String(value))
    })
  }
  return url.toString()
}

function toWsBase(httpBase) {
  if (httpBase.startsWith('https://')) return `wss://${httpBase.slice('https://'.length)}`
  if (httpBase.startsWith('http://')) return `ws://${httpBase.slice('http://'.length)}`
  return httpBase
}

function wsUrl(service, path = '', options = {}) {
  const { query = null, includeAuth = false } = options
  const { authState } = useAuth()
  const base = toWsBase((config[service] || '').replace(/\/+$/, ''))
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  const url = new URL(`${base}${normalizedPath}`)

  if (query && typeof query === 'object') {
    Object.entries(query).forEach(([key, value]) => {
      if (value === undefined || value === null || value === '') return
      url.searchParams.set(key, String(value))
    })
  }

  if (includeAuth && authState.accessToken) {
    url.searchParams.set('access_token', authState.accessToken)
  }

  return url.toString()
}

async function apiRequest(service, path, options = {}) {
  const { authState } = useAuth()
  const {
    method = 'GET',
    query = null,
    body = undefined,
    headers = {},
    parseAs = 'json',
    auth = true,
    timeout = 60_000,
  } = options

  const url = buildUrl(service, path)
  const requestHeaders = { ...headers }
  if (auth && authState.accessToken && !requestHeaders.Authorization) {
    requestHeaders.Authorization = `Bearer ${authState.accessToken}`
  }

  try {
    const response = await axios.request({
      url,
      method,
      params: query || undefined,
      data: body,
      headers: requestHeaders,
      timeout,
      responseType: parseAs === 'text' ? 'text' : 'json',
    })

    return {
      status: response.status,
      headers: response.headers,
      data: response.data,
      raw: typeof response.data === 'string' ? response.data : JSON.stringify(response.data ?? ''),
      url,
    }
  } catch (error) {
    const response = error?.response
    const wrapped = new Error(
      response ? `HTTP ${response.status} ${response.statusText || ''}`.trim() : error?.message || 'Request failed',
    )
    wrapped.status = response?.status
    wrapped.url = url
    wrapped.data = response?.data
    throw wrapped
  }
}

function useApiQuery({ queryKey, service, path, options = {}, enabled = true }) {
  return useQuery({
    queryKey,
    enabled,
    queryFn: async () => {
      const response = await apiRequest(service, path, options)
      return response.data
    },
  })
}

function useApiMutation({ service, path, defaultOptions = {} }) {
  return useMutation({
    mutationFn: async (payload) => {
      const response = await apiRequest(service, path, {
        ...defaultOptions,
        body: payload,
      })
      return response.data
    },
  })
}

export function useApi() {
  return {
    config,
    defaults,
    persistConfig,
    resetConfig,
    buildUrl,
    wsUrl,
    apiRequest,
    useApiQuery,
    useApiMutation,
  }
}
