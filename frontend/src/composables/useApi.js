import { reactive } from 'vue'

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
    if (!raw) {
      return { ...defaults }
    }
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
      if (value === undefined || value === null || value === '') {
        return
      }
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
  if (httpBase.startsWith('https://')) {
    return `wss://${httpBase.slice('https://'.length)}`
  }
  if (httpBase.startsWith('http://')) {
    return `ws://${httpBase.slice('http://'.length)}`
  }
  return httpBase
}

function wsUrl(service, path = '') {
  const base = toWsBase((config[service] || '').replace(/\/+$/, ''))
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  return `${base}${normalizedPath}`
}

async function apiRequest(service, path, options = {}) {
  const {
    method = 'GET',
    query = null,
    body = undefined,
    headers = {},
    parseAs = 'json',
  } = options

  const url = buildUrl(service, path, query)
  const init = {
    method,
    headers: {
      ...headers,
    },
  }

  if (body !== undefined) {
    if (body instanceof FormData) {
      init.body = body
    } else if (typeof body === 'string') {
      init.body = body
      if (!init.headers['Content-Type']) {
        init.headers['Content-Type'] = 'text/plain'
      }
    } else {
      init.body = JSON.stringify(body)
      if (!init.headers['Content-Type']) {
        init.headers['Content-Type'] = 'application/json'
      }
    }
  }

  const response = await fetch(url, init)
  const raw = await response.text()

  let data = raw
  if (parseAs === 'json') {
    try {
      data = raw ? JSON.parse(raw) : null
    } catch {
      data = raw
    }
  }

  if (!response.ok) {
    const error = new Error(`HTTP ${response.status} ${response.statusText}`)
    error.status = response.status
    error.url = url
    error.data = data
    throw error
  }

  return {
    status: response.status,
    headers: response.headers,
    data,
    raw,
    url,
  }
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
  }
}
