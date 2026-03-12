<script setup>
import { computed, onMounted, reactive, ref } from 'vue'

import SectionHeader from '../components/SectionHeader.vue'
import { useApi } from '../composables/useApi'
import { useAuth } from '../composables/useAuth'
import { useUiPrefs } from '../composables/useUiPrefs'

const { apiRequest } = useApi()
const { isAdmin } = useAuth()
const { language } = useUiPrefs()

const loading = ref(false)
const saving = ref(false)
const resetting = ref(false)
const error = ref('')
const success = ref('')

const schema = ref({})
const currentSettings = ref({})
const overrides = ref({})
const editBuffer = reactive({})

const messages = {
  ru: {
    subtitle: 'Единый реестр лимитов, задержек, batch-настроек и дефолтов форм.',
    adminOnly: 'Доступ только для роли admin.',
    settingsSaved: 'Настройки сохранены',
    settingsReset: 'Настройки сброшены к значениям по умолчанию',
    loading: 'Загрузка...',
    reload: 'Обновить',
    saving: 'Сохранение...',
    saveAll: 'Сохранить все',
    restoreCurrent: 'Вернуть текущие',
    resetting: 'Сброс...',
    resetDefaults: 'Сбросить в значения по умолчанию',
    key: 'Ключ',
    type: 'Тип',
    scope: 'Область',
    description: 'Описание',
    value: 'Значение',
    current: 'Текущее',
    override: 'Переопределение',
    failedLoad: 'Не удалось загрузить настройки',
    failedSave: 'Не удалось сохранить настройки',
    failedReset: 'Не удалось сбросить настройки',
  },
  en: {
    subtitle: 'Unified registry of limits, delays, batch settings, and form defaults.',
    adminOnly: 'Access is available only for admin role.',
    settingsSaved: 'Settings saved',
    settingsReset: 'Settings reset to defaults',
    loading: 'Loading...',
    reload: 'Reload',
    saving: 'Saving...',
    saveAll: 'Save All',
    restoreCurrent: 'Restore Current',
    resetting: 'Resetting...',
    resetDefaults: 'Reset to Defaults',
    key: 'Key',
    type: 'Type',
    scope: 'Scope',
    description: 'Description',
    value: 'Value',
    current: 'Current',
    override: 'Override',
    failedLoad: 'Failed to load settings',
    failedSave: 'Failed to save settings',
    failedReset: 'Failed to reset settings',
  },
}

function t(key) {
  return messages[language.value]?.[key] || messages.en[key] || key
}

function describeSetting(meta) {
  if (language.value === 'ru') {
    return meta?.description_ru || 'Описание отсутствует'
  }
  return meta?.description || '-'
}

const settingEntries = computed(() =>
  Object.keys(schema.value || {})
    .sort()
    .map((key) => {
      const meta = schema.value[key] || {}
      return {
        key,
        meta,
        currentValue: currentSettings.value[key],
        overrideValue: overrides.value[key],
      }
    }),
)

function normalizeInput(meta, value) {
  if (meta.type === 'bool') {
    if (typeof value === 'boolean') return value
    const normalized = String(value).trim().toLowerCase()
    return ['true', '1', 'yes', 'y', 'on'].includes(normalized)
  }
  if (meta.type === 'int') {
    return Number.parseInt(String(value), 10)
  }
  if (meta.type === 'float') {
    return Number.parseFloat(String(value))
  }
  return String(value)
}

function resetMessages() {
  error.value = ''
  success.value = ''
}

async function loadSettings() {
  if (!isAdmin.value) return
  loading.value = true
  resetMessages()
  try {
    const response = await apiRequest('gateway', '/api/v1/admin/runtime-settings')
    schema.value = response.data?.schema || {}
    currentSettings.value = response.data?.settings || {}
    overrides.value = response.data?.overrides || {}

    Object.keys(editBuffer).forEach((key) => delete editBuffer[key])
    Object.entries(currentSettings.value).forEach(([key, value]) => {
      editBuffer[key] = value
    })
  } catch (err) {
    error.value = err?.data?.detail || err?.message || t('failedLoad')
  } finally {
    loading.value = false
  }
}

async function saveSettings() {
  if (!isAdmin.value) return
  saving.value = true
  resetMessages()
  try {
    const updates = {}
    Object.entries(schema.value || {}).forEach(([key, meta]) => {
      const rawValue = editBuffer[key]
      const normalized = normalizeInput(meta, rawValue)
      updates[key] = normalized
    })

    const response = await apiRequest('gateway', '/api/v1/admin/runtime-settings', {
      method: 'PUT',
      body: {
        updates,
      },
    })

    currentSettings.value = response.data?.settings || currentSettings.value
    success.value = t('settingsSaved')
  } catch (err) {
    const details = err?.data?.detail
    if (details?.validation_errors) {
      error.value = JSON.stringify(details.validation_errors, null, 2)
    } else {
      error.value = err?.data?.detail || err?.message || t('failedSave')
    }
  } finally {
    saving.value = false
  }
}

async function resetToDefaults() {
  if (!isAdmin.value) return
  resetting.value = true
  resetMessages()
  try {
    const response = await apiRequest('gateway', '/api/v1/admin/runtime-settings/reset', {
      method: 'POST',
    })
    currentSettings.value = response.data?.settings || {}
    overrides.value = {}
    Object.entries(currentSettings.value).forEach(([key, value]) => {
      editBuffer[key] = value
    })
    success.value = t('settingsReset')
  } catch (err) {
    error.value = err?.data?.detail || err?.message || t('failedReset')
  } finally {
    resetting.value = false
  }
}

function restoreCurrent() {
  Object.entries(currentSettings.value).forEach(([key, value]) => {
    editBuffer[key] = value
  })
  resetMessages()
}

onMounted(() => {
  void loadSettings()
})
</script>

<template>
  <div class="space-y-4">
    <SectionHeader
      title="Admin Runtime Settings"
      :subtitle="t('subtitle')"
    >
      <button class="btn-primary" :disabled="loading || !isAdmin" @click="loadSettings">
        {{ loading ? t('loading') : t('reload') }}
      </button>
    </SectionHeader>

    <section v-if="!isAdmin" class="panel border-amber-200 bg-amber-50 p-4">
      <p class="text-sm text-amber-700">
        {{ t('adminOnly') }}
      </p>
    </section>

    <template v-else>
      <section class="panel p-4">
        <div class="mb-3 flex flex-wrap gap-2">
          <button class="btn-primary" :disabled="saving || loading" @click="saveSettings">
            {{ saving ? t('saving') : t('saveAll') }}
          </button>
          <button class="btn-secondary" :disabled="saving || loading" @click="restoreCurrent">
            {{ t('restoreCurrent') }}
          </button>
          <button class="btn-danger" :disabled="resetting || loading" @click="resetToDefaults">
            {{ resetting ? t('resetting') : t('resetDefaults') }}
          </button>
        </div>

        <p v-if="success" class="rounded-xl border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {{ success }}
        </p>
        <pre v-if="error" class="code-block mt-3 border border-rose-300 !bg-rose-950/95 !text-rose-100">{{ error }}</pre>
      </section>

      <section class="panel p-4">
        <div class="table-shell">
          <table>
            <thead>
              <tr>
                <th>{{ t('key') }}</th>
                <th>{{ t('type') }}</th>
                <th>{{ t('scope') }}</th>
                <th>{{ t('description') }}</th>
                <th>{{ t('value') }}</th>
                <th>{{ t('current') }}</th>
                <th>{{ t('override') }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in settingEntries" :key="item.key">
                <td class="font-mono text-xs">{{ item.key }}</td>
                <td>{{ item.meta.type }}</td>
                <td>{{ item.meta.scope || '-' }}</td>
                <td class="whitespace-normal">
                  {{ describeSetting(item.meta) }}
                  <div class="text-xs text-slate-500">
                    min: {{ item.meta.min ?? '-' }}, max: {{ item.meta.max ?? '-' }}
                  </div>
                </td>
                <td>
                  <input
                    v-if="item.meta.type !== 'bool'"
                    v-model="editBuffer[item.key]"
                    class="form-input font-mono text-xs"
                  />
                  <select v-else v-model="editBuffer[item.key]" class="form-input text-xs">
                    <option :value="true">true</option>
                    <option :value="false">false</option>
                  </select>
                </td>
                <td class="font-mono text-xs">{{ item.currentValue }}</td>
                <td class="font-mono text-xs">{{ item.overrideValue ?? '-' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </template>
  </div>
</template>
