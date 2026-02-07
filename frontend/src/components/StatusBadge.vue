<script setup>
import { computed } from 'vue'

const props = defineProps({
  status: {
    type: String,
    default: 'unknown',
  },
})

const normalized = computed(() => String(props.status || 'unknown').toLowerCase())

const badgeClass = computed(() => {
  if (['healthy', 'ok', 'ready', 'online', 'alive', 'success'].includes(normalized.value)) {
    return 'badge-ok'
  }
  if (['degraded', 'warning', 'warn', 'starting', 'processing'].includes(normalized.value)) {
    return 'badge-warn'
  }
  if (['error', 'failed', 'unhealthy', 'offline', 'unavailable'].includes(normalized.value)) {
    return 'badge-error'
  }
  return 'badge-neutral'
})
</script>

<template>
  <span class="badge" :class="badgeClass">
    {{ normalized }}
  </span>
</template>
