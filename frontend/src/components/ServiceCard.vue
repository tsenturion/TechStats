<script setup>
import StatusBadge from './StatusBadge.vue'

defineProps({
  name: { type: String, required: true },
  baseUrl: { type: String, default: '' },
  status: { type: String, default: 'unknown' },
  details: { type: String, default: '' },
  latency: { type: [String, Number], default: '' },
})
</script>

<template>
  <article class="panel p-4">
    <div class="mb-2 flex items-center justify-between gap-2">
      <h3 class="font-display text-base font-semibold text-slate-900">{{ name }}</h3>
      <StatusBadge :status="status" />
    </div>
    <p class="mb-1 truncate font-mono text-xs text-slate-500">{{ baseUrl }}</p>
    <p v-if="details" class="text-sm text-slate-700">{{ details }}</p>
    <p v-if="latency" class="mt-2 text-xs text-slate-500">latency: {{ latency }}</p>
  </article>
</template>
