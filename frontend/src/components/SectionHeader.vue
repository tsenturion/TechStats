<script setup>
defineProps({
  title: {
    type: String,
    required: true,
  },
  subtitle: {
    type: String,
    default: '',
  },
})
</script>

<template>
  <div class="mb-4 flex flex-wrap items-end justify-between gap-3">
    <div>
      <h2 class="font-display text-2xl font-semibold tracking-tight text-slate-900">{{ title }}</h2>
      <p v-if="subtitle" class="mt-1 text-sm text-slate-600">{{ subtitle }}</p>
    </div>
    <slot />
  </div>
</template>
