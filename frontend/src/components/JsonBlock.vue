<script setup>
import { computed } from 'vue'

const props = defineProps({
  title: {
    type: String,
    default: '',
  },
  value: {
    type: [Object, Array, String, Number, Boolean, null],
    default: null,
  },
  maxHeight: {
    type: String,
    default: '20rem',
  },
})

const rendered = computed(() => {
  if (props.value === null || props.value === undefined) {
    return ''
  }
  if (typeof props.value === 'string') {
    return props.value
  }
  try {
    return JSON.stringify(props.value, null, 2)
  } catch {
    return String(props.value)
  }
})

function copyPayload() {
  if (!rendered.value) return
  navigator.clipboard.writeText(rendered.value)
}
</script>

<template>
  <section class="panel p-4">
    <div class="mb-3 flex items-center justify-between gap-3">
      <h3 v-if="title" class="panel-title text-base">{{ title }}</h3>
      <button class="btn-secondary px-3 py-1.5 text-xs" @click="copyPayload">Copy</button>
    </div>
    <pre class="code-block" :style="{ maxHeight }">{{ rendered || 'No data yet' }}</pre>
  </section>
</template>
