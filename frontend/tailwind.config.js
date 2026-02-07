/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{vue,js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        display: ['Space Grotesk', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        body: ['Manrope', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        mono: ['IBM Plex Mono', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      colors: {
        brand: {
          50: '#f2fbfb',
          100: '#d8f4f3',
          200: '#afe8e6',
          300: '#7fd7d3',
          400: '#4ebfb9',
          500: '#2e9f9a',
          600: '#257f7b',
          700: '#1f6562',
          800: '#1d5150',
          900: '#1a4443',
        },
      },
      boxShadow: {
        panel: '0 18px 48px -26px rgba(14, 74, 72, 0.55)',
      },
      backgroundImage: {
        'aurora': 'radial-gradient(circle at 15% 15%, rgba(46,159,154,0.16), transparent 34%), radial-gradient(circle at 85% 0%, rgba(252,163,17,0.12), transparent 40%), linear-gradient(180deg, #f4f8f8 0%, #eff5f5 100%)',
      },
    },
  },
  plugins: [],
}
