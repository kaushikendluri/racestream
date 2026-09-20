/**
 * RaceStream design tokens.
 *
 * The reference is a race-control wall and a trading terminal: dark, dense,
 * quiet until something changes. Colour carries meaning here - it is reserved
 * for status, telemetry emphasis and interaction, and is never decorative.
 * That is why there is exactly one accent hue and four status hues, and why the
 * surface scale is almost neutral: any colour on screen should mean something.
 */

/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        /* Surfaces: a tight, near-neutral ramp with a trace of blue so it reads
         * as instrument glass rather than muddy grey. Each step is a deliberate
         * elevation level, not an arbitrary shade. */
        surface: {
          base: '#08090B',   // page background, almost black
          raised: '#0D0F12', // panels
          card: '#111418',   // cards on panels
          inset: '#16191E',  // wells, table headers
          hover: '#1C2027',
          active: '#22272F',
        },
        line: {
          subtle: '#1B1F26',  // hairlines between dense rows
          DEFAULT: '#252A33', // standard 1px border
          strong: '#333A45',  // focused or selected
        },
        content: {
          primary: '#E8EBF0',   // headings, live values
          secondary: '#9AA4B2', // labels
          tertiary: '#6B7483',  // units, axis ticks
          disabled: '#454C58',
        },
        /* Motorsport red, desaturated so it can sit on screen for an hour
         * without fatiguing. Used for brand and primary action only - status
         * red is a separate, hotter hue so the two are never confused. */
        accent: {
          DEFAULT: '#E10600',
          hover: '#FF1801',
          muted: '#8F0A02',
          subtle: 'rgba(225, 6, 0, 0.12)',
        },
        status: {
          healthy: '#00D48A',
          warning: '#FFB020',
          error: '#FF4757',
          info: '#3B9EFF',
          neutral: '#6B7483',
        },
        /* Tyre compounds use the real broadcast colours: an engineer reading
         * this screen already knows them, and inventing new ones would cost
         * recognition for no gain. */
        tyre: {
          soft: '#FF3333',
          medium: '#FFD700',
          hard: '#EFEFEF',
          intermediate: '#00D34D',
          wet: '#1E90FF',
          unknown: '#6B7483',
        },
        delta: {
          faster: '#00D48A',  // green: gained time
          slower: '#FFB020',  // amber, not red: losing time is not an error
          neutral: '#6B7483',
        },
      },
      fontFamily: {
        /* Human-readable UI in a grotesque; every machine-generated number in a
         * monospace. The split is the point - it lets the eye separate label
         * from measurement without reading either. */
        sans: ['Inter var', 'Inter', 'system-ui', '-apple-system', 'Segoe UI', 'sans-serif'],
        mono: ['JetBrains Mono', 'SF Mono', 'Menlo', 'Consolas', 'monospace'],
      },
      fontSize: {
        /* A compact type scale. Dense screens need small sizes that stay legible,
         * so line heights are tuned per step rather than left to a ratio. */
        '2xs': ['0.625rem', { lineHeight: '0.875rem', letterSpacing: '0.04em' }],
        xs: ['0.6875rem', { lineHeight: '1rem', letterSpacing: '0.02em' }],
        sm: ['0.8125rem', { lineHeight: '1.125rem' }],
        base: ['0.875rem', { lineHeight: '1.25rem' }],
        lg: ['1rem', { lineHeight: '1.5rem' }],
        xl: ['1.25rem', { lineHeight: '1.75rem' }],
        '2xl': ['1.5rem', { lineHeight: '2rem', letterSpacing: '-0.01em' }],
        '3xl': ['2rem', { lineHeight: '2.25rem', letterSpacing: '-0.02em' }],
        /* Reserved for the one or two figures that must be readable across a
         * room: lap time, position. */
        hero: ['2.75rem', { lineHeight: '1', letterSpacing: '-0.03em' }],
      },
      letterSpacing: {
        label: '0.08em', // uppercase section labels
      },
      borderRadius: {
        sm: '4px',
        DEFAULT: '8px',
        md: '10px',
        lg: '12px',
        xl: '14px',
      },
      boxShadow: {
        /* Shadows are barely there. On a near-black background, elevation reads
         * through border contrast far better than through a drop shadow. */
        panel: '0 1px 2px rgba(0, 0, 0, 0.4)',
        raised: '0 4px 12px rgba(0, 0, 0, 0.5)',
        overlay: '0 16px 48px rgba(0, 0, 0, 0.7)',
        'focus-accent': '0 0 0 2px #08090B, 0 0 0 4px rgba(225, 6, 0, 0.6)',
      },
      spacing: {
        rail: '13rem',          // expanded navigation rail
        'rail-collapsed': '3.5rem',
        header: '3.5rem',
      },
      animation: {
        /* Motion communicates a state change and then stops. Nothing loops
         * except the live indicator, which is genuinely reporting liveness. */
        'pulse-live': 'pulse-live 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'flash-update': 'flash-update 600ms ease-out',
        'position-gain': 'position-gain 900ms ease-out',
        'position-loss': 'position-loss 900ms ease-out',
        'skeleton': 'skeleton 1.4s ease-in-out infinite',
      },
      keyframes: {
        'pulse-live': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.35' },
        },
        'flash-update': {
          '0%': { backgroundColor: 'rgba(59, 158, 255, 0.18)' },
          '100%': { backgroundColor: 'transparent' },
        },
        'position-gain': {
          '0%': { backgroundColor: 'rgba(0, 212, 138, 0.22)' },
          '100%': { backgroundColor: 'transparent' },
        },
        'position-loss': {
          '0%': { backgroundColor: 'rgba(255, 176, 32, 0.22)' },
          '100%': { backgroundColor: 'transparent' },
        },
        skeleton: {
          '0%, 100%': { opacity: '0.35' },
          '50%': { opacity: '0.6' },
        },
      },
      transitionDuration: {
        fast: '120ms',
        DEFAULT: '180ms',
      },
      gridTemplateColumns: {
        timing: 'minmax(2.5rem,auto) minmax(4rem,1fr) minmax(3rem,auto) repeat(3, minmax(4.5rem,auto))',
      },
    },
  },
  plugins: [],
};
