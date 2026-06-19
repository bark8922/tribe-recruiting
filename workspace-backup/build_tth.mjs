import { build } from 'vite'
import react from '@vitejs/plugin-react'

await build({
  plugins: [react()],
  build: {
    outDir: '/tmp/tth_dist',
    emptyOutDir: true,
  },
})
