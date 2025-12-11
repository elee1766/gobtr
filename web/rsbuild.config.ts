import { defineConfig } from '@rsbuild/core';
import { pluginBabel } from '@rsbuild/plugin-babel';
import { pluginSolid } from '@rsbuild/plugin-solid';

export default defineConfig({
  plugins: [
    pluginBabel({
      include: /\.(?:jsx|tsx)$/,
    }),
    pluginSolid(),
  ],
  source: {
    entry: { index: './src/index.tsx' },
  },
  resolve: {
    alias: {
      '@': './src',
      '%': './src/gen/api',
      // Block accidental React imports
      'react': false,
      'react-dom': false,
    },
  },
  html: {
    template: './index.html',
  },
  output: {
    assetPrefix: '/',
    sourceMap: {
      js: 'source-map',
    },
  },
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
    },
  },
});
