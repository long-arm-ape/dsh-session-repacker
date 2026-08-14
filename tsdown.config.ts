/** dsh-session-repacker 构建：Node ESM（服务端插件，无 client bundle）。 */
export default [
  {
    entry: ['src/index.mjs'],
    format: 'esm',
    platform: 'node',
    target: 'es2024',
    outDir: 'lib',
    clean: true,
  },
]
