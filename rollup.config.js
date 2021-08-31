import commonjs from '@rollup/plugin-commonjs';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import { terser } from 'rollup-plugin-terser';

import pkg from './package.json';

export default [
  {
    input: 'src/index.js',
    output: {
      name: 'swift-multi-web',
      file: pkg.browser,
      format: 'umd',
      sourcemap: true,
    },
    plugins: [
      commonjs(),
      nodeResolve({ preferBuiltins: false }),
      terser({ format: { comments: false } }),
    ],
  },
];
