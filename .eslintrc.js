module.exports = {
  env: {
    es6: true,
    browser: true,
  },
  extends: 'eslint:recommended',
  parserOptions: {
    sourceType: 'module',
    ecmaVersion: 2020,
  },
  globals: {
    BigInt: true,
  },
  rules: {
    indent: 0,
    'linebreak-style': ['error', 'unix'],
    semi: ['error', 'always'],
    'no-console': 0,
    'no-unused-expressions': ['error', { allowShortCircuit: true }],
    'no-unused-vars': 'warn',
  },
};
