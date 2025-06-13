import { globalIgnores } from 'eslint/config'
import neostandard from 'neostandard'

const eslint = [
  ...neostandard({ ts: true }),
  globalIgnores(['dist/']),
  {
    files: ['**/*.ts'],
    rules: {
      '@typescript-eslint/consistent-type-imports': ['error', { fixStyle: 'inline-type-imports' }]
    }
  }
]

export default eslint
