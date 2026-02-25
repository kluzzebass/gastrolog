import js from "@eslint/js";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import sonarjs from "eslint-plugin-sonarjs";
import unicorn from "eslint-plugin-unicorn";

export default tseslint.config(
  js.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  sonarjs.configs.recommended,
  unicorn.configs["flat/recommended"],
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: { "react-hooks": reactHooks },
    rules: {
      ...reactHooks.configs.recommended.rules,

      // ── @typescript-eslint ──
      "@typescript-eslint/no-unused-vars": [
        "warn",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/prefer-optional-chain": "warn",
      "@typescript-eslint/no-unnecessary-type-assertion": "warn",
      "@typescript-eslint/no-unnecessary-condition": "warn",
      "@typescript-eslint/no-unnecessary-template-expression": "warn",
      "@typescript-eslint/only-throw-error": "warn",
      "@typescript-eslint/no-base-to-string": "warn",
      "@typescript-eslint/restrict-plus-operands": "warn",
      "@typescript-eslint/require-await": "warn",
      // Too noisy for our `any`-heavy codebase.
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-return": "off",
      // Not useful for this codebase.
      "@typescript-eslint/restrict-template-expressions": "off",
      "@typescript-eslint/no-confusing-void-expression": "off",
      "@typescript-eslint/no-floating-promises": "off",
      "@typescript-eslint/no-misused-promises": "off",
      "@typescript-eslint/unbound-method": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-dynamic-delete": "off",
      // Broken on ESLint 10 — parserOptions destructure fails.
      "@typescript-eslint/no-deprecated": "off",

      // ── react-hooks ──
      "react-hooks/set-state-in-effect": "off",
      "react-hooks/refs": "off",

      // ── sonarjs — keep the good signal as warnings ──
      "sonarjs/cognitive-complexity": ["warn", 15],
      "sonarjs/no-duplicated-branches": "warn",
      "sonarjs/no-nested-conditional": "warn",
      "sonarjs/no-nested-template-literals": "warn",
      "sonarjs/slow-regex": "warn",
      "sonarjs/regex-complexity": "warn",
      "sonarjs/single-character-alternation": "warn",
      "sonarjs/prefer-regexp-exec": "warn",
      "sonarjs/no-hardcoded-ip": "warn",
      "sonarjs/no-dead-store": "warn",
      "sonarjs/no-misleading-array-reverse": "warn",
      "sonarjs/no-identical-functions": "warn",
      "sonarjs/no-useless-react-setstate": "warn",
      "sonarjs/function-return-type": "warn",
      "sonarjs/prefer-read-only-props": "warn",
      "sonarjs/updated-loop-counter": "warn",
      "sonarjs/use-type-alias": "warn",
      "sonarjs/unused-import": "warn",
      "sonarjs/pseudo-random": "off",
      // Overlaps with @typescript-eslint/no-unused-vars.
      "sonarjs/no-unused-vars": "off",
      // Flags intentional sort callbacks.
      "sonarjs/no-alphabetical-sort": "off",
      // Duplicates the TS deprecation system.
      "sonarjs/deprecation": "off",
      // Nested functions are common in React components.
      "sonarjs/no-nested-functions": "off",

      // ── unicorn — keep useful, disable opinionated ──
      "unicorn/prefer-at": "warn",
      "unicorn/no-nested-ternary": "warn",
      "unicorn/no-array-reverse": "warn",
      "unicorn/prefer-dom-node-remove": "warn",
      "unicorn/no-useless-undefined": "warn",
      "unicorn/consistent-existence-index-check": "warn",
      "unicorn/escape-case": "warn",
      "unicorn/prefer-single-call": "warn",
      "unicorn/prefer-set-has": "warn",
      "unicorn/prefer-export-from": "warn",
      "unicorn/prefer-ternary": "warn",
      "unicorn/no-new-array": "warn",
      "unicorn/no-array-reduce": "warn",
      "unicorn/no-immediate-mutation": "warn",
      "unicorn/consistent-function-scoping": "warn",
      // Disabled — too opinionated.
      "unicorn/prevent-abbreviations": "off",
      "unicorn/filename-case": "off",
      "unicorn/no-null": "off",
      "unicorn/prefer-module": "off",
      "unicorn/prefer-top-level-await": "off",
      "unicorn/no-abusive-eslint-disable": "off",
      "unicorn/prefer-query-selector": "off",
      "unicorn/prefer-dom-node-text-content": "off",
      "unicorn/prefer-global-this": "off",
      "unicorn/switch-case-braces": "off",
      "unicorn/numeric-separators-style": "off",
      "unicorn/prefer-string-replace-all": "off",
      "unicorn/prefer-number-properties": "off",
      "unicorn/catch-error-name": "off",
      "unicorn/prefer-string-raw": "off",
      "unicorn/no-array-sort": "off",
      "unicorn/no-negated-condition": "off",
      "unicorn/prefer-spread": "off",
      "unicorn/prefer-bigint-literals": "off",
      "unicorn/prefer-switch": "off",
      "unicorn/prefer-dom-node-append": "off",
      "unicorn/no-for-loop": "off",
      "unicorn/no-array-callback-reference": "off",
    },
  },
  {
    ignores: ["src/api/gen/**", "dist/**"],
  },
);
