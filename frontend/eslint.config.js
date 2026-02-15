import js from "@eslint/js";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";

export default tseslint.config(
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    plugins: { "react-hooks": reactHooks },
    rules: {
      ...reactHooks.configs.recommended.rules,
      // Allow unused vars prefixed with underscore.
      "@typescript-eslint/no-unused-vars": [
        "warn",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      // We use explicit `any` intentionally in some places.
      "@typescript-eslint/no-explicit-any": "off",
      // Too strict for data-fetching sync patterns (e.g. syncing server
      // preferences into local state on first load).
      "react-hooks/set-state-in-effect": "off",
    },
  },
  {
    ignores: ["src/api/gen/**", "dist/**"],
  },
);
