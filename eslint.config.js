import eslint from "@eslint/js";
import prettier from "eslint-config-prettier";
import tseslint from "typescript-eslint";

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  prettier,
  {
    ignores: ["**/dist/", "**/node_modules/", "**/*.js", "**/*.mjs", "!eslint.config.js"],
  },
  {
    rules: {
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/no-explicit-any": "warn",
      // §2.3 hard rule — belt and braces alongside scripts/check-no-deep-imports.sh, which is the
      // CI-enforced gate. This catches it in the editor.
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            {
              group: ["@nestjs/*/dist/*", "@nestjs/*/internal/*"],
              message:
                "No deep imports into Nest internals (§2.3). Use a public entry point, or re-declare a minimal structural type locally.",
            },
          ],
        },
      ],
    },
  },
);
