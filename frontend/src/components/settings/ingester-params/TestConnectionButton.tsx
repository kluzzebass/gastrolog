import { useState } from "react";
import { useTestIngester } from "../../../api/hooks/useIngesters";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { isIngesterParamsValid } from "./index";

export function TestConnectionButton({
  type,
  params,
  dark,
}: Readonly<{
  type: string;
  params: Record<string, string>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const testIngester = useTestIngester();
  const valid = isIngesterParamsValid(type, params);
  const [testResult, setTestResult] = useState<{
    success: boolean;
    message: string;
  } | null>(null);

  return (
    <div className="flex items-center gap-3">
      <button
        type="button"
        disabled={testIngester.isPending || !valid}
        onClick={() => {
          setTestResult(null);
          testIngester.mutate(
            { type, params },
            {
              onSuccess: (resp) => {
                setTestResult({
                  success: resp.success,
                  message: resp.message,
                });
              },
              onError: (err) => {
                setTestResult({
                  success: false,
                  message: err instanceof Error ? err.message : String(err),
                });
              },
            },
          );
        }}
        className={`px-3 py-1.5 text-[0.8em] font-medium rounded border transition-colors ${c(
          "bg-ink-surface border-ink-border text-text-bright hover:border-copper-dim disabled:opacity-50",
          "bg-light-surface border-light-border text-light-text-bright hover:border-copper disabled:opacity-50",
        )}`}
      >
        {testIngester.isPending ? "Testing..." : "Test Connection"}
      </button>
      {testResult && (
        <span
          className={`text-[0.8em] ${
            testResult.success
              ? c("text-green-400", "text-green-600")
              : c("text-red-400", "text-red-600")
          }`}
        >
          {testResult.message}
        </span>
      )}
    </div>
  );
}
