import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings, useTestHTTPLookup } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { useLookupCrud } from "./useLookupCrud";
import { FormField, TextInput, ParamsEditor } from "../FormField";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { AddFormCard } from "../AddFormCard";
import { StringListEditor, ParameterListEditor } from "./FormHelpers";
import { type HTTPLookupDraft, type LookupSectionProps, emptyHttpDraft, httpLookupEqual } from "./types";
import type { HTTPLookupEntry } from "../../../api/gen/gastrolog/v1/system_pb";

function serializeHttpLookups(lookups: HTTPLookupDraft[]) {
  return lookups
    .filter((h) => h.name && h.urlTemplate)
    .map((h) => ({
      name: h.name,
      urlTemplate: h.urlTemplate,
      headers: h.headers,
      responsePaths: h.responsePaths.filter(Boolean),
      parameters: h.parameters.filter((p) => p.name),
      timeout: h.timeout || undefined,
      cacheTtl: h.cacheTtl || undefined,
      cacheSize: h.cacheSize || undefined,
    }));
}

export function HttpAddForm({
  dark,
  addToast,
  onCreated,
  onCancel,
  existingLookups,
  namePlaceholder,
}: Omit<LookupSectionProps, "managedFiles" | "uploadFile"> & {
  onCreated: (draft: HTTPLookupDraft) => void;
  onCancel: () => void;
  existingLookups: HTTPLookupDraft[];
  namePlaceholder: string;
}) {
  const putConfig = usePutSettings();
  const [draft, setDraft] = useState<HTTPLookupDraft>(() => emptyHttpDraft());

  const handleCreate = async () => {
    const final = { ...draft, name: draft.name.trim() || namePlaceholder };
    if (!final.name) return;
    const updated = [...existingLookups, final];
    try {
      await putConfig.mutateAsync({ lookup: { httpLookups: serializeHttpLookups(updated) } });
      onCreated(final);
      addToast(`HTTP lookup "${final.name}" created`, "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to create HTTP lookup", "error");
    }
  };

  return (
    <AddFormCard
      dark={dark}
      onCancel={onCancel}
      onCreate={handleCreate}
      isPending={putConfig.isPending}
      createDisabled={(!draft.name.trim() && !namePlaceholder) || !draft.urlTemplate.trim()}
      typeBadge="http"
    >
      <FormField label="Name" description="Registry name used in queries, e.g. | lookup users" dark={dark}>
        <TextInput value={draft.name} onChange={(v) => setDraft((d) => ({ ...d, name: v }))} placeholder={namePlaceholder} dark={dark} mono />
      </FormField>
      <FormField label="URL Template" description="Use {param} placeholders matching parameter names below. Fields map positionally in queries." dark={dark}>
        <TextInput value={draft.urlTemplate} onChange={(v) => setDraft((d) => ({ ...d, urlTemplate: v }))} placeholder="" dark={dark} mono />
      </FormField>
      <FormField label="Response Paths" description="JSONPath expressions to extract target objects. Results are merged." dark={dark}>
        <StringListEditor values={draft.responsePaths} onChange={(v) => setDraft((d) => ({ ...d, responsePaths: v }))} placeholder="$.data.user" dark={dark} />
      </FormField>
      <FormField label="Parameters" description="Ordered list of URL template parameters. Names become {name} placeholders." dark={dark}>
        <ParameterListEditor values={draft.parameters} onChange={(v) => setDraft((d) => ({ ...d, parameters: v }))} dark={dark} />
      </FormField>
      <FormField label="Headers" description="Custom HTTP headers (e.g. Authorization)." dark={dark}>
        <ParamsEditor params={draft.headers} onChange={(v) => setDraft((d) => ({ ...d, headers: v }))} dark={dark} />
      </FormField>
      <HttpCachingFields dark={dark} draft={draft} setDraft={setDraft} />
    </AddFormCard>
  );
}

export function HttpCards({
  dark,
  addToast,
  lookups,
  savedLookups,
  onUpdate,
  onDelete,
}: Omit<LookupSectionProps, "managedFiles" | "uploadFile"> & {
  lookups: HTTPLookupDraft[];
  savedLookups: HTTPLookupEntry[];
  onUpdate: (i: number, patch: Partial<HTTPLookupDraft>) => void;
  onDelete: (i: number) => void;
}) {
  const c = useThemeClass(dark);
  const { isDirty, save, handleDelete, putConfig } = useLookupCrud({
    lookups, savedLookups, serialize: serializeHttpLookups, equal: httpLookupEqual,
    lookupKey: "httpLookups", typeLabel: "HTTP", getName: (h) => h.name, onDelete,
  });
  const testLookup = useTestHTTPLookup();
  const { isExpanded, toggle } = useExpandedCards();
  const [testValues, setTestValues] = useState<Record<number, Record<string, string>>>({});
  const [testResults, setTestResults] = useState<
    Record<number, { success: boolean; error?: string; results?: { label: string; value: string; fields: Record<string, string> }[] }>
  >({});

  return (
    <>
      {lookups.map((h, i) => (
        <SettingsCard
          key={`http-${i}`}
          id={h.name || `HTTP Lookup ${i + 1}`}
          typeBadge="http"
          dark={dark}
          expanded={isExpanded(`http-${i}`)}
          onToggle={() => toggle(`http-${i}`)}
          onDelete={() => handleDelete(i)}
          footer={
            <Button
              onClick={() => save(i)}
              disabled={!isDirty(i) || !h.name || !h.urlTemplate || putConfig.isPending}
            >
              {putConfig.isPending ? "Saving..." : "Save"}
            </Button>
          }
        >
          <div className="flex flex-col gap-3">
            <FormField label="Name" description="Registry name used in queries, e.g. | lookup users" dark={dark}>
              <TextInput value={h.name} onChange={(v) => onUpdate(i, { name: v })} placeholder="" dark={dark} mono />
            </FormField>
            <FormField label="URL Template" description="Use {param} placeholders matching parameter names below. Fields map positionally in queries." dark={dark}>
              <TextInput value={h.urlTemplate} onChange={(v) => onUpdate(i, { urlTemplate: v })} placeholder="" dark={dark} mono />
            </FormField>
            <FormField label="Response Paths" description="JSONPath expressions to extract target objects. Results are merged." dark={dark}>
              <StringListEditor values={h.responsePaths} onChange={(v) => onUpdate(i, { responsePaths: v })} placeholder="$.data.user" dark={dark} />
            </FormField>
            <FormField label="Parameters" description="Ordered list of URL template parameters. Names become {name} placeholders. Fields map positionally in queries." dark={dark}>
              <ParameterListEditor values={h.parameters} onChange={(v) => onUpdate(i, { parameters: v })} dark={dark} />
            </FormField>
            <FormField label="Headers" description="Custom HTTP headers (e.g. Authorization)." dark={dark}>
              <ParamsEditor params={h.headers} onChange={(v) => onUpdate(i, { headers: v })} dark={dark} />
            </FormField>
            <HttpCachingFields dark={dark} draft={h} setDraft={(fn) => {
              const updated = typeof fn === "function" ? fn(h) : fn;
              onUpdate(i, updated);
            }} />

            {/* Test section */}
            <div className={`mt-1 pt-3 border-t ${c("border-ink-border", "border-light-border")}`}>
              {h.parameters.length > 0 && (
                <FormField label="Test Values" dark={dark}>
                  <div className="flex flex-col gap-1.5">
                    {h.parameters.filter((p) => p.name).map((p) => (
                      <div key={p.name} className="flex gap-1.5 items-center">
                        <span className={`w-32 text-[0.85em] font-mono shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}>
                          {p.name}
                        </span>
                        <input
                          type="text"
                          value={testValues[i]?.[p.name] ?? ""}
                          onChange={(e) => setTestValues((prev) => ({ ...prev, [i]: { ...prev[i], [p.name]: e.target.value } }))}
                          placeholder={p.description || p.name}
                          className={`flex-1 px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none ${c(
                            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
                          )}`}
                        />
                      </div>
                    ))}
                  </div>
                </FormField>
              )}
              <div className="mt-2">
                <Button
                  variant="ghost"
                  dark={dark}
                  disabled={!h.urlTemplate || testLookup.isPending}
                  onClick={() => {
                    setTestResults((prev) => ({ ...prev, [i]: undefined as never }));
                    testLookup.mutate(
                      {
                        config: {
                          name: h.name,
                          urlTemplate: h.urlTemplate,
                          headers: h.headers,
                          responsePaths: h.responsePaths.filter(Boolean),
                          timeout: h.timeout || undefined,
                          cacheTtl: h.cacheTtl || undefined,
                          cacheSize: h.cacheSize || undefined,
                        },
                        values: testValues[i] ?? {},
                      },
                      {
                        onSuccess: (res) => {
                          setTestResults((prev) => ({
                            ...prev,
                            [i]: { success: res.success, error: res.error, results: res.results },
                          }));
                        },
                        onError: (err) => {
                          setTestResults((prev) => ({
                            ...prev,
                            [i]: { success: false, error: err instanceof Error ? err.message : "Test failed" },
                          }));
                        },
                      },
                    );
                  }}
                >
                  {testLookup.isPending ? "Testing..." : "Test"}
                </Button>
              </div>
              {testResults[i] && (
                <div className={`mt-2 rounded px-3 py-2 text-[0.8em] ${
                  testResults[i].success
                    ? "bg-severity-ok/10 text-severity-ok"
                    : "bg-severity-error/10 text-severity-error"
                }`}>
                  {testResults[i].success ? (() => {
                    const fields = testResults[i].results?.[0]?.fields ?? {};
                    const entries = Object.entries(fields);
                    return entries.length > 0 ? (
                      <div className={`font-mono text-[0.9em] ${c("text-text-bright", "text-light-text-bright")}`}>
                        {entries.map(([k, v]) => (
                          <div key={k}><span className="text-copper">{k}</span> = {v}</div>
                        ))}
                      </div>
                    ) : (
                      <span>no results</span>
                    );
                  })() : (
                    <span>{testResults[i].error}</span>
                  )}
                </div>
              )}
            </div>
          </div>
        </SettingsCard>
      ))}
    </>
  );
}

function HttpCachingFields({ dark, draft, setDraft }: Readonly<{ dark: boolean; draft: HTTPLookupDraft; setDraft: (fn: (d: HTTPLookupDraft) => HTTPLookupDraft) => void }>) {
  return (
    <div className="grid grid-cols-3 gap-3">
      <FormField label="Timeout" dark={dark}>
        <TextInput value={draft.timeout} onChange={(v) => setDraft((d) => ({ ...d, timeout: v }))} placeholder="5s" dark={dark} mono />
      </FormField>
      <FormField label="Cache TTL" dark={dark}>
        <TextInput value={draft.cacheTtl} onChange={(v) => setDraft((d) => ({ ...d, cacheTtl: v }))} placeholder="5m" dark={dark} mono />
      </FormField>
      <FormField label="Cache Size" dark={dark}>
        <TextInput
          value={draft.cacheSize ? String(draft.cacheSize) : ""}
          onChange={(v) => setDraft((d) => ({ ...d, cacheSize: parseInt(v) || 0 }))}
          placeholder="10000"
          dark={dark}
          mono
        />
      </FormField>
    </div>
  );
}
