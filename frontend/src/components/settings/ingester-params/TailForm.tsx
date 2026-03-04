import { FormField, TextInput, TextArea, ExampleValues } from "../FormField";
import { useThemeClass } from "../../../hooks/useThemeClass";
import type { SubFormProps } from "./types";

export function TailForm({
  params,
  onChange,
  dark,
  defaults: d,
}: Readonly<SubFormProps>) {
  const c = useThemeClass(dark);

  // Convert between JSON array and newline-separated text.
  let text = "";
  try {
    const raw = params["paths"];
    if (raw) text = (JSON.parse(raw) as string[]).join("\n");
  } catch {
    // invalid JSON — show raw value so user can fix it
    text = params["paths"] ?? "";
  }

  const handleTextChange = (value: string) => {
    const lines = value
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean);
    onChange({ ...params, paths: lines.length > 0 ? JSON.stringify(lines) : "" });
  };

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-1">
        <label
          htmlFor="file-patterns"
          className={`text-[0.8em] font-medium ${c("text-text-muted", "text-light-text-muted")}`}
        >
          File Patterns
        </label>
        <p
          className={`text-[0.7em] mb-1.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Glob patterns for files to tail, one per line. Supports ** for
          recursive matching.
        </p>
        <TextArea
          value={text}
          onChange={handleTextChange}
          placeholder=""
          rows={3}
          dark={dark}
        />
        <ExampleValues
          examples={["/var/log/**/*.log", "/var/log/syslog", "/var/log/auth.log"]}
          value={text}
          onChange={handleTextChange}
          dark={dark}
        />
      </div>

      <FormField
        label="Poll Interval"
        description="How often to re-scan for new files and save bookmarks (0 to disable)"
        dark={dark}
      >
        <TextInput
          value={params["poll_interval"] ?? ""}
          onChange={(v) => onChange({ ...params, poll_interval: v })}
          placeholder={d["poll_interval"] ?? ""}
          dark={dark}
          mono
          examples={["30s", "1m", "5m"]}
        />
      </FormField>
    </div>
  );
}
