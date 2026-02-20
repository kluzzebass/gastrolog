import { Dialog, CloseButton } from "./Dialog";
import { useThemeClass } from "../hooks/useThemeClass";
import type { Theme } from "../utils";
import type { HighlightMode } from "../hooks/useThemeSync";

interface PreferencesDialogProps {
  dark: boolean;
  theme: Theme;
  setTheme: (t: Theme) => void;
  highlightMode: HighlightMode;
  setHighlightMode: (m: HighlightMode) => void;
  onClose: () => void;
}

export function PreferencesDialog({
  dark,
  theme,
  setTheme,
  highlightMode,
  setHighlightMode,
  onClose,
}: Readonly<PreferencesDialogProps>) {
  const c = useThemeClass(dark);

  return (
    <Dialog onClose={onClose} ariaLabel="Preferences" dark={dark} size="sm">
      <CloseButton onClick={onClose} dark={dark} />
      <h2
        className={`font-display text-lg font-semibold mb-5 ${c("text-text-bright", "text-light-text-bright")}`}
      >
        Preferences
      </h2>

      <div className="space-y-5">
        <PreferenceGroup label="Theme" dark={dark}>
          <SegmentedControl
            dark={dark}
            options={[
              { value: "dark", label: "Dark" },
              { value: "light", label: "Light" },
              { value: "system", label: "System" },
            ]}
            value={theme}
            onChange={(v) => setTheme(v as Theme)}
          />
        </PreferenceGroup>

        <PreferenceGroup label="Syntax highlighting" dark={dark}>
          <SegmentedControl
            dark={dark}
            options={[
              { value: "full", label: "Full" },
              { value: "subtle", label: "Subtle" },
            ]}
            value={highlightMode}
            onChange={(v) => setHighlightMode(v as HighlightMode)}
          />
        </PreferenceGroup>
      </div>
    </Dialog>
  );
}

function PreferenceGroup({
  label,
  dark,
  children,
}: Readonly<{
  label: string;
  dark: boolean;
  children: React.ReactNode;
}>) {
  const c = useThemeClass(dark);
  return (
    <div>
      <div
        className={`text-[0.75em] font-medium uppercase tracking-widest mb-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        {label}
      </div>
      {children}
    </div>
  );
}

function SegmentedControl<T extends string>({
  dark,
  options,
  value,
  onChange,
}: Readonly<{
  dark: boolean;
  options: { value: T; label: string }[];
  value: T;
  onChange: (v: T) => void;
}>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`inline-flex rounded border ${c("border-ink-border bg-ink", "border-light-border bg-light-bg")}`}
    >
      {options.map((opt) => {
        const active = opt.value === value;
        return (
          <button
            key={opt.value}
            onClick={() => onChange(opt.value)}
            className={`px-3.5 py-1.5 text-[0.8em] font-medium transition-colors first:rounded-l last:rounded-r ${
              active
                ? c(
                    "bg-copper/15 text-copper border-copper",
                    "bg-copper/10 text-copper border-copper",
                  )
                : c(
                    "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                    "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                  )
            }`}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}
