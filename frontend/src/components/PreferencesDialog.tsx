import { Dialog, CloseButton } from "./Dialog";
import { useThemeClass } from "../hooks/useThemeClass";
import type { Theme, Palette } from "../utils";
import type { HighlightMode } from "../hooks/useThemeSync";

interface PreferencesDialogProps {
  dark: boolean;
  theme: Theme;
  setTheme: (t: Theme) => void;
  highlightMode: HighlightMode;
  setHighlightMode: (m: HighlightMode) => void;
  palette: Palette;
  setPalette: (p: Palette) => void;
  onClose: () => void;
}

const PALETTE_OPTIONS: { value: Palette; label: string; accent: string; bg: string }[] = [
  { value: "observatory", label: "Observatory", accent: "#c8875c", bg: "#0d0f12" },
  { value: "nord", label: "Nord", accent: "#88c0d0", bg: "#2e3440" },
  { value: "solarized", label: "Solarized", accent: "#268bd2", bg: "#002b36" },
  { value: "dracula", label: "Dracula", accent: "#bd93f9", bg: "#282a36" },
  { value: "catppuccin", label: "Catppuccin", accent: "#cba6f7", bg: "#1e1e2e" },
  { value: "gruvbox", label: "Gruvbox", accent: "#fe8019", bg: "#282828" },
  { value: "tokyonight", label: "Tokyo Night", accent: "#7aa2f7", bg: "#1a1b26" },
  { value: "rosepine", label: "Rosé Pine", accent: "#ebbcba", bg: "#191724" },
  { value: "everforest", label: "Everforest", accent: "#a7c080", bg: "#2d353b" },
  { value: "synthwave", label: "Synthwave", accent: "#f72585", bg: "#1a1028" },
];

export function PreferencesDialog({
  dark,
  theme,
  setTheme,
  highlightMode,
  setHighlightMode,
  palette,
  setPalette,
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
        <PreferenceGroup label="Palette" dark={dark}>
          <div className="flex flex-wrap gap-2">
            {PALETTE_OPTIONS.map((opt) => {
              const active = opt.value === palette;
              return (
                <button
                  key={opt.value}
                  onClick={() => setPalette(opt.value)}
                  title={opt.label}
                  className={`flex flex-col items-center gap-1.5 px-3.5 py-2.5 rounded border transition-colors ${
                    active
                      ? c(
                          "border-copper bg-copper/10",
                          "border-copper bg-copper/8",
                        )
                      : c(
                          "border-ink-border hover:border-ink-hover bg-transparent",
                          "border-light-border hover:border-light-hover bg-transparent",
                        )
                  }`}
                >
                  <div className="flex gap-0.5">
                    <div
                      className="w-4 h-4 rounded-sm"
                      style={{ backgroundColor: opt.bg }}
                    />
                    <div
                      className="w-4 h-4 rounded-sm"
                      style={{ backgroundColor: opt.accent }}
                    />
                  </div>
                  <span
                    className={`text-[0.7em] font-medium ${
                      active
                        ? "text-copper"
                        : c("text-text-muted", "text-light-text-muted")
                    }`}
                  >
                    {opt.label}
                  </span>
                </button>
              );
            })}
          </div>
        </PreferenceGroup>

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
            className={`px-4 py-2.5 text-[0.8em] font-medium transition-colors first:rounded-l last:rounded-r ${
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
