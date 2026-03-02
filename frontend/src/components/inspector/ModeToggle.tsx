import { useThemeClass } from "../../hooks/useThemeClass";

export type InspectorMode = "nodes" | "entities";

interface ModeToggleProps {
  mode: InspectorMode;
  onChange: (mode: InspectorMode) => void;
  dark: boolean;
}

export function ModeToggle({ mode, onChange, dark }: Readonly<ModeToggleProps>) {
  const c = useThemeClass(dark);

  return (
    <div
      className={`flex rounded-lg p-0.5 ${c("bg-ink-hover", "bg-light-hover")}`}
    >
      <ToggleButton
        active={mode === "nodes"}
        onClick={() => onChange("nodes")}
        c={c}
      >
        Nodes
      </ToggleButton>
      <ToggleButton
        active={mode === "entities"}
        onClick={() => onChange("entities")}
        c={c}
      >
        Entities
      </ToggleButton>
    </div>
  );
}

function ToggleButton({
  active,
  onClick,
  c,
  children,
}: Readonly<{
  active: boolean;
  onClick: () => void;
  c: (d: string, l: string) => string;
  children: React.ReactNode;
}>) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`flex-1 px-3 py-1 text-[0.8em] font-medium rounded-md transition-colors ${
        active
          ? `bg-copper/15 text-copper`
          : c(
              "text-text-muted hover:text-text-bright",
              "text-light-text-muted hover:text-light-text-bright",
            )
      }`}
    >
      {children}
    </button>
  );
}
