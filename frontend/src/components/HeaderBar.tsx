import { StatPill } from "./StatPill";
import { UserMenu } from "./UserMenu";
import type { Theme } from "../utils";
import { useThemeClass } from "../hooks/useThemeClass";

interface HeaderBarProps {
  dark: boolean;
  theme: Theme;
  setTheme: (t: Theme) => void;
  statsLoading: boolean;
  totalRecords: bigint;
  totalStores: bigint;
  sealedChunks: bigint;
  totalBytes: bigint;
  inspectorGlow: boolean;
  onShowHelp: () => void;
  onShowInspector: () => void;
  onShowSettings: () => void;
  currentUser: { username: string; role: string } | null;
  onChangePassword: () => void;
  onLogout: () => void;
}

export function HeaderBar({
  dark,
  theme,
  setTheme,
  statsLoading,
  totalRecords,
  totalStores,
  sealedChunks,
  totalBytes,
  onShowHelp,
  inspectorGlow,
  onShowInspector,
  onShowSettings,
  currentUser,
  onChangePassword,
  onLogout,
}: HeaderBarProps) {
  const c = useThemeClass(dark);

  return (
    <header
      className={`flex items-center justify-between px-7 py-3.5 border-b ${c("border-ink-border-subtle bg-ink", "border-light-border-subtle bg-light-raised")}`}
    >
      <div className="flex items-center gap-3">
        <img src="/favicon.svg" alt="GastroLog" className="w-6 h-6" />
        <h1
          className={`font-display text-[1.6em] font-semibold tracking-tight leading-none ${c("text-text-bright", "text-light-text-bright")}`}
        >
          GastroLog
        </h1>
      </div>

      <div className="flex items-center gap-6">
        {/* Stats ribbon */}
        <div className="flex items-center gap-5">
          <StatPill
            label="Records"
            value={statsLoading ? "..." : totalRecords.toLocaleString()}
            dark={dark}
          />
          <span
            className={`text-xs ${c("text-ink-border", "text-light-border")}`}
          >
            |
          </span>
          <StatPill
            label="Stores"
            value={statsLoading ? "..." : totalStores.toString()}
            dark={dark}
          />
          <span
            className={`text-xs ${c("text-ink-border", "text-light-border")}`}
          >
            |
          </span>
          <StatPill
            label="Sealed"
            value={statsLoading ? "..." : sealedChunks.toString()}
            dark={dark}
          />
          <span
            className={`text-xs ${c("text-ink-border", "text-light-border")}`}
          >
            |
          </span>
          <StatPill
            label="Storage"
            value={
              statsLoading
                ? "..."
                : `${(Number(totalBytes) / 1024 / 1024).toFixed(1)} MB`
            }
            dark={dark}
          />
        </div>

        <button
          onClick={() => {
            const cycle: Theme[] = ["dark", "light", "system"];
            const next = cycle[(cycle.indexOf(theme) + 1) % cycle.length]!;
            setTheme(next);
          }}
          aria-label={`Theme: ${theme}`}
          title={`Theme: ${theme}`}
          className={`w-7 h-7 flex items-center justify-center text-sm rounded transition-all duration-200 ${c(
            "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )}`}
        >
          {theme === "dark"
            ? "\u263E"
            : theme === "light"
              ? "\u2600"
              : "\u25D1"}
        </button>

        <button
          onClick={onShowHelp}
          aria-label="Help"
          title="Help"
          className={`w-7 h-7 flex items-center justify-center rounded transition-all duration-200 ${c(
            "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )}`}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="w-4 h-4"
          >
            <circle cx="12" cy="12" r="10" />
            <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
        </button>

        <button
          onClick={onShowInspector}
          aria-label="Inspector"
          title="Inspector"
          className={`w-7 h-7 flex items-center justify-center rounded transition-all duration-500 ${c(
            "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )} ${inspectorGlow ? "text-copper drop-shadow-[0_0_4px_var(--color-copper)]" : ""}`}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="w-4 h-4"
          >
            <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
          </svg>
        </button>

        <button
          onClick={onShowSettings}
          aria-label="Settings"
          title="Settings"
          className={`w-7 h-7 flex items-center justify-center rounded transition-all duration-200 ${c(
            "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
            "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
          )}`}
        >
          <svg
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className="w-4 h-4"
          >
            <circle cx="12" cy="12" r="3" />
            <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
          </svg>
        </button>

        {currentUser && (
          <UserMenu
            username={currentUser.username}
            role={currentUser.role}
            dark={dark}
            onChangePassword={onChangePassword}
            onLogout={onLogout}
          />
        )}
      </div>
    </header>
  );
}
