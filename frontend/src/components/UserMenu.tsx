import { useState, useEffect, useRef } from "react";

export function UserMenu({
  username,
  role,
  dark,
  onChangePassword,
  onLogout,
}: {
  username: string;
  role: string;
  dark: boolean;
  onChangePassword: () => void;
  onLogout: () => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  // Close on click outside.
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  // Close on Escape.
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open]);

  const c = (d: string, l: string) => (dark ? d : l);

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        title={username}
        className={`w-7 h-7 flex items-center justify-center rounded transition-all duration-200 ${c(
          "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
          "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
        )} ${open ? c("text-text-muted bg-ink-hover", "text-light-text-muted bg-light-hover") : ""}`}
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
          <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
          <circle cx="12" cy="7" r="4" />
        </svg>
      </button>

      {open && (
        <div
          className={`absolute right-0 top-full mt-1.5 z-50 min-w-48 rounded border shadow-lg ${c(
            "bg-ink-surface border-ink-border",
            "bg-light-surface border-light-border",
          )}`}
        >
          {/* User identity header */}
          <div
            className={`px-3 py-2.5 border-b ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
          >
            <div
              className={`text-[0.85em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}
            >
              {username}
            </div>
            <div
              className={`text-[0.7em] uppercase tracking-widest mt-0.5 ${c("text-text-ghost", "text-light-text-ghost")}`}
            >
              {role}
            </div>
          </div>

          {/* Menu items */}
          <div className="py-1">
            <MenuItem
              dark={dark}
              onClick={() => {
                setOpen(false);
                onChangePassword();
              }}
            >
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className="w-3.5 h-3.5"
              >
                <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
                <path d="M7 11V7a5 5 0 0 1 10 0v4" />
              </svg>
              Change password
            </MenuItem>
            <MenuItem
              dark={dark}
              onClick={() => {
                setOpen(false);
                onLogout();
              }}
            >
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className="w-3.5 h-3.5"
              >
                <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
                <polyline points="16 17 21 12 16 7" />
                <line x1="21" y1="12" x2="9" y2="12" />
              </svg>
              Sign out
            </MenuItem>
          </div>
        </div>
      )}
    </div>
  );
}

function MenuItem({
  dark,
  onClick,
  children,
}: {
  dark: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  const c = (d: string, l: string) => (dark ? d : l);
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-2 px-3 py-1.5 text-[0.8em] text-left transition-colors ${c(
        "text-text-muted hover:text-text-bright hover:bg-ink-hover",
        "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
      )}`}
    >
      {children}
    </button>
  );
}
