import { useState, useRef } from "react";
import { useClickOutside } from "../hooks/useClickOutside";
import { useThemeClass } from "../hooks/useThemeClass";
import { UserIcon, LockIcon, SlidersIcon, SignOutIcon } from "./icons";

export function UserMenu({
  username,
  role,
  dark,
  onPreferences,
  onChangePassword,
  onLogout,
}: Readonly<{
  username: string;
  role: string;
  dark: boolean;
  onPreferences: () => void;
  onChangePassword: () => void;
  onLogout: () => void;
}>) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(
    ref,
    () => setOpen(false),
  );

  const c = useThemeClass(dark);

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((o) => !o)}
        aria-label={`User menu: ${username}`}
        title={username}
        className={`w-9 h-9 flex items-center justify-center rounded transition-all duration-200 ${c(
          "text-text-ghost hover:text-text-muted hover:bg-ink-hover",
          "text-light-text-ghost hover:text-light-text-muted hover:bg-light-hover",
        )} ${open ? c("text-text-muted bg-ink-hover", "text-light-text-muted bg-light-hover") : ""}`}
      >
        <UserIcon className="w-4 h-4" />
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
              <LockIcon className="w-4 h-4" />
              Change password
            </MenuItem>
            <MenuItem
              dark={dark}
              onClick={() => {
                setOpen(false);
                onPreferences();
              }}
            >
              <SlidersIcon className="w-4 h-4" />
              Preferences
            </MenuItem>
            <MenuItem
              dark={dark}
              onClick={() => {
                setOpen(false);
                onLogout();
              }}
            >
              <SignOutIcon className="w-4 h-4" />
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
}: Readonly<{
  dark: boolean;
  onClick: () => void;
  children: React.ReactNode;
}>) {
  const c = useThemeClass(dark);
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-2 px-3 py-2.5 text-[0.8em] text-left transition-colors ${c(
        "text-text-muted hover:text-text-bright hover:bg-ink-hover",
        "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
      )}`}
    >
      {children}
    </button>
  );
}
