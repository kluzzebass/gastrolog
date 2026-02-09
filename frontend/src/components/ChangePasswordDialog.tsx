import { useState, useEffect, useRef } from "react";
import FocusTrap from "focus-trap-react";
import { useThemeClass } from "../hooks/useThemeClass";
import { SpinnerIcon } from "./icons";
import { ConnectError } from "@connectrpc/connect";
import { useChangePassword } from "../api/hooks";

export function ChangePasswordDialog({
  username,
  dark,
  onClose,
  onSuccess,
}: {
  username: string;
  dark: boolean;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [oldPassword, setOldPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const oldRef = useRef<HTMLInputElement>(null);
  const changePassword = useChangePassword();

  useEffect(() => {
    oldRef.current?.focus();
  }, []);

  // Close on Escape.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  const c = useThemeClass(dark);
  const isPending = changePassword.isPending;
  const mismatch =
    confirmPassword.length > 0 && newPassword !== confirmPassword;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (!oldPassword || !newPassword) {
      setError("All fields are required.");
      return;
    }
    if (newPassword !== confirmPassword) {
      setError("New passwords do not match.");
      return;
    }

    try {
      await changePassword.mutateAsync({
        username,
        oldPassword,
        newPassword,
      });
      onSuccess();
    } catch (err) {
      if (err instanceof ConnectError) {
        setError(err.rawMessage);
      } else {
        setError("An unexpected error occurred.");
      }
    }
  };

  return (
    <FocusTrap focusTrapOptions={{ escapeDeactivates: false, allowOutsideClick: true }}>
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      onClick={onClose}
    >
      <div className="absolute inset-0 bg-black/40" />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Change Password"
        className={`relative w-full max-w-sm rounded-lg border p-6 shadow-2xl ${c(
          "bg-ink-surface border-ink-border",
          "bg-light-surface border-light-border",
        )}`}
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          aria-label="Close"
          className={`absolute top-3 right-3 w-7 h-7 flex items-center justify-center rounded text-lg leading-none transition-colors ${c(
            "text-text-muted hover:text-text-bright",
            "text-light-text-muted hover:text-light-text-bright",
          )}`}
        >
          &times;
        </button>

        <h2
          className={`font-display text-[1.3em] font-semibold tracking-tight mb-4 ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Change Password
        </h2>

        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          {error && (
            <div
              className={`text-[0.82em] leading-relaxed px-3 py-2 rounded border ${c(
                "bg-severity-error/10 border-severity-error/30 text-severity-error",
                "bg-severity-error/8 border-severity-error/20 text-severity-error",
              )}`}
            >
              {error}
            </div>
          )}

          <div className="flex flex-col gap-1.5">
            <label
              className={`text-[0.78em] font-medium tracking-wide uppercase ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Current Password
            </label>
            <input
              ref={oldRef}
              type="password"
              value={oldPassword}
              onChange={(e) => setOldPassword(e.target.value)}
              autoComplete="current-password"
              disabled={isPending}
              className={`px-3 py-2 text-[0.9em] border rounded focus:outline-none transition-colors ${c(
                "bg-ink border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                "bg-light-bg border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
              )} ${isPending ? "opacity-50 cursor-not-allowed" : ""}`}
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <label
              className={`text-[0.78em] font-medium tracking-wide uppercase ${c("text-text-muted", "text-light-text-muted")}`}
            >
              New Password
            </label>
            <input
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              autoComplete="new-password"
              disabled={isPending}
              className={`px-3 py-2 text-[0.9em] border rounded focus:outline-none transition-colors ${
                mismatch
                  ? "border-severity-error"
                  : c(
                      "border-ink-border focus:border-copper-dim",
                      "border-light-border focus:border-copper",
                    )
              } ${c(
                "bg-ink text-text-bright placeholder:text-text-ghost",
                "bg-light-bg text-light-text-bright placeholder:text-light-text-ghost",
              )} ${isPending ? "opacity-50 cursor-not-allowed" : ""}`}
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <label
              className={`text-[0.78em] font-medium tracking-wide uppercase ${
                mismatch
                  ? "text-severity-error"
                  : c("text-text-muted", "text-light-text-muted")
              }`}
            >
              Confirm New Password
            </label>
            <input
              type="password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              autoComplete="new-password"
              disabled={isPending}
              className={`px-3 py-2 text-[0.9em] border rounded focus:outline-none transition-colors ${
                mismatch
                  ? "border-severity-error"
                  : c(
                      "border-ink-border focus:border-copper-dim",
                      "border-light-border focus:border-copper",
                    )
              } ${c(
                "bg-ink text-text-bright placeholder:text-text-ghost",
                "bg-light-bg text-light-text-bright placeholder:text-light-text-ghost",
              )} ${isPending ? "opacity-50 cursor-not-allowed" : ""}`}
            />
            <span
              className={`text-[0.78em] ${mismatch ? "text-severity-error" : "invisible"}`}
            >
              Passwords do not match
            </span>
          </div>

          <div className="flex gap-3 mt-1">
            <button
              type="button"
              onClick={onClose}
              disabled={isPending}
              className={`flex-1 px-4 py-2.5 text-[0.85em] font-medium rounded border transition-all duration-200 ${c(
                "border-ink-border text-text-muted hover:text-text-bright hover:border-ink-border-subtle",
                "border-light-border text-light-text-muted hover:text-light-text-bright hover:border-light-border-subtle",
              )} ${isPending ? "opacity-50 cursor-not-allowed" : ""}`}
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isPending}
              className={`flex-1 px-4 py-2.5 text-[0.85em] font-medium rounded transition-all duration-200 flex items-center justify-center gap-2 ${
                isPending
                  ? "opacity-60 cursor-not-allowed"
                  : "hover:brightness-110 active:scale-[0.98]"
              } ${c("bg-copper text-ink", "bg-copper text-white")}`}
            >
              {isPending && <SpinnerIcon className="animate-spin h-4 w-4" />}
              Change Password
            </button>
          </div>
        </form>
      </div>
    </div>
    </FocusTrap>
  );
}
