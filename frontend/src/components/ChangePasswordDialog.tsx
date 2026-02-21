import { useState, useEffect, useRef } from "react";
import { useThemeClass } from "../hooks/useThemeClass";
import { Dialog } from "./Dialog";
import { SpinnerIcon } from "./icons";
import { ConnectError } from "@connectrpc/connect";
import { useChangePassword, useServerConfig } from "../api/hooks";
import { AuthFormField } from "./auth/AuthFormField";
import { PasswordRules } from "./auth/PasswordRules";

export function ChangePasswordDialog({
  username,
  dark,
  onClose,
  onSuccess,
}: Readonly<{
  username: string;
  dark: boolean;
  onClose: () => void;
  onSuccess: () => void;
}>) {
  const [oldPassword, setOldPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const oldRef = useRef<HTMLInputElement>(null);
  const changePassword = useChangePassword();
  const { data: serverConfig } = useServerConfig();

  useEffect(() => {
    oldRef.current?.focus();
  }, []);

  const c = useThemeClass(dark);
  const isPending = changePassword.isPending;
  const mismatch =
    confirmPassword.length > 0 && newPassword !== confirmPassword;

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
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
    <Dialog onClose={onClose} ariaLabel="Change Password" dark={dark} size="sm">

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

        <AuthFormField
          ref={oldRef}
          label="Current Password"
          type="password"
          value={oldPassword}
          onChange={setOldPassword}
          autoComplete="current-password"
          disabled={isPending}
          dark={dark}
        />

        <AuthFormField
          label="New Password"
          type="password"
          value={newPassword}
          onChange={setNewPassword}
          autoComplete="new-password"
          disabled={isPending}
          dark={dark}
        />

        <AuthFormField
          label="Confirm New Password"
          type="password"
          value={confirmPassword}
          onChange={setConfirmPassword}
          autoComplete="new-password"
          error={mismatch}
          disabled={isPending}
          dark={dark}
        />

        {mismatch && (
          <span className="text-[0.78em] -mt-3 text-severity-error">
            Passwords do not match
          </span>
        )}

        {serverConfig && (
          <PasswordRules password={newPassword} config={serverConfig} dark={dark} />
        )}

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
    </Dialog>
  );
}

