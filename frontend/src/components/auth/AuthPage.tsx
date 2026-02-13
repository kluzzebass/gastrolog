import { useState, useEffect, useRef } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useThemeClass } from "../../hooks/useThemeClass";
import { SpinnerIcon } from "../icons";
import { ConnectError } from "@connectrpc/connect";
import { useAuthStatus, useLogin, useRegister } from "../../api/hooks";
import { AuthFormField } from "./AuthFormField";

interface AuthPageProps {
  mode: "login" | "register";
}

export function AuthPage({ mode }: AuthPageProps) {
  const navigate = useNavigate();
  const authStatus = useAuthStatus();
  const login = useLogin();
  const register = useRegister();

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const usernameRef = useRef<HTMLInputElement>(null);

  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem("gastrolog:theme");
    if (saved === "dark") return true;
    if (saved === "light") return false;
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  });

  useEffect(() => {
    const saved = localStorage.getItem("gastrolog:theme");
    if (saved === "dark" || saved === "light") return; // explicit choice, ignore system
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => setDark(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  // Redirect based on auth status.
  useEffect(() => {
    if (authStatus.isLoading || !authStatus.data) return;
    if (authStatus.data.needsSetup && mode === "login") {
      navigate({ to: "/register" });
    } else if (!authStatus.data.needsSetup && mode === "register") {
      navigate({ to: "/login" });
    }
  }, [authStatus.data, authStatus.isLoading, mode, navigate]);

  // Autofocus username on mount and mode change.
  useEffect(() => {
    usernameRef.current?.focus();
  }, [mode]);

  const c = useThemeClass(dark);
  const isRegister = mode === "register";
  const isPending = login.isPending || register.isPending;
  const mismatch =
    isRegister && confirmPassword.length > 0 && password !== confirmPassword;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (!username.trim() || !password) {
      setError("Username and password are required.");
      return;
    }

    if (isRegister && password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }

    try {
      if (isRegister) {
        await register.mutateAsync({ username: username.trim(), password });
      } else {
        await login.mutateAsync({ username: username.trim(), password });
      }
      navigate({ to: "/search", search: { q: "" } });
    } catch (err) {
      if (err instanceof ConnectError) {
        setError(err.rawMessage);
      } else {
        setError("An unexpected error occurred.");
      }
    }
  };

  // While loading auth status, show nothing (avoids flicker).
  if (authStatus.isLoading) {
    return <div className={`h-full ${c("bg-ink", "bg-light-bg")}`} />;
  }

  return (
    <div
      className={`grain h-full flex items-center justify-center px-4 ${c("bg-ink", "bg-light-bg")}`}
    >
      <div className="w-full max-w-sm stagger-children">
        {/* Branding */}
        <div className="flex flex-col items-center gap-3 mb-10">
          <img
            src="/favicon.svg"
            alt="GastroLog"
            className={`w-10 h-10 ${c("opacity-70", "opacity-60")}`}
          />
          <h1
            className={`font-display text-[2.2em] font-semibold tracking-tight leading-none ${c("text-text-bright", "text-light-text-bright")}`}
          >
            GastroLog
          </h1>
          <p
            className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}
          >
            {isRegister
              ? "Create the first admin account"
              : "Sign in to continue"}
          </p>
        </div>

        {/* Form card */}
        <form
          onSubmit={handleSubmit}
          className={`rounded-lg border p-6 flex flex-col gap-4 ${c(
            "bg-ink-surface border-ink-border",
            "bg-light-surface border-light-border",
          )}`}
        >
          {/* Error */}
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
            ref={usernameRef}
            label="Username"
            type="text"
            value={username}
            onChange={setUsername}
            autoComplete="username"
            disabled={isPending}
            dark={dark}
          />

          <AuthFormField
            label="Password"
            type="password"
            value={password}
            onChange={setPassword}
            autoComplete={isRegister ? "new-password" : "current-password"}
            error={mismatch}
            disabled={isPending}
            dark={dark}
          />

          {isRegister && (
            <>
              <AuthFormField
                label="Confirm Password"
                type="password"
                value={confirmPassword}
                onChange={setConfirmPassword}
                autoComplete="new-password"
                error={mismatch}
                disabled={isPending}
                dark={dark}
              />
              <span
                className={`text-[0.78em] -mt-3 ${mismatch ? "text-severity-error" : "invisible"}`}
              >
                Passwords do not match
              </span>
            </>
          )}

          {/* Submit */}
          <button
            type="submit"
            disabled={isPending}
            className={`mt-1 px-4 py-2.5 text-[0.85em] font-medium rounded transition-all duration-200 flex items-center justify-center gap-2 ${
              isPending
                ? "opacity-60 cursor-not-allowed"
                : "hover:brightness-110 active:scale-[0.98]"
            } ${c("bg-copper text-ink", "bg-copper text-white")}`}
          >
            {isPending && <SpinnerIcon className="animate-spin h-4 w-4" />}
            {isRegister ? "Create Account" : "Sign In"}
          </button>
        </form>
      </div>
    </div>
  );
}
