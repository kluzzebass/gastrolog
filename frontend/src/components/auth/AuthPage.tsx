import { useState, useEffect, useRef } from "react";
import { useNavigate } from "@tanstack/react-router";
import { ConnectError } from "@connectrpc/connect";
import { useAuthStatus, useLogin, useRegister } from "../../api/hooks";

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

  const [dark, setDark] = useState(
    () => window.matchMedia("(prefers-color-scheme: dark)").matches,
  );

  useEffect(() => {
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

  const c = (d: string, l: string) => (dark ? d : l);
  const isRegister = mode === "register";
  const isPending = login.isPending || register.isPending;

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
            alt=""
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

          {/* Username */}
          <div className="flex flex-col gap-1.5">
            <label
              className={`text-[0.78em] font-medium tracking-wide uppercase ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Username
            </label>
            <input
              ref={usernameRef}
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
              disabled={isPending}
              className={`px-3 py-2 text-[0.9em] border rounded focus:outline-none transition-colors ${c(
                "bg-ink border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                "bg-light-bg border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
              )} ${isPending ? "opacity-50 cursor-not-allowed" : ""}`}
            />
          </div>

          {/* Password */}
          <div className="flex flex-col gap-1.5">
            <label
              className={`text-[0.78em] font-medium tracking-wide uppercase ${c("text-text-muted", "text-light-text-muted")}`}
            >
              Password
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete={isRegister ? "new-password" : "current-password"}
              disabled={isPending}
              className={`px-3 py-2 text-[0.9em] border rounded focus:outline-none transition-colors ${c(
                "bg-ink border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                "bg-light-bg border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
              )} ${isPending ? "opacity-50 cursor-not-allowed" : ""}`}
            />
          </div>

          {/* Confirm Password (register only) */}
          {isRegister && (
            <div className="flex flex-col gap-1.5">
              <label
                className={`text-[0.78em] font-medium tracking-wide uppercase ${c("text-text-muted", "text-light-text-muted")}`}
              >
                Confirm Password
              </label>
              <input
                type="password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                autoComplete="new-password"
                disabled={isPending}
                className={`px-3 py-2 text-[0.9em] border rounded focus:outline-none transition-colors ${c(
                  "bg-ink border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                  "bg-light-bg border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
                )} ${isPending ? "opacity-50 cursor-not-allowed" : ""}`}
              />
            </div>
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
            {isPending && (
              <svg
                className="animate-spin h-4 w-4"
                viewBox="0 0 24 24"
                fill="none"
              >
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="3"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                />
              </svg>
            )}
            {isRegister ? "Create Account" : "Sign In"}
          </button>
        </form>

        {/* Mode toggle — only show when both modes are valid */}
        {authStatus.data && !authStatus.data.needsSetup && (
          <p
            className={`text-center text-[0.82em] mt-5 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            {isRegister ? "Already have an account?" : "Don't have an account?"}{" "}
            <button
              type="button"
              onClick={() =>
                navigate({ to: isRegister ? "/login" : "/register" })
              }
              className="text-copper hover:text-copper-glow transition-colors"
            >
              {isRegister ? "Sign in" : "Register"}
            </button>
          </p>
        )}
      </div>
    </div>
  );
}
