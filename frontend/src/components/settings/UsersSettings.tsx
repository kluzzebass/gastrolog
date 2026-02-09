import { useState } from "react";
import {
  useListUsers,
  useCreateUser,
  useResetPassword,
  useUpdateUserRole,
  useDeleteUser,
  useCurrentUser,
} from "../../api/hooks/useAuth";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { SettingsCard } from "./SettingsCard";
import { FormField, TextInput, SelectInput } from "./FormField";

const roleOptions = [
  { value: "admin", label: "Admin" },
  { value: "user", label: "User" },
];

export function UsersSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data: users, isLoading } = useListUsers();
  const createUser = useCreateUser();
  const resetPassword = useResetPassword();
  const updateUserRole = useUpdateUserRole();
  const deleteUser = useDeleteUser();
  const currentUser = useCurrentUser();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState("user");

  // Per-user edit state for role changes and password resets.
  const [roleEdits, setRoleEdits] = useState<Record<string, string>>({});
  const [resetOpen, setResetOpen] = useState<string | null>(null);
  const [resetPw, setResetPw] = useState("");
  const [showNewPw, setShowNewPw] = useState(false);
  const [showResetPw, setShowResetPw] = useState(false);

  const handleCreate = async () => {
    if (!newUsername.trim()) {
      addToast("Username is required", "warn");
      return;
    }
    if (!newPassword) {
      addToast("Password is required", "warn");
      return;
    }
    try {
      await createUser.mutateAsync({
        username: newUsername.trim(),
        password: newPassword,
        role: newRole,
      });
      addToast(`User "${newUsername.trim()}" created`, "info");
      setAdding(false);
      setNewUsername("");
      setNewPassword("");
      setNewRole("user");
      setShowNewPw(false);
    } catch (err: any) {
      addToast(err.message ?? "Failed to create user", "error");
    }
  };

  const handleRoleSave = async (username: string) => {
    const role = roleEdits[username];
    if (!role) return;
    try {
      await updateUserRole.mutateAsync({ username, role });
      setRoleEdits((prev) => {
        const next = { ...prev };
        delete next[username];
        return next;
      });
      addToast(`Role updated for "${username}"`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update role", "error");
    }
  };

  const handleResetPassword = async (username: string) => {
    if (!resetPw) {
      addToast("New password is required", "warn");
      return;
    }
    try {
      await resetPassword.mutateAsync({ username, newPassword: resetPw });
      addToast(`Password reset for "${username}"`, "info");
      setResetOpen(null);
      setResetPw("");
      setShowResetPw(false);
    } catch (err: any) {
      addToast(err.message ?? "Failed to reset password", "error");
    }
  };

  const handleDelete = async (username: string) => {
    try {
      await deleteUser.mutateAsync(username);
      addToast(`User "${username}" deleted`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete user", "error");
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Users
        </h2>
        <button
          onClick={() => setAdding(!adding)}
          className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors"
        >
          {adding ? "Cancel" : "Add User"}
        </button>
      </div>

      <div className="flex flex-col gap-3">
        {adding && (
          <div
            className={`border rounded-lg p-4 ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
          >
            <div className="flex flex-col gap-3">
              <div className="grid grid-cols-2 gap-3">
                <FormField label="Username" dark={dark}>
                  <TextInput
                    value={newUsername}
                    onChange={setNewUsername}
                    placeholder="username"
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField label="Role" dark={dark}>
                  <SelectInput
                    value={newRole}
                    onChange={setNewRole}
                    options={roleOptions}
                    dark={dark}
                  />
                </FormField>
              </div>
              <FormField label="Password" dark={dark}>
                <PasswordInput
                  value={newPassword}
                  onChange={setNewPassword}
                  show={showNewPw}
                  onToggle={() => setShowNewPw(!showNewPw)}
                  placeholder="password"
                  dark={dark}
                />
              </FormField>
              <div className="flex justify-end gap-2 pt-2">
                <button
                  onClick={() => setAdding(false)}
                  className={`px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                    "border-ink-border text-text-muted hover:bg-ink-hover",
                    "border-light-border text-light-text-muted hover:bg-light-hover",
                  )}`}
                >
                  Cancel
                </button>
                <button
                  onClick={handleCreate}
                  disabled={createUser.isPending}
                  className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                >
                  {createUser.isPending ? "Creating..." : "Create"}
                </button>
              </div>
            </div>
          </div>
        )}

        {isLoading && (
          <div
            className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Loading...
          </div>
        )}

        {users?.map((user) => {
          const isSelf = currentUser?.username === user.username;
          const editedRole = roleEdits[user.username];
          const roleDirty =
            editedRole !== undefined && editedRole !== user.role;

          return (
            <SettingsCard
              key={user.username}
              id={user.username}
              typeBadge={user.role}
              dark={dark}
              expanded={expanded === user.username}
              onToggle={() =>
                setExpanded(expanded === user.username ? null : user.username)
              }
              onDelete={isSelf ? undefined : () => handleDelete(user.username)}
            >
              <div className="flex flex-col gap-4">
                <div className="flex items-end gap-3">
                  <div className="flex-1">
                    <FormField label="Role" dark={dark}>
                      <SelectInput
                        value={editedRole ?? user.role}
                        onChange={(v) =>
                          setRoleEdits((prev) => ({
                            ...prev,
                            [user.username]: v,
                          }))
                        }
                        options={roleOptions}
                        dark={dark}
                        disabled={isSelf}
                      />
                    </FormField>
                  </div>
                  {roleDirty && (
                    <button
                      onClick={() => handleRoleSave(user.username)}
                      disabled={updateUserRole.isPending}
                      className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                    >
                      {updateUserRole.isPending ? "Saving..." : "Save"}
                    </button>
                  )}
                </div>

                {/* Reset password */}
                {resetOpen === user.username ? (
                  <div className="flex items-end gap-3">
                    <div className="flex-1">
                      <FormField label="New Password" dark={dark}>
                        <PasswordInput
                          value={resetPw}
                          onChange={setResetPw}
                          show={showResetPw}
                          onToggle={() => setShowResetPw(!showResetPw)}
                          placeholder="new password"
                          dark={dark}
                        />
                      </FormField>
                    </div>
                    <button
                      onClick={() => handleResetPassword(user.username)}
                      disabled={resetPassword.isPending}
                      className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
                    >
                      {resetPassword.isPending ? "Resetting..." : "Reset"}
                    </button>
                    <button
                      onClick={() => {
                        setResetOpen(null);
                        setResetPw("");
                        setShowResetPw(false);
                      }}
                      className={`px-3 py-1.5 text-[0.8em] rounded transition-colors ${c(
                        "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                        "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                      )}`}
                    >
                      Cancel
                    </button>
                  </div>
                ) : (
                  <button
                    onClick={() => {
                      setResetOpen(user.username);
                      setResetPw("");
                    }}
                    className={`self-start px-3 py-1.5 text-[0.8em] rounded border transition-colors ${c(
                      "border-ink-border text-text-muted hover:text-text-bright hover:bg-ink-hover",
                      "border-light-border text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                    )}`}
                  >
                    Reset Password
                  </button>
                )}

                <div
                  className={`text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Created{" "}
                  {new Date(Number(user.createdAt) * 1000).toLocaleDateString()}
                  {isSelf && " (you)"}
                </div>
              </div>
            </SettingsCard>
          );
        })}

        {!isLoading && users?.length === 0 && !adding && (
          <div
            className={`text-center py-8 text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            No users configured.
          </div>
        )}
      </div>
    </div>
  );
}

function PasswordInput({
  value,
  onChange,
  show,
  onToggle,
  placeholder,
  dark,
}: {
  value: string;
  onChange: (v: string) => void;
  show: boolean;
  onToggle: () => void;
  placeholder?: string;
  dark: boolean;
}) {
  const c = useThemeClass(dark);
  return (
    <div className="relative">
      <input
        type={show ? "text" : "password"}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className={`w-full px-2.5 py-1.5 pr-9 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
          "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
          "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
        )}`}
      />
      <button
        type="button"
        onClick={onToggle}
        className={`absolute right-2 top-1/2 -translate-y-1/2 transition-colors ${c(
          "text-text-ghost hover:text-text-muted",
          "text-light-text-ghost hover:text-light-text-muted",
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
          {show ? (
            <>
              <path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94" />
              <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19" />
              <path d="M14.12 14.12a3 3 0 1 1-4.24-4.24" />
              <line x1="1" y1="1" x2="23" y2="23" />
            </>
          ) : (
            <>
              <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
              <circle cx="12" cy="12" r="3" />
            </>
          )}
        </svg>
      </button>
    </div>
  );
}
