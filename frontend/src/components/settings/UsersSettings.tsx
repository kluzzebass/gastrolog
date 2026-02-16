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
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { PrimaryButton, GhostButton } from "./Buttons";
import { EyeIcon, EyeOffIcon } from "../icons";

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

  const handleRoleSave = async (user: { id: string; username: string }) => {
    const role = roleEdits[user.id];
    if (!role) return;
    try {
      await updateUserRole.mutateAsync({ id: user.id, role });
      setRoleEdits((prev) => {
        const next = { ...prev };
        delete next[user.id];
        return next;
      });
      addToast(`Role updated for "${user.username}"`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update role", "error");
    }
  };

  const handleResetPassword = async (user: { id: string; username: string }) => {
    if (!resetPw) {
      addToast("New password is required", "warn");
      return;
    }
    try {
      await resetPassword.mutateAsync({ id: user.id, newPassword: resetPw });
      addToast(`Password reset for "${user.username}"`, "info");
      setResetOpen(null);
      setResetPw("");
      setShowResetPw(false);
    } catch (err: any) {
      addToast(err.message ?? "Failed to reset password", "error");
    }
  };

  const handleDeleteUser = async (user: { id: string; username: string }) => {
    try {
      await deleteUser.mutateAsync(user.id);
      addToast(`User "${user.username}" deleted`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to delete user", "error");
    }
  };

  return (
    <SettingsSection
      title="Users"
      helpTopicId="user-management"
      addLabel="Add User"
      adding={adding}
      onToggleAdd={() => setAdding(!adding)}
      isLoading={false}
      isEmpty={!isLoading && (users?.length ?? 0) === 0}
      emptyMessage="No users configured."
      dark={dark}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => setAdding(false)}
          onCreate={handleCreate}
          isPending={createUser.isPending}
        >
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
        </AddFormCard>
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
        const editedRole = roleEdits[user.id];
        const roleDirty =
          editedRole !== undefined && editedRole !== user.role;

        return (
          <SettingsCard
            key={user.id}
            id={user.username}
            typeBadge={user.role}
            dark={dark}
            expanded={expanded === user.id}
            onToggle={() =>
              setExpanded(expanded === user.id ? null : user.id)
            }
            onDelete={isSelf ? undefined : () => handleDeleteUser(user)}
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
                          [user.id]: v,
                        }))
                      }
                      options={roleOptions}
                      dark={dark}
                      disabled={isSelf}
                    />
                  </FormField>
                </div>
                {roleDirty && (
                  <PrimaryButton
                    onClick={() => handleRoleSave(user)}
                    disabled={updateUserRole.isPending}
                  >
                    {updateUserRole.isPending ? "Saving..." : "Save"}
                  </PrimaryButton>
                )}
              </div>

              {/* Reset password */}
              {resetOpen === user.id ? (
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
                  <PrimaryButton
                    onClick={() => handleResetPassword(user)}
                    disabled={resetPassword.isPending}
                  >
                    {resetPassword.isPending ? "Resetting..." : "Reset"}
                  </PrimaryButton>
                  <GhostButton
                    onClick={() => {
                      setResetOpen(null);
                      setResetPw("");
                      setShowResetPw(false);
                    }}
                    dark={dark}
                  >
                    Cancel
                  </GhostButton>
                </div>
              ) : (
                <GhostButton
                  onClick={() => {
                    setResetOpen(user.id);
                    setResetPw("");
                  }}
                  dark={dark}
                  bordered
                  className="self-start"
                >
                  Reset Password
                </GhostButton>
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
    </SettingsSection>
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
        {show ? (
          <EyeOffIcon className="w-4 h-4" />
        ) : (
          <EyeIcon className="w-4 h-4" />
        )}
      </button>
    </div>
  );
}
