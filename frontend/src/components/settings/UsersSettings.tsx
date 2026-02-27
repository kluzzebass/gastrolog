import { useState, useReducer } from "react";
import {
  useListUsers,
  useCreateUser,
  useResetPassword,
  useUpdateUserRole,
  useRenameUser,
  useDeleteUser,
  useCurrentUser,
} from "../../api/hooks/useAuth";
import { useSettings } from "../../api/hooks/useConfig";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useToast } from "../Toast";
import { useEditState } from "../../hooks/useEditState";
import { PasswordRules } from "../auth/PasswordRules";
import { SettingsCard } from "./SettingsCard";
import { SettingsSection } from "./SettingsSection";
import { AddFormCard } from "./AddFormCard";
import { FormField, TextInput, SelectInput } from "./FormField";
import { PrimaryButton } from "./Buttons";
import { EyeIcon, EyeOffIcon } from "../icons";

const roleOptions = [
  { value: "admin", label: "Admin" },
  { value: "user", label: "User" },
];

// -- Reducer for "Add user" form state --

interface AddUserFormState {
  adding: boolean;
  newUsername: string;
  newPassword: string;
  newRole: string;
  showNewPw: boolean;
}

const addUserFormInitial: AddUserFormState = {
  adding: false,
  newUsername: "",
  newPassword: "",
  newRole: "user",
  showNewPw: false,
};

type AddUserFormAction =
  | { type: "setAdding"; value: boolean }
  | { type: "setNewUsername"; value: string }
  | { type: "setNewPassword"; value: string }
  | { type: "setNewRole"; value: string }
  | { type: "toggleShowNewPw" }
  | { type: "resetForm" };

function addUserFormReducer(state: AddUserFormState, action: AddUserFormAction): AddUserFormState {
  switch (action.type) {
    case "setAdding":
      return { ...state, adding: action.value };
    case "setNewUsername":
      return { ...state, newUsername: action.value };
    case "setNewPassword":
      return { ...state, newPassword: action.value };
    case "setNewRole":
      return { ...state, newRole: action.value };
    case "toggleShowNewPw":
      return { ...state, showNewPw: !state.showNewPw };
    case "resetForm":
      return addUserFormInitial;
    default:
      return state;
  }
}

interface UserEdit {
  username: string;
  role: string;
  newPassword: string;
  showPassword: boolean;
}

export function UsersSettings({ dark, noAuth }: Readonly<{ dark: boolean; noAuth?: boolean }>) {
  const c = useThemeClass(dark);
  const { data: users, isLoading } = useListUsers();
  const createUser = useCreateUser();
  const resetPassword = useResetPassword();
  const updateUserRole = useUpdateUserRole();
  const renameUser = useRenameUser();
  const deleteUser = useDeleteUser();
  const currentUser = useCurrentUser();
  const { data: settings } = useSettings();
  const { addToast } = useToast();

  const [expanded, setExpanded] = useState<string | null>(null);

  const [addForm, dispatchAdd] = useReducer(addUserFormReducer, addUserFormInitial);
  const { adding, newUsername, newPassword, newRole, showNewPw } = addForm;

  const defaults = (id: string): UserEdit => {
    const user = users?.find((u) => u.id === id);
    if (!user) return { username: "", role: "user", newPassword: "", showPassword: false };
    return { username: user.username, role: user.role, newPassword: "", showPassword: false };
  };

  const { getEdit, setEdit, clearEdit, isDirty } = useEditState(defaults);

  const isSaving = renameUser.isPending || updateUserRole.isPending || resetPassword.isPending;

  const handleSave = async (id: string) => {
    const user = users?.find((u) => u.id === id);
    if (!user) return;
    const edit = getEdit(id);

    try {
      if (edit.username !== user.username) {
        await renameUser.mutateAsync({ id, newUsername: edit.username.trim() });
      }
      if (edit.role !== user.role) {
        await updateUserRole.mutateAsync({ id, role: edit.role });
      }
      if (edit.newPassword) {
        await resetPassword.mutateAsync({ id, newPassword: edit.newPassword });
      }
      clearEdit(id);
      addToast(`User "${edit.username || user.username}" updated`, "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update user", "error");
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
      dispatchAdd({ type: "resetForm" });
    } catch (err: any) {
      addToast(err.message ?? "Failed to create user", "error");
    }
  };

  return (
    <SettingsSection
      title="Users"
      titleSuffix={noAuth ? "— disabled by --no-auth" : undefined}
      helpTopicId="user-management"
      addLabel={noAuth ? undefined : "Add User"}
      adding={adding}
      onToggleAdd={() => dispatchAdd({ type: "setAdding", value: !adding })}
      isLoading={false}
      isEmpty={!isLoading && (users?.length ?? 0) === 0}
      emptyMessage={noAuth ? "Authentication is disabled. Start the server without --no-auth to manage users." : "No users configured."}
      dark={dark}
      disabled={noAuth}
    >
      {adding && (
        <AddFormCard
          dark={dark}
          onCancel={() => dispatchAdd({ type: "resetForm" })}
          onCreate={handleCreate}
          isPending={createUser.isPending}
        >
          <div className="grid grid-cols-2 gap-3">
            <FormField label="Username" dark={dark}>
              <TextInput
                value={newUsername}
                onChange={(v) => dispatchAdd({ type: "setNewUsername", value: v })}
                placeholder="username"
                dark={dark}
                mono
              />
            </FormField>
            <FormField label="Role" dark={dark}>
              <SelectInput
                value={newRole}
                onChange={(v) => dispatchAdd({ type: "setNewRole", value: v })}
                options={roleOptions}
                dark={dark}
              />
            </FormField>
          </div>
          <FormField label="Password" dark={dark}>
            <PasswordInput
              value={newPassword}
              onChange={(v) => dispatchAdd({ type: "setNewPassword", value: v })}
              show={showNewPw}
              onToggle={() => dispatchAdd({ type: "toggleShowNewPw" })}
              placeholder="password"
              dark={dark}
            />
          </FormField>
          {settings && (
            <PasswordRules password={newPassword} config={settings} dark={dark} />
          )}
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
        const edit = getEdit(user.id);
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
            footer={
              <PrimaryButton
                onClick={() => handleSave(user.id)}
                disabled={isSaving || !isDirty(user.id)}
              >
                {isSaving ? "Saving..." : "Save"}
              </PrimaryButton>
            }
          >
            <div className="flex flex-col gap-3">
              <div className="grid grid-cols-2 gap-3">
                <FormField label="Username" dark={dark}>
                  <TextInput
                    value={edit.username}
                    onChange={(v) => setEdit(user.id, { username: v })}
                    dark={dark}
                    mono
                  />
                </FormField>
                <FormField label="Role" dark={dark}>
                  <SelectInput
                    value={edit.role}
                    onChange={(v) => setEdit(user.id, { role: v })}
                    options={roleOptions}
                    dark={dark}
                    disabled={isSelf}
                  />
                </FormField>
              </div>
              <FormField label="Reset Password" dark={dark}>
                <PasswordInput
                  value={edit.newPassword}
                  onChange={(v) => setEdit(user.id, { newPassword: v })}
                  show={edit.showPassword}
                  onToggle={() => setEdit(user.id, { showPassword: !edit.showPassword })}
                  placeholder="leave blank to keep current"
                  dark={dark}
                />
              </FormField>
              {edit.newPassword && settings && (
                <PasswordRules password={edit.newPassword} config={settings} dark={dark} />
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
}: Readonly<{
  value: string;
  onChange: (v: string) => void;
  show: boolean;
  onToggle: () => void;
  placeholder?: string;
  dark: boolean;
}>) {
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
