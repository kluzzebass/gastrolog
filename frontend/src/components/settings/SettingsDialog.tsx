import { useEffect, useState } from "react";
import { StoresSettings } from "./StoresSettings";
import { IngestersSettings } from "./IngestersSettings";
import { FiltersSettings } from "./FiltersSettings";
import { PoliciesSettings } from "./PoliciesSettings";
import { RetentionPoliciesSettings } from "./RetentionPoliciesSettings";
import { useServerConfig, usePutServerConfig } from "../../api/hooks/useConfig";
import { useToast } from "../Toast";
import { FormField, TextInput } from "./FormField";

export type SettingsTab =
  | "service"
  | "stores"
  | "ingesters"
  | "filters"
  | "policies"
  | "retention";

interface SettingsDialogProps {
  dark: boolean;
  tab: SettingsTab;
  onTabChange: (tab: SettingsTab) => void;
  onClose: () => void;
}

const tabs: {
  id: SettingsTab;
  label: string;
  icon: (p: { className?: string }) => React.ReactNode;
}[] = [
  { id: "service", label: "Service", icon: ServiceIcon },
  { id: "ingesters", label: "Ingesters", icon: IngesterIcon },
  { id: "filters", label: "Filters", icon: FilterIcon },
  { id: "policies", label: "Rotation Policies", icon: PolicyIcon },
  { id: "retention", label: "Retention Policies", icon: RetentionIcon },
  { id: "stores", label: "Stores", icon: StorageIcon },
];

export function SettingsDialog({
  dark,
  tab,
  onTabChange,
  onClose,
}: SettingsDialogProps) {
  const c = (d: string, l: string) => (dark ? d : l);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    };
    window.addEventListener("keydown", handler, true);
    return () => window.removeEventListener("keydown", handler, true);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      onClick={onClose}
    >
      <div className="absolute inset-0 bg-black/40" />
      <div
        className={`relative w-[90vw] max-w-5xl h-[85vh] flex flex-col rounded-lg shadow-2xl overflow-hidden ${c("bg-ink-raised border border-ink-border-subtle", "bg-light-raised border border-light-border-subtle")}`}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          className={`flex items-center gap-4 px-5 py-3 border-b shrink-0 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`font-display text-[1.2em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Settings
          </h2>

          {/* Tabs */}
          <div className="flex gap-1 ml-4">
            {tabs.map(({ id, label, icon: Icon }) => (
              <button
                key={id}
                onClick={() => onTabChange(id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded text-[0.8em] transition-colors ${
                  tab === id
                    ? "bg-copper/15 text-copper font-medium"
                    : c(
                        "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                        "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                      )
                }`}
              >
                <Icon className="w-3.5 h-3.5" />
                {label}
              </button>
            ))}
          </div>

          <div className="flex-1" />

          <button
            onClick={onClose}
            className={`w-7 h-7 flex items-center justify-center rounded text-lg leading-none transition-colors ${c("text-text-muted hover:text-text-bright", "text-light-text-muted hover:text-light-text-bright")}`}
          >
            &times;
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-5">
          {tab === "service" && <ServiceSettings dark={dark} />}
          {tab === "ingesters" && <IngestersSettings dark={dark} />}
          {tab === "filters" && <FiltersSettings dark={dark} />}
          {tab === "policies" && <PoliciesSettings dark={dark} />}
          {tab === "retention" && <RetentionPoliciesSettings dark={dark} />}
          {tab === "stores" && <StoresSettings dark={dark} />}
        </div>
      </div>
    </div>
  );
}

function ServiceSettings({ dark }: { dark: boolean }) {
  const c = (d: string, l: string) => (dark ? d : l);
  const { data, isLoading } = useServerConfig();
  const putConfig = usePutServerConfig();
  const { addToast } = useToast();

  const [tokenDuration, setTokenDuration] = useState("");
  const [jwtSecret, setJwtSecret] = useState("");
  const [initialized, setInitialized] = useState(false);

  useEffect(() => {
    if (data && !initialized) {
      setTokenDuration(data.tokenDuration);
      setJwtSecret(data.jwtSecret);
      setInitialized(true);
    }
  }, [data, initialized]);

  const dirty =
    initialized &&
    data &&
    (tokenDuration !== data.tokenDuration || jwtSecret !== data.jwtSecret);

  const handleSave = async () => {
    try {
      await putConfig.mutateAsync({ tokenDuration, jwtSecret });
      addToast("Server configuration updated", "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update server configuration", "error");
    }
  };

  const handleReset = () => {
    if (data) {
      setTokenDuration(data.tokenDuration);
      setJwtSecret(data.jwtSecret);
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Service
        </h2>
      </div>

      {isLoading ? (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Loading...
        </div>
      ) : (
        <div className="flex flex-col gap-5 max-w-lg">
          <FormField
            label="Token Duration"
            description="How long authentication tokens remain valid. Use Go duration syntax, e.g. 168h, 720h, 24h."
            dark={dark}
          >
            <TextInput
              value={tokenDuration}
              onChange={setTokenDuration}
              placeholder="168h"
              dark={dark}
              mono
            />
          </FormField>

          <FormField
            label="JWT Secret"
            description="The signing key used for authentication tokens. Changing this will invalidate all existing sessions."
            dark={dark}
          >
            <TextInput
              value={jwtSecret}
              onChange={setJwtSecret}
              placeholder=""
              dark={dark}
              mono
            />
          </FormField>

          <div className="flex gap-2 mt-1">
            <button
              onClick={handleSave}
              disabled={!dirty || putConfig.isPending}
              className="px-3 py-1.5 text-[0.8em] rounded bg-copper text-white hover:bg-copper-glow transition-colors disabled:opacity-50"
            >
              {putConfig.isPending ? "Saving..." : "Save"}
            </button>
            {dirty && (
              <button
                onClick={handleReset}
                className={`px-3 py-1.5 text-[0.8em] rounded transition-colors ${c(
                  "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                  "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                )}`}
              >
                Reset
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function ServiceIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </svg>
  );
}

function FilterIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
    </svg>
  );
}

function StorageIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </svg>
  );
}

function IngesterIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <path d="M12 3v12" />
      <path d="M8 11l4 4 4-4" />
      <path d="M3 17h18" />
      <path d="M3 21h18" />
    </svg>
  );
}

function RetentionIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <circle cx="12" cy="12" r="9" />
      <path d="M12 6v6l4 2" />
      <path d="M4 20l2-2" />
      <path d="M20 20l-2-2" />
    </svg>
  );
}

function PolicyIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
    >
      <path d="M21 12a9 9 0 1 1-9-9" />
      <path d="M21 3v6h-6" />
      <path d="M21 3l-9 9" />
    </svg>
  );
}
