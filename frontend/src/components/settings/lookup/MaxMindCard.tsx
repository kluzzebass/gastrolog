import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { usePutSettings, MAXMIND_KEEP } from "../../../api/hooks/useSettings";
import { useExpandedCard } from "../../../hooks/useExpandedCards";
import { FormField, TextInput } from "../FormField";
import { Checkbox } from "../Checkbox";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { PasswordInput } from "./FormHelpers";
import type { MaxMindSettings } from "../../../api/gen/gastrolog/v1/config_pb";

export function MaxMindCard({
  dark,
  visible,
  setVisible,
  savedMaxmind,
  addToast,
}: Readonly<{
  dark: boolean;
  visible: boolean;
  setVisible: (v: boolean) => void;
  savedMaxmind?: MaxMindSettings;
  addToast: (msg: string, type: "info" | "error") => void;
}>) {
  const c = useThemeClass(dark);
  const putConfig = usePutSettings();
  const { isExpanded, toggle } = useExpandedCard();

  const [autoDownload, setAutoDownload] = useState(savedMaxmind?.autoDownload ?? false);
  const [accountId, setAccountId] = useState("");
  const [licenseKey, setLicenseKey] = useState("");
  const [justSaved, setJustSaved] = useState(false);

  const isDirty =
    !justSaved &&
    (autoDownload !== (savedMaxmind?.autoDownload ?? false) ||
      accountId !== "" ||
      licenseKey !== "");

  const save = async () => {
    try {
      await putConfig.mutateAsync({
        maxmind: {
          autoDownload,
          accountId: accountId || undefined,
          licenseKey: licenseKey || MAXMIND_KEEP,
        },
      });
      setJustSaved(true);
      requestAnimationFrame(() => setJustSaved(false));
      setAccountId("");
      setLicenseKey("");
      addToast("MaxMind configuration updated", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to update MaxMind", "error");
    }
  };

  const handleDelete = async () => {
    try {
      await putConfig.mutateAsync({
        maxmind: { autoDownload: false },
      });
      setVisible(false);
      setAutoDownload(false);
      setAccountId("");
      setLicenseKey("");
      addToast("MaxMind auto-download disabled", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to update MaxMind", "error");
    }
  };

  if (!visible) return null;

  return (
    <SettingsCard
      id="MaxMind Auto-Download"
      typeBadge="maxmind"
      dark={dark}
      expanded={isExpanded("maxmind")}
      onToggle={() => toggle("maxmind")}
      onDelete={handleDelete}
      deleteLabel="Disable"
      status={
        autoDownload ? (
          <span className={`text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}>enabled</span>
        ) : undefined
      }
      footer={
        <Button onClick={save} disabled={!isDirty || putConfig.isPending}>
          {putConfig.isPending ? "Saving..." : "Save"}
        </Button>
      }
    >
      <div className="flex flex-col gap-4">
        <Checkbox
          checked={autoDownload}
          onChange={setAutoDownload}
          label="Enable automatic database downloads"
          dark={dark}
        />

        <p className={`text-[0.8em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}>
          When enabled, GeoLite2-City and GeoLite2-ASN databases are downloaded automatically and
          updated twice weekly (Tue/Fri at 03:00). MMDB lookups with no uploaded file will use the
          auto-downloaded database matching their type. Requires a free{" "}
          <a
            href="https://www.maxmind.com/en/geolite2/signup"
            target="_blank"
            rel="noopener noreferrer"
            className="text-copper hover:underline"
          >
            MaxMind account
          </a>
          .
        </p>

        <FormField label="Account ID" description="Your MaxMind account ID (numeric)." dark={dark}>
          <TextInput
            value={accountId}
            onChange={setAccountId}
            placeholder={savedMaxmind?.licenseConfigured ? "(configured — leave blank to keep)" : ""}
            dark={dark}
            mono
          />
        </FormField>

        <FormField label="License Key" description="Your MaxMind license key." dark={dark}>
          <PasswordInput
            value={licenseKey}
            onChange={setLicenseKey}
            placeholder={savedMaxmind?.licenseConfigured ? "(configured — leave blank to keep)" : ""}
            dark={dark}
          />
        </FormField>

        {savedMaxmind?.lastUpdate && (
          <div className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            Last updated: {new Date(savedMaxmind.lastUpdate).toLocaleString()}
          </div>
        )}
      </div>
    </SettingsCard>
  );
}
