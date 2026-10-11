import { useState } from "react";
import { useThemeClass } from "../../../hooks/useThemeClass";
import { formatDateTimestamp } from "../../../utils/temporal";
import { usePutMaxMindSettings, MAXMIND_KEEP } from "../../../api/hooks/useSettings";
import { useExpandedCards } from "../../../hooks/useExpandedCards";
import { FormField, TextInput } from "../FormField";
import { Checkbox } from "../Checkbox";
import { Button } from "../Buttons";
import { SettingsCard } from "../SettingsCard";
import { PasswordInput } from "./FormHelpers";
import type { MaxMindSettings } from "../../../api/gen/gastrolog/v1/system_pb";

/**
 * The MaxMind auto-download settings, always on screen: its status badge
 * says whether downloads run, so there is no hidden "not set up yet" state.
 */
export function MaxMindCard({
  dark,
  savedMaxmind,
  addToast,
}: Readonly<{
  dark: boolean;
  savedMaxmind?: MaxMindSettings;
  addToast: (msg: string, type: "info" | "error") => void;
}>) {
  const c = useThemeClass(dark);
  const putConfig = usePutMaxMindSettings();
  const { isExpanded, toggle } = useExpandedCards();

  const [autoDownload, setAutoDownload] = useState(savedMaxmind?.autoDownload ?? false);
  const [accountId, setAccountId] = useState("");
  const [licenseKey, setLicenseKey] = useState("");
  const [justSaved, setJustSaved] = useState(false);

  const isDirty =
    !justSaved &&
    (autoDownload !== (savedMaxmind?.autoDownload ?? false) ||
      accountId !== "" ||
      licenseKey !== "");

  const discard = () => {
    setAutoDownload(savedMaxmind?.autoDownload ?? false);
    setAccountId("");
    setLicenseKey("");
  };

  const save = async () => {
    const maxmind = {
      autoDownload,
      accountId: accountId || undefined,
      licenseKey: licenseKey || MAXMIND_KEEP,
    };
    try {
      await putConfig.mutateAsync({ maxmind: { ...maxmind } });
      setJustSaved(true);
      requestAnimationFrame(() => setJustSaved(false));
      setAccountId("");
      setLicenseKey("");
      addToast("MaxMind configuration updated", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to update MaxMind", "error");
    }
  };

  const handleDisable = async () => {
    try {
      await putConfig.mutateAsync({ maxmind: { autoDownload: false } });
      setAutoDownload(false);
      setAccountId("");
      setLicenseKey("");
      addToast("MaxMind auto-download disabled", "info");
    } catch (err: unknown) {
      addToast(err instanceof Error ? err.message : "Failed to update MaxMind", "error");
    }
  };

  const savedEnabled = savedMaxmind?.autoDownload ?? false;

  return (
    <SettingsCard
      id="MaxMind Auto-Download"
      typeBadge="maxmind"
      helpRef="managed-files#maxmind-auto-download"
      dark={dark}
      expanded={isExpanded("maxmind")}
      onToggle={() => toggle("maxmind")}
      onDelete={savedEnabled ? handleDisable : undefined}
      deleteLabel="Disable"
      status={
        <span className={`text-[0.75em] ${c("text-text-muted", "text-light-text-muted")}`}>
          {savedEnabled ? "enabled" : "disabled"}
        </span>
      }
      footer={
        <>
          {isDirty && (
            <Button onClick={discard} disabled={putConfig.isPending} dark={dark} variant="ghost">
              Discard
            </Button>
          )}
          <Button onClick={save} disabled={!isDirty || putConfig.isPending}>
            {putConfig.isPending ? "Saving..." : "Save"}
          </Button>
        </>
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
          <div className={`text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}>
            Last updated: {formatDateTimestamp(new Date(savedMaxmind.lastUpdate))}
          </div>
        )}
      </div>
    </SettingsCard>
  );
}
