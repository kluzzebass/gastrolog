import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { useServerConfig, usePutServerConfig } from "../../api/hooks/useConfig";
import { useToast } from "../Toast";
import { FormField, TextInput } from "./FormField";
import { PrimaryButton, GhostButton } from "./Buttons";

export function LookupsSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useServerConfig();
  const putConfig = usePutServerConfig();
  const { addToast } = useToast();

  const [geoipDbPath, setGeoipDbPath] = useState("");
  const [initialized, setInitialized] = useState(false);

  if (data && !initialized) {
    setGeoipDbPath(data.geoipDbPath ?? "");
    setInitialized(true);
  }

  const dirty = initialized && data && geoipDbPath !== (data.geoipDbPath ?? "");

  const handleSave = async () => {
    try {
      await putConfig.mutateAsync({ geoipDbPath });
      addToast("Lookup configuration updated", "info");
    } catch (err: any) {
      const msg = err.message ?? "Failed to update lookup configuration";
      addToast(msg, "error");
    }
  };

  const handleReset = () => {
    if (data) {
      setGeoipDbPath(data.geoipDbPath ?? "");
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Lookups
        </h2>
      </div>

      {isLoading ? (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Loading...
        </div>
      ) : (
        <div className="flex flex-col gap-8 max-w-lg">
          <section className="flex flex-col gap-5">
            <h3
              className={`text-[0.75em] uppercase tracking-wider font-medium pb-1 border-b ${c("text-text-ghost border-ink-border", "text-light-text-ghost border-light-border")}`}
            >
              GeoIP
            </h3>

            <FormField
              label="MaxMind MMDB Path"
              description="Path to a MaxMind GeoLite2 or GeoIP2 database file (.mmdb). Used by the `| lookup geoip` pipeline operator to enrich IP addresses with country, city, and ASN. The file is hot-reloaded on changes. Hot-reload does not follow symlinks."
              dark={dark}
            >
              <TextInput
                value={geoipDbPath}
                onChange={setGeoipDbPath}
                placeholder="path/to/GeoLite2-City.mmdb"
                dark={dark}
                mono
              />
            </FormField>
          </section>

          <div className="flex gap-2 mt-1">
            <PrimaryButton
              onClick={handleSave}
              disabled={!dirty || putConfig.isPending}
            >
              {putConfig.isPending ? "Saving..." : "Save"}
            </PrimaryButton>
            {dirty && (
              <GhostButton onClick={handleReset} dark={dark}>
                Reset
              </GhostButton>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
