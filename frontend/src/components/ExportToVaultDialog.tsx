import { useState } from "react";
import { Dialog } from "./Dialog";
import { useConfig, useExportToVault } from "../api/hooks";
import { useToast } from "./Toast";
import { useThemeClass } from "../hooks/useThemeClass";
import { encode } from "../api/glid";

interface ExportToVaultDialogProps {
  dark: boolean;
  expression: string;
  onClose: () => void;
}

export function ExportToVaultDialog({
  dark,
  expression,
  onClose,
}: Readonly<ExportToVaultDialogProps>) {
  const c = useThemeClass(dark);
  const { data: config } = useConfig();
  const exportMutation = useExportToVault();
  const { addToast } = useToast();
  const [selectedVaultId, setSelectedVaultId] = useState("");

  const vaults = (config?.vaults ?? [])
    .map((v) => ({ id: v.id, name: v.name }))
    .sort((a, b) => a.name.localeCompare(b.name));

  const handleExport = () => {
    if (!selectedVaultId) return;
    exportMutation.mutate(
      { expression, target: selectedVaultId },
      {
        onSuccess: (jobId) => {
          addToast(`Export started (job ${jobId})`, "info");
          onClose();
        },
        onError: (err) => {
          addToast(
            `Export failed: ${err instanceof Error ? err.message : String(err)}`,
            "error",
          );
        },
      },
    );
  };

  return (
    <Dialog onClose={onClose} ariaLabel="Export to vault" dark={dark} size="sm">
      <h2
        className={`font-display text-lg font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}
      >
        Export to vault
      </h2>
      <p
        className={`text-sm mb-4 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Search results will be appended to the selected vault as a background
        job.
      </p>
      <label
        className={`block text-xs font-mono mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Target vault
      </label>
      <select
        value={selectedVaultId}
        onChange={(e) => setSelectedVaultId(e.target.value)}
        className={`w-full rounded border px-3 py-2 text-sm font-mono mb-6 ${c(
          "bg-ink-well border-ink-border text-text-bright",
          "bg-light-well border-light-border text-light-text-bright",
        )}`}
      >
        <option value="">Select a vault...</option>
        {vaults.map((v) => (
          <option key={encode(v.id)} value={encode(v.id)}>
            {v.name}
          </option>
        ))}
      </select>
      <div className="flex justify-end gap-3">
        <button
          onClick={onClose}
          className={`px-4 py-2 text-sm rounded transition-colors ${c(
            "text-text-muted hover:text-text-bright",
            "text-light-text-muted hover:text-light-text-bright",
          )}`}
        >
          Cancel
        </button>
        <button
          onClick={handleExport}
          disabled={!selectedVaultId || exportMutation.isPending}
          className={`px-4 py-2 text-sm rounded font-medium transition-colors disabled:opacity-40 disabled:cursor-not-allowed ${c(
            "bg-copper text-ink-bg hover:bg-copper-bright",
            "bg-copper text-white hover:bg-copper-bright",
          )}`}
        >
          {exportMutation.isPending ? "Exporting..." : "Export"}
        </button>
      </div>
    </Dialog>
  );
}
