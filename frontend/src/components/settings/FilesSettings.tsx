import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { formatDateTimestamp } from "../../utils/temporal";
import { formatBytes } from "../../utils/units";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { useConfig } from "../../api/hooks/useSystem";
import { useSettings } from "../../api/hooks/useSettings";
import { useUploadManagedFile } from "../../api/hooks/useUploadManagedFile";
import { useDeleteManagedFile } from "../../api/hooks/useManagedFiles";
import { useToast } from "../Toast";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { ExpandableCard } from "./ExpandableCard";
import { MaxMindCard } from "./lookup/MaxMindCard";
import { handleDragOver, handleDragEnter, handleDragLeave } from "./CertificateForms";
import type { ManagedFileInfo } from "../../api/gen/gastrolog/v1/system_pb";

interface FileGroup {
  name: string;
  versions: ManagedFileInfo[]; // newest first (UUIDv7 order reversed)
}

/** Maps a filename to its known usage(s) in the system. */
function fileUsage(name: string): string[] {
  const uses: string[] = [];
  if (name.includes("City") && name.endsWith(".mmdb")) uses.push("lookup geoip");
  if ((name.includes("ASN") || name.includes("ISP")) && name.endsWith(".mmdb")) uses.push("lookup asn");
  return uses;
}

function groupByName(files: ManagedFileInfo[]): FileGroup[] {
  const map = new Map<string, ManagedFileInfo[]>();
  // Files come sorted by UUIDv7 (oldest first). We want newest first per group.
  for (const f of files) {
    const arr = map.get(f.name);
    if (arr) arr.unshift(f);
    else map.set(f.name, [f]);
  }
  // Sort groups by most recent upload (first version in each group).
  const groups = [...map.entries()].map(([name, versions]) => ({ name, versions }));
  groups.sort((a, b) => (b.versions[0]?.uploadedAt ?? "").localeCompare(a.versions[0]?.uploadedAt ?? ""));
  return groups;
}

export function FilesSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useConfig();
  const { data: settings } = useSettings();
  const uploadFile = useUploadManagedFile();
  const deleteFile = useDeleteManagedFile();
  const { addToast } = useToast();

  const savedMaxmind = settings?.maxmind;
  const [maxmindVisible, setMaxmindVisible] = useState(false);
  const [maxmindInit, setMaxmindInit] = useState(false);
  if (savedMaxmind && !maxmindInit) {
    setMaxmindVisible(savedMaxmind.autoDownload || savedMaxmind.licenseConfigured);
    setMaxmindInit(true);
  }

  const files = data?.managedFiles ?? [];
  const groups = groupByName(files);
  const initialExpanded = Object.fromEntries(groups.map((g) => [g.name, false]));
  const { toggle, isExpanded } = useExpandedCards(initialExpanded);

  const [dragging, setDragging] = useState(false);

  const doUpload = (file: File) => {
    uploadFile.mutate(file, {
      onSuccess: (result) => {
        addToast(`Uploaded ${result.name} (${formatBytes(result.size)})`, "info");
      },
      onError: (err) => {
        addToast(err instanceof Error ? err.message : "Upload failed", "error");
      },
    });
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) doUpload(file);
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) doUpload(file);
    e.target.value = "";
  };

  const handleDelete = (id: string, name: string) => {
    deleteFile.mutate(id, {
      onSuccess: () => addToast(`Deleted ${name}`, "info"),
      onError: (err) => addToast(err instanceof Error ? err.message : "Delete failed", "error"),
    });
  };

  if (isLoading) return <LoadingPlaceholder dark={dark} />;

  return (
    <div className="flex flex-col gap-3">
      <div
        role="button"
        tabIndex={0}
        onDragOver={handleDragOver}
        onDragEnter={(e) => { handleDragEnter(e); setDragging(true); }}
        onDragLeave={(e) => { handleDragLeave(e); if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragging(false); }}
        onDrop={handleDrop}
        className={`relative flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed px-4 py-6 transition-all cursor-pointer ${
          dragging
            ? "ring-2 ring-copper border-copper"
            : c("border-ink-border hover:border-copper-dim", "border-light-border hover:border-copper")
        } ${c("bg-ink-surface/50", "bg-light-surface/50")}`}
        onClick={() => {
          const input = document.getElementById("managed-file-upload") as HTMLInputElement | null;
          input?.click();
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            const input = document.getElementById("managed-file-upload") as HTMLInputElement | null;
            input?.click();
          }
        }}
      >
        <input
          id="managed-file-upload"
          type="file"
          className="hidden"
          onChange={handleFileInput}
        />

        {uploadFile.isPending ? (
          <span className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
            Uploading...
          </span>
        ) : (
          <>
            <span className={`text-[0.85em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}>
              Drop a file here to upload
            </span>
            <span className={`text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
              or click to browse
            </span>
          </>
        )}
      </div>

      <MaxMindCard
        dark={dark}
        visible={maxmindVisible}
        setVisible={setMaxmindVisible}
        savedMaxmind={savedMaxmind}
        addToast={addToast}
      />

      {!maxmindVisible && (
        <button
          onClick={() => setMaxmindVisible(true)}
          className={`w-full rounded-lg border border-dashed px-4 py-3 text-[0.8em] transition-colors ${c(
            "border-ink-border text-text-ghost hover:border-copper-dim hover:text-text-muted",
            "border-light-border text-light-text-ghost hover:border-copper hover:text-light-text-muted",
          )}`}
        >
          Enable MaxMind Auto-Download
        </button>
      )}

      {groups.length === 0 && !maxmindVisible && (
        <p className={`text-[0.8em] text-center py-4 ${c("text-text-ghost", "text-light-text-ghost")}`}>
          No managed files. Upload a file above to get started.
        </p>
      )}

      {groups.map((group) => {
        const latest = group.versions[0]!;
        const older = group.versions.slice(1);
        const uses = fileUsage(group.name);

        return (
          <ExpandableCard
            key={group.name}
            id={group.name}
            dark={dark}
            expanded={isExpanded(group.name)}
            onToggle={() => toggle(group.name)}
            typeBadge={formatBytes(Number(latest.size))}
            monoTitle
            status={uses.length > 0 ? (
              <span className="flex items-center gap-1.5">
                {uses.map((u) => (
                  <span key={u} className="font-mono text-[0.8em] text-copper">{u}</span>
                ))}
              </span>
            ) : undefined}
          >
            <div className="flex flex-col gap-3">
              <VersionRow file={latest} label="current" dark={dark} onDelete={() => handleDelete(latest.id, latest.name)} />

              {older.length > 0 && (
                <div className="flex flex-col gap-2">
                  <span className={`text-[0.75em] uppercase tracking-wider font-medium ${c("text-text-ghost", "text-light-text-ghost")}`}>
                    Previous versions ({older.length})
                  </span>
                  {older.map((v) => (
                    <VersionRow key={v.id} file={v} dark={dark} onDelete={() => handleDelete(v.id, v.name)} />
                  ))}
                </div>
              )}
            </div>
          </ExpandableCard>
        );
      })}
    </div>
  );
}

function VersionRow({
  file,
  label,
  dark,
  onDelete,
}: Readonly<{
  file: ManagedFileInfo;
  label?: string;
  dark: boolean;
  onDelete: () => void;
}>) {
  const c = useThemeClass(dark);
  const [confirmDelete, setConfirmDelete] = useState(false);

  return (
    <div className={`flex items-center gap-3 px-3 py-2 rounded text-[0.8em] ${c("bg-ink-surface", "bg-light-surface")}`}>
      <div className="flex-1 min-w-0 flex flex-col gap-0.5">
        <div className="flex items-center gap-2">
          <span className={`font-mono truncate ${c("text-text-muted", "text-light-text-muted")}`}>
            {file.sha256.slice(0, 12)}
          </span>
          <span className={c("text-text-ghost", "text-light-text-ghost")}>
            {formatBytes(Number(file.size))}
          </span>
          {file.uploadedAt && (
            <span className={c("text-text-ghost", "text-light-text-ghost")}>
              {formatDateTimestamp(new Date(file.uploadedAt))}
            </span>
          )}
          {label && (
            <span className="text-[0.85em] text-copper font-medium">{label}</span>
          )}
        </div>
      </div>

      {!confirmDelete ? (
        <button
          onClick={() => setConfirmDelete(true)}
          className={`shrink-0 px-2 py-1 rounded text-[0.85em] transition-colors ${c(
            "text-text-ghost hover:text-severity-error hover:bg-ink-hover",
            "text-light-text-ghost hover:text-severity-error hover:bg-light-hover",
          )}`}
        >
          Delete
        </button>
      ) : (
        <div className="shrink-0 flex items-center gap-1">
          <button
            onClick={() => { onDelete(); setConfirmDelete(false); }}
            className="px-2 py-1 rounded text-[0.85em] bg-severity-error/15 text-severity-error hover:bg-severity-error/25 transition-colors"
          >
            Yes
          </button>
          <button
            onClick={() => setConfirmDelete(false)}
            className={`px-2 py-1 rounded text-[0.85em] transition-colors ${c(
              "text-text-muted hover:bg-ink-hover",
              "text-light-text-muted hover:bg-light-hover",
            )}`}
          >
            No
          </button>
        </div>
      )}
    </div>
  );
}
