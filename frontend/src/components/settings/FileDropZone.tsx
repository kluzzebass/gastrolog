import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { formatDateOnly } from "../../utils/temporal";
import { handleDragOver, handleDragEnter, handleDragLeave } from "./CertificateForms";
import type { ManagedFileInfo } from "../../api/gen/gastrolog/v1/config_pb";
import type { useUploadManagedFile } from "../../api/hooks/useUploadManagedFile";

export function FileDropZone({
  dark,
  inputId,
  accept,
  label,
  currentFile,
  pickableFiles,
  uploadFile,
  addToast,
  onFileSelected,
}: Readonly<{
  dark: boolean;
  inputId: string;
  accept: string;
  label: string;
  currentFile?: ManagedFileInfo;
  pickableFiles?: ManagedFileInfo[];
  uploadFile: ReturnType<typeof useUploadManagedFile>;
  addToast: (msg: string, type: "info" | "error") => void;
  onFileSelected?: (fileId: string) => void;
}>) {
  const c = useThemeClass(dark);
  const [dragging, setDragging] = useState(false);
  const [pendingFile, setPendingFile] = useState<{ name: string; size: number } | null>(null);

  const displayFile = currentFile ?? pendingFile;

  const doUpload = (file: File) => {
    const exts = accept.split(",").map((s) => s.trim());
    if (!exts.some((ext) => file.name.endsWith(ext))) {
      addToast(`Only ${accept} files are accepted`, "error");
      return;
    }
    setPendingFile({ name: file.name, size: file.size });
    uploadFile.mutate(file, {
      onSuccess: (result) => {
        addToast(`Uploaded ${result.name}`, "info");
        onFileSelected?.(result.file_id);
      },
      onError: (err) => {
        setPendingFile(null);
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

  const filteredPickable = pickableFiles?.filter((f) => f.id !== currentFile?.id) ?? [];

  return (
    <div className="flex flex-col gap-2">
      {displayFile && (
        <div className={`flex items-center gap-2 px-3 py-2 rounded text-[0.8em] ${c("bg-ink-surface", "bg-light-surface")}`}>
          <span className={`font-mono ${c("text-text-bright", "text-light-text-bright")}`}>
            {displayFile.name}
          </span>
          <span className={c("text-text-ghost", "text-light-text-ghost")}>
            {formatBytes(Number(displayFile.size))}
          </span>
          {"uploadedAt" in displayFile && displayFile.uploadedAt && (
            <span className={c("text-text-ghost", "text-light-text-ghost")}>
              &middot; {formatDateOnly(new Date(displayFile.uploadedAt))}
            </span>
          )}
        </div>
      )}

      <div
        role="button"
        tabIndex={0}
        aria-label="Click or drag files to upload"
        onDragOver={handleDragOver}
        onDragEnter={(e) => { handleDragEnter(e); setDragging(true); }}
        onDragLeave={(e) => { handleDragLeave(e); if (!e.currentTarget.contains(e.relatedTarget as Node)) setDragging(false); }}
        onDrop={handleDrop}
        className={`relative flex flex-col items-center justify-center gap-1 rounded-lg border-2 border-dashed px-4 py-4 transition-all cursor-pointer ${
          dragging
            ? "ring-2 ring-copper border-copper"
            : c("border-ink-border hover:border-copper-dim", "border-light-border hover:border-copper")
        } ${c("bg-ink-surface/50", "bg-light-surface/50")}`}
        onClick={() => {
          const input = document.getElementById(inputId) as HTMLInputElement | null;
          input?.click();
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            const input = document.getElementById(inputId) as HTMLInputElement | null;
            input?.click();
          }
        }}
      >
        <input
          id={inputId}
          type="file"
          accept={accept}
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
              {displayFile ? "Replace" : "Drop"} {label}
            </span>
            <span className={`text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
              or click to browse
            </span>
          </>
        )}
      </div>

      {pickableFiles && (
        <div className={`flex flex-col gap-1 rounded-lg border p-2 ${c("border-ink-border-subtle bg-ink-surface/50", "border-light-border-subtle bg-light-surface/50")}`}>
          <span className={`text-[0.75em] ${c("text-text-ghost", "text-light-text-ghost")}`}>
            Existing files
          </span>
          {filteredPickable.length === 0 ? (
            <span className={`text-[0.8em] italic ${c("text-text-ghost", "text-light-text-ghost")}`}>
              No files uploaded yet
            </span>
          ) : (
            filteredPickable.map((f) => (
              <button
                key={f.id}
                onClick={() => onFileSelected?.(f.id)}
                className={`flex items-center gap-2 px-2 py-1.5 rounded text-left text-[0.8em] transition-colors ${c(
                  "hover:bg-ink-hover",
                  "hover:bg-light-hover",
                )}`}
              >
                <span className={`font-mono ${c("text-text-bright", "text-light-text-bright")}`}>{f.name}</span>
                <span className={c("text-text-ghost", "text-light-text-ghost")}>{formatBytes(Number(f.size))}</span>
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
