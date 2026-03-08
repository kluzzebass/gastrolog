import { useMutation, useQueryClient } from "@tanstack/react-query";
import { getToken } from "../client";

interface UploadResult {
  file_id: string;
  name: string;
  sha256: string;
  size: number;
  uploaded_at: string;
}

/**
 * Uploads a file to the managed file system via the multipart upload endpoint.
 * On success, invalidates config/settings queries so the UI picks up the new file.
 */
export function useUploadManagedFile() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (file: File): Promise<UploadResult> => {
      const form = new FormData();
      form.append("file", file);

      const headers: Record<string, string> = {};
      const token = getToken();
      if (token) {
        headers["Authorization"] = `Bearer ${token}`;
      }

      const res = await fetch("/api/v1/managed-files/upload", {
        method: "POST",
        headers,
        body: form,
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `Upload failed (${res.status})`);
      }
      return res.json();
    },
    onSuccess: () => {
      // The backend fires configSignal.Notify() which pushes via
      // WatchConfig, but invalidate eagerly for snappy feedback.
      qc.invalidateQueries({ queryKey: ["config"] });
      qc.invalidateQueries({ queryKey: ["settings"] });
    },
  });
}
