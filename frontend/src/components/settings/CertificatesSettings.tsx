import { useState, useEffect, useCallback } from "react";
import { unzipSync, decompressSync } from "fflate";
import { HelpButton } from "../HelpButton";

function parseTar(data: Uint8Array): [string, Uint8Array][] {
  const entries: [string, Uint8Array][] = [];
  let offset = 0;
  while (offset + 512 <= data.length) {
    const header = data.subarray(offset, offset + 512);
    const name = new TextDecoder().decode(header.subarray(0, 100)).replace(/\0.*$/, "");
    const sizeStr = new TextDecoder().decode(header.subarray(124, 136)).replace(/\0.*$/, "");
    const size = parseInt(sizeStr, 8) || 0;
    offset += 512;
    if (name && size > 0 && offset + size <= data.length) {
      entries.push([name, data.subarray(offset, offset + size)]);
    }
    offset += size;
    offset = Math.ceil(offset / 512) * 512;
  }
  return entries;
}

import { useThemeClass } from "../../hooks/useThemeClass";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
import {
  useCertificates,
  useCertificate,
  usePutCertificate,
  useDeleteCertificate,
  useServerConfig,
} from "../../api/hooks/useConfig";
import { useToast } from "../Toast";
import { FormField, TextInput } from "./FormField";
import { SettingsCard } from "./SettingsCard";

type CertSource = "pem" | "files";

interface PemCertFormProps {
  dark: boolean;
  name: string;
  certPem: string;
  keyPem: string;
  setAsDefault: boolean;
  setCertPem: (v: string) => void;
  setKeyPem: (v: string) => void;
  setSetAsDefault: (v: boolean) => void;
  onSave: () => void;
  onCancel?: () => void;
  saving?: boolean;
  onDrop: (e: React.DragEvent) => void;
  onDragOver: (e: React.DragEvent) => void;
  onDragEnter: (e: React.DragEvent) => void;
  onDragLeave: (e: React.DragEvent) => void;
  isEditing?: boolean;
  hideActions?: boolean;
}

function PemCertForm({
  dark,
  name: _name,
  certPem,
  keyPem,
  setAsDefault,
  setCertPem,
  setKeyPem,
  setSetAsDefault,
  onSave,
  onCancel,
  saving,
  onDrop,
  onDragOver,
  onDragEnter,
  onDragLeave,
  isEditing,
  hideActions,
}: Readonly<PemCertFormProps>) {
  const c = useThemeClass(dark);
  return (
    <div
      onDrop={onDrop}
      onDragOver={onDragOver}
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
      className="transition-all"
    >
      <p
        className={`text-[0.8em] mb-3 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Paste PEM content or drop .pem, .crt, .key, .zip, or .tar(.gz) files.
      </p>
      <div className="flex flex-col gap-4">
        <FormField
          label="Certificate (PEM)"
          description="PEM-encoded certificate content"
          dark={dark}
        >
          <textarea
            value={certPem}
            onChange={(e) => setCertPem(e.target.value)}
            placeholder="-----BEGIN CERTIFICATE-----\n..."
            rows={6}
            className={`w-full px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors resize-y app-scroll app-resize ${c(
              "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
            )}`}
          />
        </FormField>
        <FormField
          label="Private key (PEM)"
          description={isEditing ? "Private key not shown; paste new key to replace" : "PEM-encoded private key content"}
          dark={dark}
        >
          <textarea
            value={keyPem}
            onChange={(e) => setKeyPem(e.target.value)}
            placeholder={isEditing ? "•••••••• (paste to replace)" : "-----BEGIN PRIVATE KEY-----\n..."}
            rows={6}
            className={`w-full px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors resize-y app-scroll app-resize ${c(
              "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
              "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
            )}`}
          />
        </FormField>
        <FormField
          label="Use as default"
          description="Use this certificate for TLS-wrapping the main server (when HTTPS is enabled)"
          dark={dark}
        >
          <Checkbox
            checked={setAsDefault}
            onChange={setSetAsDefault}
            dark={dark}
          />
        </FormField>
        {!hideActions && (
          <div className="flex justify-end gap-2 pt-2">
            {onCancel && (
              <GhostButton onClick={onCancel} dark={dark}>
                Cancel
              </GhostButton>
            )}
            <PrimaryButton onClick={onSave} disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </PrimaryButton>
          </div>
        )}
      </div>
    </div>
  );
}

interface FilesCertFormProps {
  dark: boolean;
  name: string;
  certFile: string;
  keyFile: string;
  setAsDefault: boolean;
  setCertFile: (v: string) => void;
  setKeyFile: (v: string) => void;
  setSetAsDefault: (v: boolean) => void;
  onSave: () => void;
  onCancel?: () => void;
  saving?: boolean;
  hideActions?: boolean;
}

function FilesCertForm({
  dark,
  name: _name,
  certFile,
  keyFile,
  setAsDefault,
  setCertFile,
  setKeyFile,
  setSetAsDefault,
  onSave,
  onCancel,
  saving,
  hideActions,
}: Readonly<FilesCertFormProps>) {
  const c = useThemeClass(dark);
  return (
    <div className="transition-all">
      <p
        className={`text-[0.8em] mb-3 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        Paths to certificate and key files on the server. Files are monitored and
        reloaded when they change.
      </p>
      <div className="flex flex-col gap-4">
        <FormField
          label="Certificate file path"
          description="Path to the PEM-encoded certificate file"
          dark={dark}
        >
          <TextInput
            value={certFile}
            onChange={setCertFile}
            placeholder="/path/to/cert.pem"
            dark={dark}
            mono
          />
        </FormField>
        <FormField
          label="Key file path"
          description="Path to the PEM-encoded private key file"
          dark={dark}
        >
          <TextInput
            value={keyFile}
            onChange={setKeyFile}
            placeholder="/path/to/key.pem"
            dark={dark}
            mono
          />
        </FormField>
        <FormField
          label="Use as default"
          description="Use this certificate for TLS-wrapping the main server (when HTTPS is enabled)"
          dark={dark}
        >
          <Checkbox
            checked={setAsDefault}
            onChange={setSetAsDefault}
            dark={dark}
          />
        </FormField>
        {!hideActions && (
          <div className="flex justify-end gap-2 pt-2">
            {onCancel && (
              <GhostButton onClick={onCancel} dark={dark}>
                Cancel
              </GhostButton>
            )}
            <PrimaryButton onClick={onSave} disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </PrimaryButton>
          </div>
        )}
      </div>
    </div>
  );
}

export function CertificatesSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const [adding, setAdding] = useState<CertSource | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [certPem, setCertPem] = useState("");
  const [keyPem, setKeyPem] = useState("");
  const [certFile, setCertFile] = useState("");
  const [keyFile, setKeyFile] = useState("");
  const [setAsDefault, setSetAsDefault] = useState(false);

  const { data, isLoading } = useCertificates();
  const { data: serverConfig } = useServerConfig();
  const { data: certData } = useCertificate(expanded);
  const putCert = usePutCertificate();
  const deleteCert = useDeleteCertificate();
  const { addToast } = useToast();

  const certs = data?.certificates ?? [];
  const defaultCert = serverConfig?.tlsDefaultCert ?? "";

  const resetForm = () => {
    setName("");
    setCertPem("");
    setKeyPem("");
    setCertFile("");
    setKeyFile("");
    setSetAsDefault(false);
    setAdding(null);
    setExpanded(null);
  };

  const handleSavePem = async () => {
    const n = name.trim();
    if (!n) {
      addToast("Name is required", "error");
      return;
    }
    if (!certPem.trim() || !keyPem.trim()) {
      addToast("Certificate and private key PEM are required", "error");
      return;
    }
    try {
      await putCert.mutateAsync({
        id: "",
        name: n,
        certPem: certPem.trim(),
        keyPem: keyPem.trim(),
        certFile: "",
        keyFile: "",
        setAsDefault,
      });
      addToast(`Certificate "${n}" saved`, "info");
      resetForm();
    } catch (err: unknown) {
      addToast((err as Error)?.message ?? "Failed to save certificate", "error");
    }
  };

  const handleSaveFiles = async () => {
    const n = name.trim();
    if (!n) {
      addToast("Name is required", "error");
      return;
    }
    if (!certFile.trim() || !keyFile.trim()) {
      addToast("Certificate and key file paths are required", "error");
      return;
    }
    try {
      await putCert.mutateAsync({
        id: "",
        name: n,
        certPem: "",
        keyPem: "",
        certFile: certFile.trim(),
        keyFile: keyFile.trim(),
        setAsDefault,
      });
      addToast(`Certificate "${n}" saved`, "info");
      resetForm();
    } catch (err: unknown) {
      addToast((err as Error)?.message ?? "Failed to save certificate", "error");
    }
  };

  const handleSaveEditPem = async (id: string) => {
    if (!certPem.trim()) {
      addToast("Certificate PEM is required", "error");
      return;
    }
    const certName = certs.find((c) => c.id === id)?.name ?? "";
    // keyPem empty when editing means keep existing key
    try {
      await putCert.mutateAsync({
        id,
        name: certName,
        certPem: certPem.trim(),
        keyPem: keyPem.trim(),
        certFile: "",
        keyFile: "",
        setAsDefault,
      });
      addToast(`Certificate "${certName || id}" saved`, "info");
      setExpanded(null);
    } catch (err: unknown) {
      addToast((err as Error)?.message ?? "Failed to save certificate", "error");
    }
  };

  const handleSaveEditFiles = async (id: string) => {
    if (!certFile.trim() || !keyFile.trim()) {
      addToast("Certificate and key file paths are required", "error");
      return;
    }
    const certName = certs.find((c) => c.id === id)?.name ?? "";
    try {
      await putCert.mutateAsync({
        id,
        name: certName,
        certPem: "",
        keyPem: "",
        certFile: certFile.trim(),
        keyFile: keyFile.trim(),
        setAsDefault,
      });
      addToast(`Certificate "${certName || id}" saved`, "info");
      setExpanded(null);
    } catch (err: unknown) {
      addToast((err as Error)?.message ?? "Failed to save certificate", "error");
    }
  };

  const handleDelete = async (id: string) => {
    const certName = certs.find((c) => c.id === id)?.name ?? id;
    try {
      await deleteCert.mutateAsync(id);
      addToast(`Certificate "${certName}" deleted`, "info");
    } catch (err: unknown) {
      addToast((err as Error)?.message ?? "Failed to delete certificate", "error");
    }
  };

  const startAddPem = () => {
    resetForm();
    setAdding("pem");
  };

  const startAddFiles = () => {
    resetForm();
    setAdding("files");
  };

  useEffect(() => {
    if (expanded && certData && certData.id === expanded) {
      const _isFileBased = !!(certData.certFile && certData.keyFile);
      setCertPem(certData.certPem ?? "");
      setKeyPem("");
      setCertFile(certData.certFile ?? "");
      setKeyFile(certData.keyFile ?? "");
      setSetAsDefault(defaultCert === expanded);
    }
  }, [expanded, certData, defaultCert]);

  const assignPem = useCallback((text: string) => {
    if (
      text.includes("-----BEGIN CERTIFICATE-----") ||
      text.includes("-----BEGIN TRUSTED CERTIFICATE-----")
    ) {
      setCertPem(text.trim());
    } else if (
      text.includes("-----BEGIN PRIVATE KEY-----") ||
      text.includes("-----BEGIN RSA PRIVATE KEY-----") ||
      text.includes("-----BEGIN EC PRIVATE KEY-----") ||
      text.includes("-----BEGIN ENCRYPTED PRIVATE KEY-----")
    ) {
      setKeyPem(text.trim());
    }
  }, []);

  const handleDrop = useCallback(
    async (e: React.DragEvent) => {
      e.preventDefault();
      e.currentTarget.classList.remove("ring-2", "ring-copper");
      const files = Array.from(e.dataTransfer.files);
      if (files.length === 0) return;
      for (const file of files) {
        const name = file.name.toLowerCase();
        if (name.endsWith(".zip")) {
          const buf = await file.arrayBuffer();
          const unzipped = unzipSync(new Uint8Array(buf));
          for (const [_path, data] of Object.entries(unzipped)) {
            const text = new TextDecoder().decode(data);
            assignPem(text);
          }
        } else if (name.endsWith(".tar.gz") || name.endsWith(".tgz") || name.endsWith(".tar")) {
          const buf = await file.arrayBuffer();
          let data = new Uint8Array(buf);
          if (name.endsWith(".gz") || name.endsWith(".tgz")) {
            data = decompressSync(data) as Uint8Array<ArrayBuffer>;
          }
          const entries = parseTar(data);
          for (const [, content] of entries) {
            const text = new TextDecoder().decode(content);
            assignPem(text);
          }
        } else {
          const text = await file.text();
          assignPem(text);
        }
      }
    },
    [assignPem],
  );

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }, []);

  const handleDragEnter = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.currentTarget.classList.add("ring-2", "ring-copper");
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    if (!e.currentTarget.contains(e.relatedTarget as Node)) {
      e.currentTarget.classList.remove("ring-2", "ring-copper");
    }
  }, []);

  const isExpandedFileBased = expanded && certData && certData.id === expanded && !!(certData.certFile && certData.keyFile);

  if (isLoading) {
    return (
      <div
        className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
      >
        Loading...
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-2">
          <h2
            className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Certificates
          </h2>
          <HelpButton topicId="certificates" />
        </div>
        {!adding && !expanded && (
          <div className="flex gap-2">
            <PrimaryButton onClick={startAddPem}>
              Add pasted certificate
            </PrimaryButton>
            <PrimaryButton onClick={startAddFiles}>
              Add monitored files
            </PrimaryButton>
          </div>
        )}
      </div>

      <p
        className={`text-[0.85em] mb-5 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        TLS certificates for the server, ingesters, or storage engines. Choose
        either pasted PEM content (stored in config) or file paths (monitored for
        changes).
      </p>

      {adding === "pem" && (
        <div
          className={`border rounded-lg p-4 mb-5 transition-all ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
          onDrop={handleDrop}
          onDragOver={handleDragOver}
          onDragEnter={handleDragEnter}
          onDragLeave={handleDragLeave}
        >
          <h3
            className={`text-[1em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Add pasted certificate
          </h3>
          <div className="flex flex-col gap-4">
            <FormField
              label="Name"
              description="Identifier for this certificate (e.g. server, ingester.http)"
              dark={dark}
            >
              <TextInput
                value={name}
                onChange={setName}
                placeholder="server"
                dark={dark}
                mono
              />
            </FormField>
            <PemCertForm
              dark={dark}
              name={name}
              certPem={certPem}
              keyPem={keyPem}
              setAsDefault={setAsDefault}
              setCertPem={setCertPem}
              setKeyPem={setKeyPem}
              setSetAsDefault={setSetAsDefault}
              onSave={handleSavePem}
              onCancel={resetForm}
              saving={putCert.isPending}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
              onDragEnter={handleDragEnter}
              onDragLeave={handleDragLeave}
              isEditing={false}
            />
          </div>
        </div>
      )}

      {adding === "files" && (
        <div
          className={`border rounded-lg p-4 mb-5 transition-all ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
        >
          <h3
            className={`text-[1em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Add monitored files certificate
          </h3>
          <div className="flex flex-col gap-4">
            <FormField
              label="Name"
              description="Identifier for this certificate (e.g. server, ingester.http)"
              dark={dark}
            >
              <TextInput
                value={name}
                onChange={setName}
                placeholder="server"
                dark={dark}
                mono
              />
            </FormField>
            <FilesCertForm
              dark={dark}
              name={name}
              certFile={certFile}
              keyFile={keyFile}
              setAsDefault={setAsDefault}
              setCertFile={setCertFile}
              setKeyFile={setKeyFile}
              setSetAsDefault={setSetAsDefault}
              onSave={handleSaveFiles}
              onCancel={resetForm}
              saving={putCert.isPending}
            />
          </div>
        </div>
      )}

      {certs.length > 0 && (
        <div className="flex flex-col gap-3">
          {certs.map((cert) => (
            <SettingsCard
              key={cert.id}
              id={cert.name || cert.id}
              typeBadge={defaultCert === cert.id ? "default" : undefined}
              dark={dark}
              expanded={expanded === cert.id}
              onToggle={() => {
                if (expanded === cert.id) {
                  setExpanded(null);
                } else {
                  setCertPem("");
                  setKeyPem("");
                  setCertFile("");
                  setKeyFile("");
                  setExpanded(cert.id);
                }
              }}
              onDelete={() => handleDelete(cert.id)}
              deleteLabel="Delete"
              footer={
                expanded === cert.id ? (
                  <PrimaryButton
                    onClick={() =>
                      isExpandedFileBased
                        ? handleSaveEditFiles(cert.id)
                        : handleSaveEditPem(cert.id)
                    }
                    disabled={putCert.isPending}
                  >
                    {putCert.isPending ? "Saving..." : "Save"}
                  </PrimaryButton>
                ) : undefined
              }
            >
              {expanded === cert.id && (
                isExpandedFileBased ? (
                  <FilesCertForm
                    dark={dark}
                    name={cert.name || cert.id}
                    certFile={certFile}
                    keyFile={keyFile}
                    setAsDefault={setAsDefault}
                    setCertFile={setCertFile}
                    setKeyFile={setKeyFile}
                    setSetAsDefault={setSetAsDefault}
                    onSave={() => handleSaveEditFiles(cert.id)}
                    saving={putCert.isPending}
                    hideActions
                  />
                ) : (
                  <PemCertForm
                    dark={dark}
                    name={cert.name || cert.id}
                    certPem={certPem}
                    keyPem={keyPem}
                    setAsDefault={setAsDefault}
                    setCertPem={setCertPem}
                    setKeyPem={setKeyPem}
                    setSetAsDefault={setSetAsDefault}
                    onSave={() => handleSaveEditPem(cert.id)}
                    saving={putCert.isPending}
                    onDrop={handleDrop}
                    onDragOver={handleDragOver}
                    onDragEnter={handleDragEnter}
                    onDragLeave={handleDragLeave}
                    isEditing
                    hideActions
                  />
                )
              )}
            </SettingsCard>
          ))}
        </div>
      )}

      {certs.length === 0 && !adding && (
        <div
          className={`text-[0.85em] py-8 text-center ${c("text-text-muted", "text-light-text-muted")}`}
        >
          No certificates configured. Add one to enable TLS.
        </div>
      )}
    </div>
  );
}
