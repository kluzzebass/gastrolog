import { useThemeClass } from "../../hooks/useThemeClass";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { FormField, TextInput, TextArea } from "./FormField";

// -- Archive parsing utilities --

function stripNul(s: string): string {
  const idx = s.indexOf("\0");
  return idx !== -1 ? s.slice(0, idx) : s;
}

export function parseTar(data: Uint8Array): [string, Uint8Array][] {
  const entries: [string, Uint8Array][] = [];
  let offset = 0;
  while (offset + 512 <= data.length) {
    const header = data.subarray(offset, offset + 512);
    const name = stripNul(new TextDecoder().decode(header.subarray(0, 100)));
    const sizeStr = stripNul(new TextDecoder().decode(header.subarray(124, 136)));
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

// -- Drag helpers --

export function handleDragOver(e: React.DragEvent) {
  e.preventDefault();
  e.dataTransfer.dropEffect = "copy";
}

export function handleDragEnter(e: React.DragEvent) {
  e.preventDefault();
  e.currentTarget.classList.add("ring-2", "ring-copper");
}

export function handleDragLeave(e: React.DragEvent) {
  e.preventDefault();
  if (!e.currentTarget.contains(e.relatedTarget as Node)) {
    e.currentTarget.classList.remove("ring-2", "ring-copper");
  }
}

// -- PEM cert form --

export interface PemCertFormProps {
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

export function PemCertForm({
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
          <TextArea
            value={certPem}
            onChange={setCertPem}
            placeholder="-----BEGIN CERTIFICATE-----\n..."
            rows={6}
            dark={dark}
            className="w-full app-scroll app-resize"
          />
        </FormField>
        <FormField
          label="Private key (PEM)"
          description={isEditing ? "Private key not shown; paste new key to replace" : "PEM-encoded private key content"}
          dark={dark}
        >
          <TextArea
            value={keyPem}
            onChange={setKeyPem}
            placeholder={isEditing ? "•••••••• (paste to replace)" : "-----BEGIN PRIVATE KEY-----\n..."}
            rows={6}
            dark={dark}
            className="w-full app-scroll app-resize"
          />
        </FormField>
        <Checkbox
          checked={setAsDefault}
          onChange={setSetAsDefault}
          label="Use as default for TLS-wrapping the main server"
          dark={dark}
        />
        {!hideActions && (
          <div className="flex justify-end gap-2 pt-2">
            {onCancel && (
              <Button variant="ghost" onClick={onCancel} dark={dark}>
                Cancel
              </Button>
            )}
            <Button onClick={onSave} disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}

// -- Files cert form --

export interface FilesCertFormProps {
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

export function FilesCertForm({
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
        <Checkbox
          checked={setAsDefault}
          onChange={setSetAsDefault}
          label="Use as default for TLS-wrapping the main server"
          dark={dark}
        />
        {!hideActions && (
          <div className="flex justify-end gap-2 pt-2">
            {onCancel && (
              <Button variant="ghost" onClick={onCancel} dark={dark}>
                Cancel
              </Button>
            )}
            <Button onClick={onSave} disabled={saving}>
              {saving ? "Saving..." : "Save"}
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}

// -- Add certificate panel --

export function AddCertificatePanel({
  title,
  dark,
  name,
  setName,
  onDrop,
  onDragOver,
  onDragEnter,
  onDragLeave,
  children,
}: Readonly<{
  title: string;
  dark: boolean;
  name: string;
  setName: (v: string) => void;
  onDrop?: (e: React.DragEvent) => void;
  onDragOver?: (e: React.DragEvent) => void;
  onDragEnter?: (e: React.DragEvent) => void;
  onDragLeave?: (e: React.DragEvent) => void;
  children: React.ReactNode;
}>) {
  const c = useThemeClass(dark);
  return (
    <div
      className={`border rounded-lg p-4 mb-5 transition-all ${c("border-copper/40 bg-ink-surface", "border-copper/40 bg-light-surface")}`}
      onDrop={onDrop}
      onDragOver={onDragOver}
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
    >
      <h3
        className={`text-[1em] font-semibold mb-4 ${c("text-text-bright", "text-light-text-bright")}`}
      >
        {title}
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
        {children}
      </div>
    </div>
  );
}
