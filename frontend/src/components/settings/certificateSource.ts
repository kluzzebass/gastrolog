/** The kind badge: where the certificate's key material lives. */
export function certSourceBadge(cert: { fromFiles: boolean }) {
  return cert.fromFiles
    ? { text: "files", title: "file-path certificate — read from files on disk, reloaded when they change", helpRef: "certificates#file-path-certificates" }
    : { text: "pem", title: "PEM material stored in the cluster configuration", helpRef: "certificates#adding-a-certificate" };
}
