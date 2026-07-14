// Client-side mirror of the backend's cloud endpoint refusal
// (backend/internal/blobstore/factory.go, validateEndpoint): a custom
// endpoint must carry an explicit scheme. A bare host:port used to be
// silently upgraded to plaintext "http://", pointing data-plane
// credentials at an unencrypted endpoint whenever an operator merely
// forgot the scheme. The form rejects it before submit; the backend
// refusal remains the backstop.
//
// This check is deliberately narrower than the backend's (which only
// requires some "://"): cloud object stores speak HTTP, so anything
// other than http:// or https:// is a mistake worth catching here.

/**
 * Returns a human-readable error when the endpoint value lacks an
 * explicit http:// or https:// scheme, or null when the value is
 * acceptable. An empty value is acceptable — whether the endpoint is
 * required at all is governed by the field's own semantics.
 */
export function endpointSchemeError(endpoint: string): string | null {
  if (endpoint === "") return null;
  const trimmed = endpoint.trim();
  if (/^https?:\/\//i.test(trimmed)) return null;
  const host = trimmed || "host:port";
  return `Endpoint has no scheme — use "https://${host}", or "http://${host}" for a plaintext local/dev endpoint`;
}

/**
 * Whether the endpoint value blocks submitting for this provider. Only
 * S3 and GCS take a custom endpoint; the backend refuses a scheme-less
 * endpoint for those providers at store creation (blobstore factory).
 */
export function endpointBlocked(provider: string, endpoint: string): boolean {
  if (provider !== "s3" && provider !== "gcs") return false;
  return endpointSchemeError(endpoint) !== null;
}
