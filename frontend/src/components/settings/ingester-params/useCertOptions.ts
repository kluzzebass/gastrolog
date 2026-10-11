import { encode } from "../../../api/glid";
import { useCertificates } from "../../../api/hooks/useCertificates";
import type { CertificateInfo } from "../../../api/gen/gastrolog/v1/system_pb";
import type { EligibilityOption } from "../EligibilitySelect";

/**
 * useCertOptions lists stored certificates as picker candidates, sorted by
 * name. Ingester params reference a certificate by name; Docker params by
 * ID — the caller says which.
 */
export function useCertOptions(valueOf: (c: CertificateInfo) => string = (c) => c.name || encode(c.id)): EligibilityOption[] {
  const { data: certsData } = useCertificates();
  const certs = certsData?.certificates ?? [];
  return certs
    .map((c) => ({ value: valueOf(c), label: c.name || encode(c.id) }))
    .sort((a, b) => a.label.localeCompare(b.label));
}
