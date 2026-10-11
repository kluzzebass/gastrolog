// Memory is a real provider the server accepts: an in-process store whose
// contents vanish with the node. Its label says so, since nothing else
// about a "cloud" picker would.
export const providerOptions = [
  { value: "s3", label: "S3" },
  { value: "gcs", label: "GCS" },
  { value: "azure", label: "Azure" },
  { value: "memory", label: "Memory — in-process test store, lost on restart" },
];

/** Why a provider has no restore speed to choose, or undefined when it has one. */
export function restoreSpeedReason(provider: string): string | undefined {
  switch (provider) {
    case "gcs":
      return "GCS has no restore step — archived objects are read directly.";
    case "memory":
      return "The memory provider has no restore step.";
    default:
      return undefined;
  }
}

/** Why a provider has no restore window, or undefined when it has one. */
export function restoreDaysReason(provider: string): string | undefined {
  if (provider === "s3") return undefined;
  return "S3 only — a restored object expires after this many days; other providers restore in place.";
}
