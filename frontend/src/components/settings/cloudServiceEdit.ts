interface TransitionEdit {
  after: string;
  cloudStorageClass: string;
}

export interface CloudServiceEdit {
  name: string;
  provider: string;
  bucket: string;
  region: string;
  endpoint: string;
  accessKey: string;
  secretKey: string;
  credentialsConfigured: boolean;
  clearCredentials: boolean;
  container: string;
  connectionString: string;
  credentialsJson: string;
  archivalMode: string;
  transitions: TransitionEdit[];
  restoreSpeed: string;
  restoreDays: number;
  suspectGraceDays: number;
  reconcileSchedule: string;
}

export function cloudServiceSaveRequest(id: string, e: CloudServiceEdit) {
  return {
    id,
    clearCredentials: e.clearCredentials,
    name: e.name,
    provider: e.provider,
    bucket: e.bucket,
    region: e.region,
    endpoint: e.endpoint,
    accessKey: e.accessKey,
    secretKey: e.secretKey,
    container: e.container,
    connectionString: e.connectionString,
    credentialsJson: e.credentialsJson,
    archivalMode: e.archivalMode,
    transitions: e.transitions.map((t) => ({
      after: t.after,
      cloudStorageClass: t.cloudStorageClass,
    })),
    restoreSpeed: e.restoreSpeed,
    restoreDays: e.restoreDays,
    suspectGraceDays: e.suspectGraceDays,
    reconcileSchedule: e.reconcileSchedule,
  };
}
