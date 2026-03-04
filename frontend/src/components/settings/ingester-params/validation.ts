// Returns true when the params are valid enough to save.
const VALID_MAP: Record<string, (params: Record<string, string>) => boolean> = {
  tail: (p) => !!p["paths"],
  mqtt: (p) => !!p["broker"] && !!p["topics"],
  kafka: (p) => !!p["brokers"] && !!p["topic"],
  syslog: (p) => !!p["udp_addr"] || !!p["tcp_addr"],
  relp: (p) => !!p["addr"],
  http: (p) => !!p["addr"],
  fluentfwd: (p) => !!p["addr"],
  otlp: (p) => !!p["http_addr"] || !!p["grpc_addr"],
};

export function isIngesterParamsValid(type: string, params: Record<string, string>): boolean {
  const check = VALID_MAP[type];
  return !check || check(params);
}
