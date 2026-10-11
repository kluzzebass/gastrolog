import { encode } from "../../../api/glid";

// Params an ingester cannot be saved without, by type. Each inner list is
// one requirement satisfied by any of its params; labels match the form fields.
const REQUIRED_PARAMS: Record<string, { key: string; label: string }[][]> = {
  tail: [[{ key: "paths", label: "File Patterns" }]],
  mqtt: [[{ key: "broker", label: "Broker" }], [{ key: "topics", label: "Topics" }]],
  kafka: [[{ key: "brokers", label: "Brokers" }], [{ key: "topic", label: "Topic" }]],
  syslog: [[{ key: "udp_addr", label: "UDP Address" }, { key: "tcp_addr", label: "TCP Address" }]],
  relp: [[{ key: "addr", label: "Listen Address" }]],
  http: [[{ key: "addr", label: "Listen Address" }]],
  fluentfwd: [[{ key: "addr", label: "Listen Address" }]],
  otlp: [[{ key: "http_addr", label: "HTTP Address" }, { key: "grpc_addr", label: "gRPC Address" }]],
};

/** One line naming the required params still empty, or undefined when none are. */
export function missingIngesterParamsReason(type: string, params: Record<string, string>): string | undefined {
  const missing = (REQUIRED_PARAMS[type] ?? [])
    .filter((alternatives) => !alternatives.some((p) => !!params[p.key]))
    .map((alternatives) => alternatives.map((p) => p.label).join(" or "));
  if (missing.length === 0) return undefined;
  return `${missing.join(" and ")} ${missing.length === 1 ? "is" : "are"} required`;
}

export function isIngesterParamsValid(type: string, params: Record<string, string>): boolean {
  return missingIngesterParamsReason(type, params) === undefined;
}

// Listen address extraction — mirrors the Go ListenAddrs functions.
interface ListenAddr {
  network: string;
  address: string;
}

type AddrExtractor = (params: Record<string, string>, defaults: Record<string, string>) => ListenAddr[];

const LISTEN_ADDRS: Record<string, AddrExtractor> = {
  syslog: (p, d) => {
    const addrs: ListenAddr[] = [];
    const udp = p["udp_addr"] || d["udp_addr"];
    if (udp) addrs.push({ network: "udp", address: udp });
    const tcp = p["tcp_addr"] || d["tcp_addr"];
    if (tcp) addrs.push({ network: "tcp", address: tcp });
    return addrs;
  },
  http: (p, d) => [{ network: "tcp", address: p["addr"] || d["addr"] || ":3100" }],
  fluentfwd: (p, d) => [{ network: "tcp", address: p["addr"] || d["addr"] || ":24224" }],
  relp: (p, d) => [{ network: "tcp", address: p["addr"] || d["addr"] || ":2514" }],
  otlp: (p, d) => {
    const addrs: ListenAddr[] = [];
    const grpc = p["grpc_addr"] || d["grpc_addr"];
    if (grpc) addrs.push({ network: "tcp", address: grpc });
    const http = p["http_addr"] || d["http_addr"];
    if (http) addrs.push({ network: "tcp", address: http });
    return addrs;
  },
};

function getListenAddrs(type: string, params: Record<string, string>, defaults: Record<string, string>): ListenAddr[] {
  const extractor = LISTEN_ADDRS[type];
  return extractor ? extractor(params, defaults) : [];
}

/** Returns a conflict message if any address in `wanted` overlaps with `other`. */
function findAddrOverlap(wanted: ListenAddr[], other: ListenAddr[]): string | null {
  for (const w of wanted) {
    for (const o of other) {
      if (w.network === o.network && w.address === o.address) {
        return `${w.network} ${w.address} is already used by another ingester`;
      }
    }
  }
  return null;
}

/** Check if an ingester's listen addresses conflict with any other configured ingester whose node set overlaps. */
export function listenAddrConflict(
  selfId: string,
  selfType: string,
  selfParams: Record<string, string>,
  selfNodeIds: string[],
  allIngesters: readonly { id: Uint8Array; type: string; params: { [key: string]: string }; nodeIds: Uint8Array[] }[],
  allDefaults: Record<string, Record<string, string>>,
): string | null {
  const selfDefaults = allDefaults[selfType] ?? {};
  const wanted = getListenAddrs(selfType, selfParams, selfDefaults);
  if (wanted.length === 0) return null;

  const selfSet = new Set(selfNodeIds);
  for (const other of allIngesters) {
    if (encode(other.id) === selfId) continue;
    const otherNodeIds = other.nodeIds.map(encode);
    // Empty set = all nodes → always overlaps. Otherwise check intersection.
    if (selfSet.size > 0 && otherNodeIds.length > 0 && !otherNodeIds.some((n) => selfSet.has(n))) continue;
    const otherDefaults = allDefaults[other.type] ?? {};
    const otherAddrs = getListenAddrs(other.type, other.params, otherDefaults);
    const conflict = findAddrOverlap(wanted, otherAddrs);
    if (conflict) return conflict;
  }
  return null;
}
