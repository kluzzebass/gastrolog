import { encode } from "../../../api/glid";

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

/** Check if an ingester's listen addresses conflict with any other configured ingester on the same node. */
export function listenAddrConflict(
  selfId: string,
  selfType: string,
  selfParams: Record<string, string>,
  selfNodeId: string,
  allIngesters: readonly { id: Uint8Array; type: string; params: { [key: string]: string }; nodeId: Uint8Array }[],
  allDefaults: Record<string, Record<string, string>>,
): string | null {
  const selfDefaults = allDefaults[selfType] ?? {};
  const wanted = getListenAddrs(selfType, selfParams, selfDefaults);
  if (wanted.length === 0) return null;

  for (const other of allIngesters) {
    if (encode(other.id) === selfId) continue;
    if (encode(other.nodeId) !== selfNodeId) continue;
    const otherDefaults = allDefaults[other.type] ?? {};
    const otherAddrs = getListenAddrs(other.type, other.params, otherDefaults);
    const conflict = findAddrOverlap(wanted, otherAddrs);
    if (conflict) return conflict;
  }
  return null;
}
