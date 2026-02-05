// Mock data for design previews
import type { Record, StoreInfo, ChunkMeta, Stats, ChunkPlan } from '../types/api'

const logMessages = [
  'INFO  [http-server] Request completed method=GET path=/api/users status=200 duration=45ms',
  'DEBUG [db-pool] Connection acquired pool_size=10 active=3 idle=7',
  'WARN  [auth] Rate limit approaching user_id=usr_8x7k2m threshold=0.85',
  'ERROR [payment] Transaction failed tx_id=tx_9f3k2 error="insufficient funds" amount=150.00 currency=USD',
  'INFO  [scheduler] Job completed job_id=cleanup_logs records_deleted=1523 duration=2.3s',
  'DEBUG [cache] Cache miss key="user:profile:12345" ttl=3600',
  'INFO  [websocket] Client connected client_id=ws_k2m9x remote_addr=192.168.1.105',
  'ERROR [storage] Disk space critical node=storage-03 available=2.1GB threshold=5GB',
  'WARN  [metrics] High memory usage service=api-gateway percent=87.3',
  'INFO  [deploy] Rolling update started version=v2.4.1 replicas=5',
  'DEBUG [grpc] Stream opened service=QueryService method=Search',
  'ERROR [ssl] Certificate expiring in 7 days domain=api.example.com expires=2024-02-15',
  'INFO  [audit] User action user_id=admin action=delete_record target=log_chunk_0x8f2k',
  'WARN  [circuit-breaker] Circuit opened service=payment-gateway failures=5 timeout=30s',
  'DEBUG [tracing] Span completed trace_id=abc123 span_id=def456 duration=12ms',
  'INFO  [k8s] Pod scheduled pod=api-7f8d9c-x2k4m node=worker-02 cpu=500m memory=512Mi',
  'ERROR [queue] Message processing failed queue=notifications message_id=msg_x2k9 retry=3/5',
  'INFO  [backup] Snapshot completed snapshot_id=snap_20240205 size=2.4GB duration=45s',
  'DEBUG [feature-flag] Flag evaluated flag=new_dashboard user_id=usr_123 result=true',
  'WARN  [api] Deprecated endpoint called endpoint=/v1/legacy/users calls_remaining=1000',
]

const hosts = ['api-gateway-01', 'worker-node-02', 'db-primary', 'cache-redis-01', 'storage-03', 'scheduler-01']
const services = ['http-server', 'db-pool', 'auth', 'payment', 'scheduler', 'cache', 'websocket', 'storage', 'metrics', 'deploy']
const levels = ['DEBUG', 'INFO', 'WARN', 'ERROR']
const envs = ['production', 'staging', 'development']

function randomFrom<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]
}

function generateRecord(index: number): Record {
  const now = new Date()
  const offset = Math.floor(Math.random() * 3600000) // Random offset up to 1 hour
  const ts = new Date(now.getTime() - offset)

  return {
    ingestTs: ts,
    writeTs: new Date(ts.getTime() + Math.floor(Math.random() * 100)),
    attrs: {
      host: randomFrom(hosts),
      service: randomFrom(services),
      level: randomFrom(levels),
      env: randomFrom(envs),
    },
    raw: randomFrom(logMessages),
    ref: {
      chunkId: `chunk_${Math.floor(index / 100)}`,
      pos: index % 100,
      storeId: 'default',
    },
  }
}

export function generateMockRecords(count: number): Record[] {
  return Array.from({ length: count }, (_, i) => generateRecord(i))
}

export const mockStores: StoreInfo[] = [
  { id: 'default', type: 'file', route: '*', chunkCount: 12, recordCount: 145823 },
  { id: 'metrics', type: 'file', route: 'service=metrics', chunkCount: 5, recordCount: 52341 },
  { id: 'audit', type: 'file', route: 'service=audit', chunkCount: 3, recordCount: 8921 },
]

export const mockChunks: ChunkMeta[] = [
  { id: 'chunk_0', startTs: new Date('2024-02-05T10:00:00Z'), endTs: new Date('2024-02-05T11:00:00Z'), sealed: true, recordCount: 12500 },
  { id: 'chunk_1', startTs: new Date('2024-02-05T11:00:00Z'), endTs: new Date('2024-02-05T12:00:00Z'), sealed: true, recordCount: 11800 },
  { id: 'chunk_2', startTs: new Date('2024-02-05T12:00:00Z'), endTs: new Date('2024-02-05T13:00:00Z'), sealed: true, recordCount: 13200 },
  { id: 'chunk_3', startTs: new Date('2024-02-05T13:00:00Z'), endTs: new Date(), sealed: false, recordCount: 4523 },
]

export const mockStats: Stats = {
  totalStores: 3,
  totalChunks: 20,
  sealedChunks: 17,
  totalRecords: 207085,
  totalBytes: 52428800,
  oldestRecord: new Date('2024-02-01T00:00:00Z'),
  newestRecord: new Date(),
}

export const mockExplainPlan: ChunkPlan[] = [
  {
    chunkId: 'chunk_0',
    sealed: true,
    recordCount: 12500,
    steps: [
      { name: 'time', inputEstimate: 12500, outputEstimate: 10000, action: 'seek', reason: 'binary_search', detail: 'skip 2500 via idx.log' },
      { name: 'token', inputEstimate: 10000, outputEstimate: 2500, action: 'indexed', reason: 'indexed', detail: '1 token(s) intersected' },
      { name: 'kv', inputEstimate: 2500, outputEstimate: 800, action: 'indexed', reason: 'indexed', detail: 'attr_kv=800 msg_kv=0' },
    ],
    scanMode: 'index-driven',
    estimatedRecords: 800,
    runtimeFilters: ['time bounds'],
    storeId: 'default',
  },
  {
    chunkId: 'chunk_1',
    sealed: true,
    recordCount: 11800,
    steps: [
      { name: 'time', inputEstimate: 11800, outputEstimate: 11800, action: 'filter', reason: 'full_scan', detail: 'chunk within time range' },
      { name: 'token', inputEstimate: 11800, outputEstimate: 3200, action: 'indexed', reason: 'indexed', detail: '1 token(s) intersected' },
    ],
    scanMode: 'index-driven',
    estimatedRecords: 3200,
    runtimeFilters: [],
    storeId: 'default',
  },
]

// Highlight matching tokens in log text
export function highlightMatches(text: string, tokens: string[]): { text: string; highlighted: boolean }[] {
  if (!tokens.length) return [{ text, highlighted: false }]

  const regex = new RegExp(`(${tokens.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`, 'gi')
  const parts: { text: string; highlighted: boolean }[] = []
  let lastIndex = 0
  let match: RegExpExecArray | null

  while ((match = regex.exec(text)) !== null) {
    if (match.index > lastIndex) {
      parts.push({ text: text.slice(lastIndex, match.index), highlighted: false })
    }
    parts.push({ text: match[0], highlighted: true })
    lastIndex = regex.lastIndex
  }

  if (lastIndex < text.length) {
    parts.push({ text: text.slice(lastIndex), highlighted: false })
  }

  return parts.length ? parts : [{ text, highlighted: false }]
}
