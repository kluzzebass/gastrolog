// API types matching the proto definitions

export interface Query {
  start?: Date
  end?: Date
  tokens?: string[]
  kvPredicates?: KVPredicate[]
  limit?: number
  contextBefore?: number
  contextAfter?: number
}

export interface KVPredicate {
  key: string
  value: string
}

export interface Record {
  ingestTs: Date
  writeTs: Date
  attrs: Record<string, string>
  raw: string
  ref: RecordRef
}

export interface RecordRef {
  chunkId: string
  pos: number
  storeId: string
}

export interface SearchResponse {
  records: Record[]
  resumeToken?: string
  hasMore: boolean
}

export interface StoreInfo {
  id: string
  type: string
  route: string
  chunkCount: number
  recordCount: number
}

export interface ChunkMeta {
  id: string
  startTs: Date
  endTs: Date
  sealed: boolean
  recordCount: number
}

export interface ChunkPlan {
  chunkId: string
  sealed: boolean
  recordCount: number
  steps: PipelineStep[]
  scanMode: string
  estimatedRecords: number
  runtimeFilters: string[]
  storeId: string
}

export interface PipelineStep {
  name: string
  inputEstimate: number
  outputEstimate: number
  action: string
  reason: string
  detail: string
}

export interface Stats {
  totalStores: number
  totalChunks: number
  sealedChunks: number
  totalRecords: number
  totalBytes: number
  oldestRecord?: Date
  newestRecord?: Date
}
