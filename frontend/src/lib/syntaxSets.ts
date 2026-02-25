/** Keyword sets used by autocomplete and syntax classification. */
export interface SyntaxSets {
  directives: Set<string>;
  pipeKeywords: Set<string>;
  pipeFunctions: Set<string>;
  lookupTables: Set<string>;
}

/** Default sets — used when the backend hasn't been queried yet (e.g. tests). */
export const DEFAULT_SYNTAX: SyntaxSets = {
  directives: new Set([
    "reverse", "start", "end", "last", "limit", "pos",
    "source_start", "source_end", "ingest_start", "ingest_end",
  ]),
  pipeKeywords: new Set([
    "stats", "where", "eval", "sort", "head", "tail", "slice",
    "rename", "fields", "timechart", "raw", "lookup",
    "barchart", "donut", "map",
  ]),
  pipeFunctions: new Set([
    "count", "avg", "sum", "min", "max", "bin", "tonumber",
    "tostring", "abs", "ceil", "floor", "round", "sqrt", "pow",
    "log", "log10", "log2", "exp",
    "len", "lower", "upper", "substr", "replace", "trim", "concat",
    "coalesce", "isnull", "typeof",
    "bitor", "bitand", "bitxor", "bitnot", "bitshl", "bitshr",
  ]),
  lookupTables: new Set(["rdns", "geoip"]),
};
