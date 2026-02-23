// Query input tokenizer for syntax highlighting.
// Lightweight TypeScript port of backend/internal/querylang/lexer.go.
// Produces position-annotated tokens including whitespace so that
// concatenating all token texts reproduces the original input exactly.

type QueryTokenKind =
  | "word"
  | "quoted"
  | "operator" // AND, OR, NOT
  | "lparen"
  | "rparen"
  | "eq"
  | "ne" // !=
  | "gt" // >
  | "gte" // >=
  | "lt" // <
  | "lte" // <=
  | "star"
  | "glob" // bareword with glob metacharacters (*, ?, [)
  | "regex" // /pattern/
  | "pipe" // |
  | "comma" // ,
  | "whitespace"
  | "error"; // unterminated quote or regex

// After the post-pass, words in key=value positions get reclassified.
export type HighlightRole =
  | "operator" // AND, OR, NOT
  | "directive-key" // key portion of a control arg (last, reverse, ...)
  | "key" // key in key=value predicate
  | "eq" // = sign
  | "compare-op" // comparison operator (!=, >, >=, <, <=)
  | "value" // value in key=value predicate
  | "token" // bare search term
  | "quoted" // quoted string (standalone or as value)
  | "glob" // glob pattern (error*, *timeout, etc.)
  | "regex" // /pattern/ regex literal
  | "paren" // ( or )
  | "star" // *
  | "pipe" // |
  | "pipe-keyword" // stats, where
  | "function" // count, avg, sum, min, max, bin, toNumber
  | "comma" // ,
  | "whitespace"
  | "error";

export interface QueryToken {
  text: string;
  pos: number;
  kind: QueryTokenKind;
}

export interface HighlightSpan {
  text: string;
  role: HighlightRole;
}

// Keyword sets used by the classifier and validator.
// Exported as an interface so the frontend can supply server-provided sets.
export interface SyntaxSets {
  directives: Set<string>;
  pipeKeywords: Set<string>;
  pipeFunctions: Set<string>;
}

// Scalar functions that can appear in filter expression predicates.
// These are the same as pipeFunctions minus aggregation-only functions
// (count, avg, sum, min, max, bin, dcount, median, first, last, values).
const SCALAR_FUNCTIONS = new Set([
  "tonumber", "tostring",
  "abs", "ceil", "floor", "round", "sqrt", "pow",
  "log", "log10", "log2", "exp",
  "len", "lower", "upper", "substr", "replace", "trim", "concat",
  "coalesce", "isnull", "typeof",
  "bitor", "bitand", "bitxor", "bitnot", "bitshl", "bitshr",
]);

// Default sets — used when the backend hasn't been queried yet (e.g. tests).
export const DEFAULT_SYNTAX: SyntaxSets = {
  directives: new Set([
    "reverse", "start", "end", "last", "limit", "pos",
    "source_start", "source_end", "ingest_start", "ingest_end",
  ]),
  pipeKeywords: new Set(["stats", "where", "eval", "sort", "head", "tail", "slice", "rename", "fields", "timechart", "raw"]),
  pipeFunctions: new Set([
    "count", "avg", "sum", "min", "max", "bin", "tonumber",
    // Scalar functions.
    "tostring", "abs", "ceil", "floor", "round", "sqrt", "pow",
    "log", "log10", "log2", "exp",
    "len", "lower", "upper", "substr", "replace", "trim", "concat",
    "coalesce", "isnull", "typeof",
    "bitor", "bitand", "bitxor", "bitnot", "bitshl", "bitshr",
    // Aggregation functions.
    "dcount", "median", "first", "last", "values",
  ]),
};

/** @deprecated Use DEFAULT_SYNTAX.directives instead. */
export const DIRECTIVES = DEFAULT_SYNTAX.directives;

// Phase 1: Raw lexing — character scan producing tokens with whitespace preserved.
export function lex(input: string): QueryToken[] {
  const tokens: QueryToken[] = [];
  let pos = 0;

  while (pos < input.length) {
    const ch = input[pos];

    // Whitespace
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      const start = pos;
      while (
        pos < input.length &&
        (input[pos] === " " ||
          input[pos] === "\t" ||
          input[pos] === "\n" ||
          input[pos] === "\r")
      ) {
        pos++;
      }
      tokens.push({
        text: input.slice(start, pos),
        pos: start,
        kind: "whitespace",
      });
      continue;
    }

    // Pipe
    if (ch === "|") {
      tokens.push({ text: "|", pos, kind: "pipe" });
      pos++;
      continue;
    }

    // Comma
    if (ch === ",") {
      tokens.push({ text: ",", pos, kind: "comma" });
      pos++;
      continue;
    }

    // Single-character tokens
    if (ch === "(") {
      tokens.push({ text: "(", pos, kind: "lparen" });
      pos++;
      continue;
    }
    if (ch === ")") {
      tokens.push({ text: ")", pos, kind: "rparen" });
      pos++;
      continue;
    }
    if (ch === "=") {
      tokens.push({ text: "=", pos, kind: "eq" });
      pos++;
      continue;
    }
    if (ch === "!") {
      if (pos + 1 < input.length && input[pos + 1] === "=") {
        tokens.push({ text: "!=", pos, kind: "ne" });
        pos += 2;
      } else {
        tokens.push({ text: "!", pos, kind: "error" });
        pos++;
      }
      continue;
    }
    if (ch === ">") {
      if (pos + 1 < input.length && input[pos + 1] === "=") {
        tokens.push({ text: ">=", pos, kind: "gte" });
        pos += 2;
      } else {
        tokens.push({ text: ">", pos, kind: "gt" });
        pos++;
      }
      continue;
    }
    if (ch === "<") {
      if (pos + 1 < input.length && input[pos + 1] === "=") {
        tokens.push({ text: "<=", pos, kind: "lte" });
        pos += 2;
      } else {
        tokens.push({ text: "<", pos, kind: "lt" });
        pos++;
      }
      continue;
    }
    if (ch === "*") {
      // Peek ahead: if followed by a bareword or glob char, this is a glob (e.g. *error)
      if (pos + 1 < input.length && isGlobBarewordChar(input[pos + 1]!)) {
        const start = pos;
        pos++; // skip leading *
        while (pos < input.length && (isBarewordChar(input[pos]!) || isGlobMeta(input[pos]!))) {
          if (input[pos] === "[") {
            pos++;
            while (pos < input.length && input[pos] !== "]") pos++;
            if (pos < input.length) pos++; // skip ]
          } else {
            pos++;
          }
        }
        tokens.push({ text: input.slice(start, pos), pos: start, kind: "glob" });
        continue;
      }
      tokens.push({ text: "*", pos, kind: "star" });
      pos++;
      continue;
    }

    // Quoted string
    if (ch === '"' || ch === "'") {
      const start = pos;
      const quote = ch;
      pos++; // skip opening quote
      while (pos < input.length) {
        if (input[pos] === "\\") {
          pos += 2; // skip escape
          continue;
        }
        if (input[pos] === quote) {
          pos++; // skip closing quote
          tokens.push({
            text: input.slice(start, pos),
            pos: start,
            kind: "quoted",
          });
          break;
        }
        pos++;
      }
      // Unterminated quote
      if (
        pos > input.length ||
        tokens.length === 0 ||
        tokens[tokens.length - 1]!.pos !== start
      ) {
        tokens.push({
          text: input.slice(start, pos),
          pos: start,
          kind: "error",
        });
      }
      continue;
    }

    // Regex literal: /pattern/ (only in filter context, not after pipe)
    if (ch === "/") {
      // In pipe context (after |), / is division — don't lex as regex.
      // Check if we've seen a pipe token.
      const afterPipe = tokens.some((t) => t.kind === "pipe");
      if (!afterPipe) {
        const start = pos;
        pos++; // skip opening /
        while (pos < input.length) {
          if (input[pos] === "\\" && pos + 1 < input.length && input[pos + 1] === "/") {
            pos += 2; // skip escaped slash
            continue;
          }
          if (input[pos] === "/") {
            pos++; // skip closing /
            tokens.push({
              text: input.slice(start, pos),
              pos: start,
              kind: "regex",
            });
            break;
          }
          pos++;
        }
        // Unterminated regex
        if (
          tokens.length === 0 ||
          tokens[tokens.length - 1]!.pos !== start
        ) {
          tokens.push({
            text: input.slice(start, pos),
            pos: start,
            kind: "error",
          });
        }
        continue;
      }
      // In pipe context: treat / as a word (division operator highlighting
      // is handled at the classifier level via "compare-op" or "token").
      tokens.push({ text: "/", pos, kind: "word" });
      pos++;
      continue;
    }

    // Bareword (may contain glob metacharacters *, ?, [)
    const start = pos;
    let hasGlobMeta = false;
    while (pos < input.length && (isBarewordChar(input[pos]!) || isGlobMeta(input[pos]!))) {
      const c = input[pos]!;
      if (isGlobMeta(c)) {
        hasGlobMeta = true;
        if (c === "[") {
          pos++;
          while (pos < input.length && input[pos] !== "]") pos++;
          if (pos < input.length) pos++; // skip ]
        } else {
          pos++;
        }
      } else {
        pos++;
      }
    }
    const lit = input.slice(start, pos);
    if (hasGlobMeta) {
      tokens.push({ text: lit, pos: start, kind: "glob" });
    } else {
      const upper = lit.toUpperCase();
      const kind: QueryTokenKind =
        upper === "AND" || upper === "OR" || upper === "NOT"
          ? "operator"
          : "word";
      tokens.push({ text: lit, pos: start, kind });
    }
  }

  return tokens;
}

function isBarewordChar(ch: string): boolean {
  return (
    ch !== " " &&
    ch !== "\t" &&
    ch !== "\n" &&
    ch !== "\r" &&
    ch !== "(" &&
    ch !== ")" &&
    ch !== "=" &&
    ch !== "*" &&
    ch !== "?" &&
    ch !== "[" &&
    ch !== '"' &&
    ch !== "'" &&
    ch !== "/" &&
    ch !== ">" &&
    ch !== "<" &&
    ch !== "!" &&
    ch !== "|" &&
    ch !== ","
  );
}

function isGlobMeta(ch: string): boolean {
  return ch === "*" || ch === "?" || ch === "[";
}

function isGlobBarewordChar(ch: string): boolean {
  return isBarewordChar(ch) || ch === "?" || ch === "[";
}

function isCompareOpKind(kind: QueryTokenKind): boolean {
  return kind === "eq" || kind === "ne" || kind === "gt" || kind === "gte" || kind === "lt" || kind === "lte";
}

function isValueKind(kind: QueryTokenKind): boolean {
  return kind === "word" || kind === "glob" || kind === "star" || kind === "quoted";
}

// Phase 2: Post-pass — classify tokens into highlight roles.
// Split on pipes first, then classify filter and pipe segments separately.
function classify(raw: QueryToken[], syntax: SyntaxSets): HighlightSpan[] {
  // Find pipe token indices to split into segments.
  const pipeIndices: number[] = [];
  for (let i = 0; i < raw.length; i++) {
    if (raw[i]!.kind === "pipe") pipeIndices.push(i);
  }

  if (pipeIndices.length === 0) {
    // No pipes — classify entire input as a filter expression.
    return classifyFilter(raw, syntax);
  }

  const spans: HighlightSpan[] = [];

  // Filter segment: everything before the first pipe.
  const filterTokens = raw.slice(0, pipeIndices[0]!);
  spans.push(...classifyFilter(filterTokens, syntax));

  // Pipe segments.
  for (let p = 0; p < pipeIndices.length; p++) {
    const pipeIdx = pipeIndices[p]!;
    spans.push({ text: "|", role: "pipe" });

    const nextPipe = p + 1 < pipeIndices.length ? pipeIndices[p + 1]! : raw.length;
    const segTokens = raw.slice(pipeIdx + 1, nextPipe);
    spans.push(...classifyPipeSegment(segTokens, syntax));
  }

  return spans;
}

// Classify a filter expression segment (the part before | or a where clause).
// Detects key=value triples, scalar function predicates, and maps remaining tokens to roles.
function classifyFilter(raw: QueryToken[], syntax: SyntaxSets): HighlightSpan[] {
  const spans: HighlightSpan[] = [];

  let i = 0;
  while (i < raw.length) {
    const tok = raw[i]!;

    // Try to detect scalar function calls in filter context: func(args...) <op> value.
    // When a word matches a known scalar function and is followed by '(', classify
    // the function call with pipe-style highlighting.
    if (tok.kind === "word" && SCALAR_FUNCTIONS.has(tok.text.toLowerCase())) {
      // Look ahead for '(' (skip whitespace).
      let j = i + 1;
      while (j < raw.length && raw[j]!.kind === "whitespace") j++;
      if (j < raw.length && raw[j]!.kind === "lparen") {
        // Find the matching ')' by counting paren depth.
        let depth = 0;
        let closeIdx = -1;
        for (let k = j; k < raw.length; k++) {
          if (raw[k]!.kind === "lparen") depth++;
          else if (raw[k]!.kind === "rparen") {
            depth--;
            if (depth === 0) { closeIdx = k; break; }
          }
        }
        if (closeIdx >= 0) {
          // Classify the function call tokens using stats-style highlighting.
          spans.push({ text: tok.text, role: "function" });
          // Whitespace between function name and '('.
          for (let w = i + 1; w < j; w++) {
            spans.push({ text: raw[w]!.text, role: "whitespace" });
          }
          // Arguments from '(' to ')' inclusive.
          const argTokens = raw.slice(j, closeIdx + 1);
          spans.push(...classifyStatsArgs(argTokens, syntax));
          i = closeIdx + 1;
          continue;
        }
      }
    }

    // Try to detect key <op> value patterns (whitespace-tolerant lookahead).
    if (tok.kind === "word" || tok.kind === "star" || tok.kind === "glob") {
      // Skip whitespace to find operator.
      let j = i + 1;
      while (j < raw.length && raw[j]!.kind === "whitespace") j++;
      const opTok = j < raw.length ? raw[j]! : null;

      if (opTok && isCompareOpKind(opTok.kind)) {
        // Skip whitespace to find value.
        let k = j + 1;
        while (k < raw.length && raw[k]!.kind === "whitespace") k++;
        const valTok = k < raw.length ? raw[k]! : null;

        if (valTok && isValueKind(valTok.kind)) {
          const isEq = opTok.kind === "eq";
          const isDirective = isEq && tok.kind === "word" && syntax.directives.has(tok.text.toLowerCase());
          const opRole: HighlightRole = isEq ? "eq" : "compare-op";

          // Emit all tokens from i through k (including intervening whitespace).
          if (isDirective) {
            spans.push({ text: tok.text, role: "directive-key" });
          } else {
            spans.push({
              text: tok.text,
              role: tok.kind === "star" ? "star" : tok.kind === "glob" ? "glob" : "key",
            });
          }
          // Whitespace between key and op.
          for (let w = i + 1; w < j; w++) {
            spans.push({ text: raw[w]!.text, role: "whitespace" });
          }
          spans.push({ text: opTok.text, role: opRole });
          // Whitespace between op and value.
          for (let w = j + 1; w < k; w++) {
            spans.push({ text: raw[w]!.text, role: "whitespace" });
          }
          spans.push({
            text: valTok.text,
            role: valTok.kind === "quoted" ? "quoted"
              : valTok.kind === "star" ? "star"
              : valTok.kind === "glob" ? "glob"
              : "value",
          });
          i = k + 1;
          continue;
        }
      }
    }

    // Map remaining tokens to roles.
    spans.push(mapTokenToRole(tok));
    i++;
  }

  return spans;
}

// Classify a pipe segment (everything after | up to the next | or end).
// Recognizes pipe keywords (stats, where), functions, "by", "as", commas.
function classifyPipeSegment(tokens: QueryToken[], syntax: SyntaxSets): HighlightSpan[] {
  const spans: HighlightSpan[] = [];

  // Find the keyword (first non-whitespace word).
  let keywordIdx = -1;
  let keyword = "";
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i]!.kind === "whitespace") continue;
    if (tokens[i]!.kind === "word") {
      keyword = tokens[i]!.text.toLowerCase();
      keywordIdx = i;
    }
    break;
  }

  if (syntax.pipeKeywords.has(keyword)) {
    const isFilterKeyword = keyword === "where";

    for (let i = 0; i < tokens.length; i++) {
      if (i < keywordIdx) {
        spans.push({ text: tokens[i]!.text, role: "whitespace" });
      } else if (i === keywordIdx) {
        spans.push({ text: tokens[i]!.text, role: "pipe-keyword" });
      } else {
        if (isFilterKeyword) {
          // Everything after "where" is a filter expression.
          spans.push(...classifyFilter(tokens.slice(i), syntax));
        } else if (keyword === "eval") {
          // eval: field = expr, field = expr, ...
          spans.push(...classifyEvalArgs(tokens.slice(i), syntax));
        } else if (keyword === "rename") {
          // rename: field as field, field as field, ...
          spans.push(...classifyRenameArgs(tokens.slice(i)));
        } else if (keyword === "sort" || keyword === "head" || keyword === "tail" || keyword === "slice" || keyword === "timechart" || keyword === "fields") {
          // sort/head/fields: tokens with - as punctuation
          spans.push(...classifySimplePipeArgs(tokens.slice(i)));
        } else {
          // Everything after "stats" (or similar) is stats arguments.
          spans.push(...classifyStatsArgs(tokens.slice(i), syntax));
        }
        break;
      }
    }
    return spans;
  }

  // Unknown keyword — emit leading whitespace, then keyword as error, rest as tokens.
  if (keywordIdx >= 0) {
    for (let i = 0; i < keywordIdx; i++) {
      spans.push({ text: tokens[i]!.text, role: "whitespace" });
    }
    // Unknown pipe keyword — still classify remaining tokens generically.
    for (let i = keywordIdx; i < tokens.length; i++) {
      spans.push(mapTokenToRole(tokens[i]!));
    }
  } else {
    for (const tok of tokens) {
      spans.push(mapTokenToRole(tok));
    }
  }

  return spans;
}

// Classify eval arguments: field = expr, field = expr, ...
// Fields before = are "token", = is "eq", expressions use classifyStatsArgs.
function classifyEvalArgs(tokens: QueryToken[], syntax: SyntaxSets): HighlightSpan[] {
  const spans: HighlightSpan[] = [];

  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i]!;

    if (tok.kind === "whitespace") {
      spans.push({ text: tok.text, role: "whitespace" });
      continue;
    }

    if (tok.kind === "comma") {
      spans.push({ text: tok.text, role: "comma" });
      continue;
    }

    if (tok.kind === "eq") {
      spans.push({ text: tok.text, role: "eq" });
      continue;
    }

    // After = sign, everything until next comma or end is an expression.
    // Delegate to classifyStatsArgs for expression highlighting.
    if (tok.kind === "word" || tok.kind === "quoted") {
      // Check if this might be a field name followed by =.
      let j = i + 1;
      while (j < tokens.length && tokens[j]!.kind === "whitespace") j++;
      if (j < tokens.length && tokens[j]!.kind === "eq") {
        // Field name before =.
        spans.push({ text: tok.text, role: "token" });
        continue;
      }
    }

    // Expression part — use stats args classifier for function calls etc.
    spans.push(...classifyStatsArgs(tokens.slice(i, i + 1), syntax));
  }

  return spans;
}

// Classify rename arguments: field as field, field as field, ...
function classifyRenameArgs(tokens: QueryToken[]): HighlightSpan[] {
  const spans: HighlightSpan[] = [];

  for (const tok of tokens) {
    if (tok.kind === "whitespace") {
      spans.push({ text: tok.text, role: "whitespace" });
    } else if (tok.kind === "comma") {
      spans.push({ text: tok.text, role: "comma" });
    } else if (tok.kind === "word" && tok.text.toLowerCase() === "as") {
      spans.push({ text: tok.text, role: "pipe-keyword" });
    } else if (tok.kind === "word") {
      spans.push({ text: tok.text, role: "token" });
    } else {
      spans.push(mapTokenToRole(tok));
    }
  }

  return spans;
}

// Classify simple pipe arguments (sort, head, fields, timechart): tokens with
// - as punctuation and "by"/"as" as keywords.
function classifySimplePipeArgs(tokens: QueryToken[]): HighlightSpan[] {
  const spans: HighlightSpan[] = [];

  for (const tok of tokens) {
    if (tok.kind === "whitespace") {
      spans.push({ text: tok.text, role: "whitespace" });
    } else if (tok.kind === "comma") {
      spans.push({ text: tok.text, role: "comma" });
    } else if (tok.kind === "word" && tok.text === "-") {
      spans.push({ text: tok.text, role: "compare-op" });
    } else if (tok.kind === "word" && (tok.text.toLowerCase() === "by" || tok.text.toLowerCase() === "as")) {
      spans.push({ text: tok.text, role: "pipe-keyword" });
    } else if (tok.kind === "word") {
      spans.push({ text: tok.text, role: "token" });
    } else {
      spans.push(mapTokenToRole(tok));
    }
  }

  return spans;
}

// Classify the arguments portion of a stats segment.
// Recognizes: function calls, "by" keyword, "as" keyword, commas, field refs.
function classifyStatsArgs(tokens: QueryToken[], syntax: SyntaxSets): HighlightSpan[] {
  const spans: HighlightSpan[] = [];

  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i]!;

    if (tok.kind === "whitespace") {
      spans.push({ text: tok.text, role: "whitespace" });
      continue;
    }

    if (tok.kind === "comma") {
      spans.push({ text: tok.text, role: "comma" });
      continue;
    }

    if (tok.kind === "lparen") {
      spans.push({ text: tok.text, role: "paren" });
      continue;
    }

    if (tok.kind === "rparen") {
      spans.push({ text: tok.text, role: "paren" });
      continue;
    }

    if (tok.kind === "quoted") {
      spans.push({ text: tok.text, role: "quoted" });
      continue;
    }

    if (tok.kind === "word") {
      const lower = tok.text.toLowerCase();

      // "by" keyword separating aggregations from group-by.
      if (lower === "by") {
        spans.push({ text: tok.text, role: "pipe-keyword" });
        continue;
      }

      // "as" keyword for aliases.
      if (lower === "as") {
        spans.push({ text: tok.text, role: "pipe-keyword" });
        continue;
      }

      // Known function: check if followed by "(" (whitespace-tolerant).
      if (syntax.pipeFunctions.has(lower)) {
        // "count" can appear without parens as bare aggregate.
        let j = i + 1;
        while (j < tokens.length && tokens[j]!.kind === "whitespace") j++;
        if (lower === "count" && (j >= tokens.length || tokens[j]!.kind !== "lparen")) {
          // Bare "count" — still a function.
          spans.push({ text: tok.text, role: "function" });
          continue;
        }
        if (j < tokens.length && tokens[j]!.kind === "lparen") {
          spans.push({ text: tok.text, role: "function" });
          continue;
        }
      }

      // Arithmetic operators in pipe context: +, -, *, /
      // These are lexed as separate word tokens; classify as compare-op for styling.

      // Otherwise: field reference or value — classify as token.
      spans.push({ text: tok.text, role: "token" });
      continue;
    }

    // Arithmetic: *, /, etc. — in pipe context these are operators.
    if (tok.kind === "star") {
      spans.push({ text: tok.text, role: "star" });
      continue;
    }

    // Comparison ops that appear in pipe expressions.
    if (isCompareOpKind(tok.kind)) {
      spans.push({ text: tok.text, role: "compare-op" });
      continue;
    }

    // Fallback.
    spans.push(mapTokenToRole(tok));
  }

  return spans;
}

// Map a single token to its default highlight role.
function mapTokenToRole(tok: QueryToken): HighlightSpan {
  switch (tok.kind) {
    case "operator":
      return { text: tok.text, role: "operator" };
    case "quoted":
      return { text: tok.text, role: "quoted" };
    case "lparen":
    case "rparen":
      return { text: tok.text, role: "paren" };
    case "eq":
      return { text: tok.text, role: "eq" };
    case "ne":
    case "gt":
    case "gte":
    case "lt":
    case "lte":
      return { text: tok.text, role: "compare-op" };
    case "star":
      return { text: tok.text, role: "star" };
    case "glob":
      return { text: tok.text, role: "glob" };
    case "regex":
      return { text: tok.text, role: "regex" };
    case "pipe":
      return { text: tok.text, role: "pipe" };
    case "comma":
      return { text: tok.text, role: "comma" };
    case "whitespace":
      return { text: tok.text, role: "whitespace" };
    case "error":
      return { text: tok.text, role: "error" };
    default:
      return { text: tok.text, role: "token" };
  }
}

// Phase 3: Validate via recursive descent parser mirroring backend grammar.
// The source of truth is backend/internal/querylang/parser.go — changes
// there must be reflected here.
// Finds the first span index where parsing fails; marks it and everything
// after it as "error".
//
// Grammar (matches backend/internal/querylang/{parser,pipeline_parser}.go):
//   pipeline    = filter_expr ( "|" pipe_op )*
//   filter_expr = or_expr
//   or_expr     = and_expr ( "OR" and_expr )*
//   and_expr    = unary_expr ( [ "AND" ] unary_expr )*
//   unary_expr  = "NOT" unary_expr | primary
//   primary     = "(" or_expr ")" | predicate
//   predicate   = kv_triple | regex | token | quoted | star_pred
//   kv_triple   = (key|directive-key) (eq|compare-op) (value|quoted|star)
//   pipe_op     = stats_op | where_op
//   stats_op    = "stats" agg_list ( "by" group_list )?
//   agg_list    = agg_expr ( "," agg_expr )*
//   agg_expr    = "count" ( "as" IDENT )? | IDENT "(" pipe_expr ")" ( "as" IDENT )?
//   group_list  = group_expr ( "," group_expr )*
//   group_expr  = "bin" "(" ... ")" | IDENT
//   where_op    = "where" filter_expr
interface ValidateResult {
  spans: HighlightSpan[];
  errorMessage: string | null;
}

function validate(spans: HighlightSpan[]): ValidateResult {
  // Filter to non-whitespace span indices for parsing.
  const indices: number[] = [];
  for (let i = 0; i < spans.length; i++) {
    if (spans[i]!.role !== "whitespace") indices.push(i);
  }

  // Already-errored spans (unterminated quotes) — skip validation if present.
  if (spans.some((s) => s.role === "error"))
    return { spans, errorMessage: "unterminated string" };
  // Empty query — nothing to validate.
  if (indices.length === 0) return { spans, errorMessage: null };

  let pos = 0; // cursor into indices[]
  let errorAt = -1; // first span index where parse fails
  let errorMessage: string | null = null;

  function cur(): HighlightSpan | null {
    return pos < indices.length ? spans[indices[pos]!]! : null;
  }
  function _curIdx(): number {
    return pos < indices.length ? indices[pos]! : -1;
  }
  function advance() {
    pos++;
  }
  function fail(msg: string) {
    if (errorAt < 0) {
      errorAt =
        pos < indices.length ? indices[pos]! : indices[indices.length - 1]!;
      errorMessage = msg;
    }
  }

  // Is the current span a predicate start? (token, quoted, key, directive-key, star, glob, function, or paren)
  function isPredStart(): boolean {
    const s = cur();
    if (!s) return false;
    return (
      s.role === "token" ||
      s.role === "quoted" ||
      s.role === "regex" ||
      s.role === "glob" ||
      s.role === "key" ||
      s.role === "directive-key" ||
      s.role === "star" ||
      s.role === "function" ||
      (s.role === "paren" && s.text === "(")
    );
  }

  function parseOrExpr(): boolean {
    if (!parseAndExpr()) return false;
    while (cur()?.role === "operator" && cur()!.text.toUpperCase() === "OR") {
      advance(); // consume OR
      if (!parseAndExpr()) return false;
    }
    return true;
  }

  function parseAndExpr(): boolean {
    if (!parseUnaryExpr()) return false;
    while (true) {
      const s = cur();
      if (!s) break;
      if (s.role === "operator" && s.text.toUpperCase() === "AND") {
        advance(); // consume AND
        if (!parseUnaryExpr()) return false;
      } else if (
        (s.role === "operator" && s.text.toUpperCase() === "NOT") ||
        isPredStart()
      ) {
        // Implicit AND
        if (!parseUnaryExpr()) return false;
      } else {
        break;
      }
    }
    return true;
  }

  function parseUnaryExpr(): boolean {
    const s = cur();
    if (s?.role === "operator" && s.text.toUpperCase() === "NOT") {
      advance(); // consume NOT
      const next = cur();
      if (!next) {
        fail("expected expression after NOT");
        return false;
      }
      if (next.role === "operator" && next.text.toUpperCase() !== "NOT") {
        fail(`expected expression after NOT, got ${next.text.toUpperCase()}`);
        return false;
      }
      if (next.role === "paren" && next.text === ")") {
        fail("expected expression after NOT, got )");
        return false;
      }
      return parseUnaryExpr();
    }
    return parsePrimary();
  }

  function parsePrimary(): boolean {
    const s = cur();
    if (!s) {
      fail("unexpected end of expression");
      return false;
    }

    if (s.role === "paren" && s.text === "(") {
      advance(); // consume (
      if (cur()?.role === "paren" && cur()!.text === ")") {
        fail("empty parentheses");
        return false;
      }
      if (!parseOrExpr()) return false;
      if (cur()?.role !== "paren" || cur()!.text !== ")") {
        fail("unmatched opening parenthesis");
        return false;
      }
      advance(); // consume )
      return true;
    }

    return parsePredicate();
  }

  function parsePredicate(): boolean {
    const s = cur();
    if (!s) {
      fail("unexpected end of expression");
      return false;
    }

    // Expression predicate: function(args...) <op> value
    if (s.role === "function") {
      // Parse the pipe expression (function call + optional arithmetic).
      parsePipeExpr();
      if (errorAt >= 0) return false;
      // Expect comparison operator.
      const op = cur();
      if (op && (op.role === "eq" || op.role === "compare-op")) {
        advance(); // consume operator
        const v = cur();
        if (
          !v ||
          (v.role !== "value" &&
            v.role !== "token" &&
            v.role !== "quoted" &&
            v.role !== "star" &&
            v.role !== "glob")
        ) {
          fail(`expected value after '${op.text}'`);
          return false;
        }
        advance(); // consume value
        return true;
      }
      // No comparison operator — this is valid (function was consumed as pipe expr).
      // But in filter context a standalone function call isn't valid; validation
      // continues and the next token determines the result.
      return true;
    }

    if (s.role === "key" || s.role === "directive-key") {
      advance(); // consume key
      const op = cur();
      if (!op || (op.role !== "eq" && op.role !== "compare-op")) {
        fail("expected operator after key");
        return false;
      }
      advance(); // consume eq or compare-op
      const v = cur();
      if (
        !v ||
        (v.role !== "value" &&
          v.role !== "quoted" &&
          v.role !== "star" &&
          v.role !== "glob")
      ) {
        fail(`expected value after '${op.text}'`);
        return false;
      }
      advance(); // consume value
      return true;
    }

    if (s.role === "star") {
      const saved = pos;
      advance(); // consume *
      const op = cur();
      if (op && (op.role === "eq" || op.role === "compare-op")) {
        advance(); // consume operator
        const v = cur();
        if (
          !v ||
          (v.role !== "value" &&
            v.role !== "quoted" &&
            v.role !== "star" &&
            v.role !== "glob")
        ) {
          fail(`expected value after '*${op.text}'`);
          return false;
        }
        advance(); // consume value
        return true;
      }
      pos = saved;
      fail("'*' must be followed by '='");
      return false;
    }

    // Glob can be standalone (like a token) or as KV key (already classified)
    if (s.role === "glob") {
      advance(); // consume glob
      // Check if it's a KV: glob op ...
      const op = cur();
      if (op && (op.role === "eq" || op.role === "compare-op")) {
        advance(); // consume operator
        const v = cur();
        if (
          !v ||
          (v.role !== "value" &&
            v.role !== "quoted" &&
            v.role !== "star" &&
            v.role !== "glob")
        ) {
          fail(`expected value after '${op.text}'`);
          return false;
        }
        advance(); // consume value
        return true;
      }
      // Standalone glob predicate
      return true;
    }

    if (
      s.role === "token" ||
      s.role === "quoted" ||
      s.role === "regex"
    ) {
      advance();
      return true;
    }

    fail(`unexpected '${s.text}'`);
    return false;
  }

  // Parse pipeline: filter_expr ( "|" pipe_op )*
  function parsePipeline(): void {
    // The filter part may be empty (e.g. "| stats count") — only parse if there's content.
    const s = cur();
    if (s && s.role !== "pipe") {
      parseOrExpr();
      if (errorAt >= 0) return;
    }

    // Parse pipe operators.
    while (cur()?.role === "pipe") {
      advance(); // consume |
      parsePipeOp();
      if (errorAt >= 0) return;
    }
  }

  // Parse a single pipe operator: stats_op | where_op
  function parsePipeOp(): void {
    const s = cur();
    if (!s) {
      fail("expected pipe operator after '|'");
      return;
    }

    if (s.role !== "pipe-keyword") {
      fail(`expected pipe keyword (stats, where), got '${s.text}'`);
      return;
    }

    const keyword = s.text.toLowerCase();
    advance(); // consume keyword

    switch (keyword) {
      case "stats":
        parseStatsOp();
        return;
      case "where":
        parseWhereOp();
        return;
      case "eval":
        parseEvalOp();
        return;
      case "sort":
        parseSortOp();
        return;
      case "head":
        parseHeadOp();
        return;
      case "tail":
        parseTailOp();
        return;
      case "timechart":
        parseTimechartOp();
        return;
      case "slice":
        parseSliceOp();
        return;
      case "rename":
        parseRenameOp();
        return;
      case "fields":
        parseFieldsOp();
        return;
      case "raw":
        // raw takes no arguments — nothing to parse.
        return;
      default:
        fail(`unknown pipe operator '${s.text}'`);
        return;
    }
  }

  // Parse stats: agg_list ( "by" group_list )?
  function parseStatsOp(): void {
    parseAggList();
    if (errorAt >= 0) return;

    // Optional "by" clause.
    if (cur()?.role === "pipe-keyword" && cur()!.text.toLowerCase() === "by") {
      advance(); // consume "by"
      parseGroupList();
    }
  }

  // Parse agg_list: agg_expr ( "," agg_expr )*
  function parseAggList(): void {
    parseAggExpr();
    if (errorAt >= 0) return;

    while (cur()?.role === "comma") {
      advance(); // consume ","
      parseAggExpr();
      if (errorAt >= 0) return;
    }
  }

  // Parse agg_expr: "count" ("as" IDENT)? | func "(" pipe_expr ")" ("as" IDENT)?
  function parseAggExpr(): void {
    const s = cur();
    if (!s) {
      fail("expected aggregation expression");
      return;
    }

    if (s.role === "function") {
      const funcName = s.text.toLowerCase();
      advance(); // consume function name

      if (funcName === "count" && cur()?.role !== "paren") {
        // Bare "count" without parens.
        parseOptionalAs();
        return;
      }

      // Expect "("
      if (cur()?.role !== "paren" || cur()!.text !== "(") {
        fail(`expected '(' after '${s.text}'`);
        return;
      }
      advance(); // consume "("

      // Parse the argument expression (consume until matching ")").
      parsePipeExpr();
      if (errorAt >= 0) return;

      if (cur()?.role !== "paren" || cur()!.text !== ")") {
        fail("expected ')'");
        return;
      }
      advance(); // consume ")"

      parseOptionalAs();
      return;
    }

    // Could be a bare field or expression — accept anything that looks like a token.
    if (s.role === "token" || s.role === "quoted") {
      advance();
      parseOptionalAs();
      return;
    }

    fail(`expected aggregation function or field, got '${s.text}'`);
  }

  // Parse a pipe expression (inside function args).
  // Simplified: accept tokens, numbers, fields, nested function calls, and arithmetic.
  function parsePipeExpr(): void {
    parsePipeAtom();
    if (errorAt >= 0) return;

    // Optional arithmetic: op atom op atom ...
    while (isPipeArithOp()) {
      advance(); // consume operator
      parsePipeAtom();
      if (errorAt >= 0) return;
    }
  }

  function isPipeArithOp(): boolean {
    const s = cur();
    if (!s) return false;
    // In pipe context, / and * are arithmetic operators but they're lexed
    // as word "/" and star "*" respectively. Also match compare-ops for >, <, etc.
    if (s.role === "star") return true;
    if (s.role === "token" && (s.text === "/" || s.text === "+" || s.text === "-" || s.text === "%")) return true;
    if (s.role === "compare-op") return true;
    return false;
  }

  function parsePipeAtom(): void {
    const s = cur();
    if (!s) {
      fail("expected expression");
      return;
    }

    // Parenthesized sub-expression.
    if (s.role === "paren" && s.text === "(") {
      advance(); // consume "("
      parsePipeExpr();
      if (errorAt >= 0) return;
      if (cur()?.role !== "paren" || cur()!.text !== ")") {
        fail("expected ')'");
        return;
      }
      advance(); // consume ")"
      return;
    }

    // Function call.
    if (s.role === "function") {
      advance(); // consume function name
      if (cur()?.role === "paren" && cur()!.text === "(") {
        advance(); // consume "("
        // Parse comma-separated arguments.
        if (cur()?.role !== "paren" || cur()!.text !== ")") {
          parsePipeExpr();
          if (errorAt >= 0) return;
          while (cur()?.role === "comma") {
            advance(); // consume ","
            parsePipeExpr();
            if (errorAt >= 0) return;
          }
        }
        if (cur()?.role !== "paren" || cur()!.text !== ")") {
          fail("expected ')'");
          return;
        }
        advance(); // consume ")"
      }
      return;
    }

    // Token (field reference or number literal).
    if (s.role === "token" || s.role === "quoted") {
      advance();
      return;
    }

    fail(`unexpected '${s.text}' in expression`);
  }

  // Parse optional "as" alias.
  function parseOptionalAs(): void {
    if (cur()?.role === "pipe-keyword" && cur()!.text.toLowerCase() === "as") {
      advance(); // consume "as"
      const alias = cur();
      if (!alias || alias.role !== "token") {
        fail("expected alias name after 'as'");
        return;
      }
      advance(); // consume alias
    }
  }

  // Parse group_list: group_expr ( "," group_expr )*
  function parseGroupList(): void {
    parseGroupExpr();
    if (errorAt >= 0) return;

    while (cur()?.role === "comma") {
      advance(); // consume ","
      parseGroupExpr();
      if (errorAt >= 0) return;
    }
  }

  // Parse group_expr: "bin" "(" ... ")" | IDENT
  function parseGroupExpr(): void {
    const s = cur();
    if (!s) {
      fail("expected group expression");
      return;
    }

    // bin() function call.
    if (s.role === "function" && s.text.toLowerCase() === "bin") {
      advance(); // consume "bin"
      if (cur()?.role !== "paren" || cur()!.text !== "(") {
        fail("expected '(' after 'bin'");
        return;
      }
      advance(); // consume "("
      // Consume args until ")".
      while (cur() && !(cur()!.role === "paren" && cur()!.text === ")")) {
        advance();
      }
      if (cur()?.role !== "paren" || cur()!.text !== ")") {
        fail("expected ')'");
        return;
      }
      advance(); // consume ")"
      return;
    }

    // Simple field reference.
    if (s.role === "token") {
      advance();
      return;
    }

    fail(`expected field name or bin(), got '${s.text}'`);
  }

  // Parse where: filter_expr
  function parseWhereOp(): void {
    const s = cur();
    if (!s) {
      fail("expected expression after 'where'");
      return;
    }
    parseOrExpr();
  }

  // Parse eval: field = expr (, field = expr)*
  function parseEvalOp(): void {
    parseEvalAssignment();
    if (errorAt >= 0) return;
    while (cur()?.role === "comma") {
      advance(); // consume ","
      parseEvalAssignment();
      if (errorAt >= 0) return;
    }
  }

  function parseEvalAssignment(): void {
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected field name in eval");
      return;
    }
    advance(); // consume field name
    if (cur()?.role !== "eq") {
      fail("expected '=' after field name in eval");
      return;
    }
    advance(); // consume "="
    parsePipeExpr();
  }

  // Parse sort: [-]field (, [-]field)*
  function parseSortOp(): void {
    parseSortField();
    if (errorAt >= 0) return;
    while (cur()?.role === "comma") {
      advance(); // consume ","
      parseSortField();
      if (errorAt >= 0) return;
    }
  }

  function parseSortField(): void {
    // Optional leading "-" for descending.
    if (cur()?.role === "compare-op" && cur()!.text === "-") {
      advance(); // consume "-"
    } else if (cur()?.role === "token" && cur()!.text === "-") {
      advance(); // consume "-"
    }
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected field name in sort");
      return;
    }
    advance(); // consume field name
  }

  // Parse head: NUMBER
  function parseHeadOp(): void {
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected number after 'head'");
      return;
    }
    if (!/^\d+$/.test(s.text)) {
      fail("expected positive integer after 'head'");
      return;
    }
    advance(); // consume number
  }

  function parseTailOp(): void {
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected number after 'tail'");
      return;
    }
    if (!/^\d+$/.test(s.text)) {
      fail("expected positive integer after 'tail'");
      return;
    }
    advance(); // consume number
  }

  // Parse timechart: NUMBER ["by" FIELD]
  function parseTimechartOp(): void {
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected number after 'timechart'");
      return;
    }
    if (!/^\d+$/.test(s.text)) {
      fail("expected positive integer after 'timechart'");
      return;
    }
    advance(); // consume number

    // Optional "by" FIELD
    const byKw = cur();
    if (byKw && byKw.role === "pipe-keyword" && byKw.text.toLowerCase() === "by") {
      advance(); // consume "by"
      const field = cur();
      if (!field || field.role !== "token") {
        fail("expected field name after 'by'");
        return;
      }
      advance(); // consume field
    }
  }

  // Parse slice: START END (both positive integers)
  function parseSliceOp(): void {
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected start index after 'slice'");
      return;
    }
    if (!/^\d+$/.test(s.text)) {
      fail("expected positive integer for slice start");
      return;
    }
    advance(); // consume start
    const e = cur();
    if (!e || e.role !== "token") {
      fail("expected end index after slice start");
      return;
    }
    if (!/^\d+$/.test(e.text)) {
      fail("expected positive integer for slice end");
      return;
    }
    advance(); // consume end
  }

  // Parse rename: field as field (, field as field)*
  function parseRenameOp(): void {
    parseRenameMapping();
    if (errorAt >= 0) return;
    while (cur()?.role === "comma") {
      advance(); // consume ","
      parseRenameMapping();
      if (errorAt >= 0) return;
    }
  }

  function parseRenameMapping(): void {
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected field name in rename");
      return;
    }
    advance(); // consume old name
    if (cur()?.role !== "pipe-keyword" || cur()!.text.toLowerCase() !== "as") {
      fail("expected 'as' after field name in rename");
      return;
    }
    advance(); // consume "as"
    const n = cur();
    if (!n || n.role !== "token") {
      fail("expected new field name after 'as' in rename");
      return;
    }
    advance(); // consume new name
  }

  // Parse fields: [-] field (, field)*
  function parseFieldsOp(): void {
    // Optional leading "-" for drop mode.
    if (cur()?.role === "compare-op" && cur()!.text === "-") {
      advance(); // consume "-"
    } else if (cur()?.role === "token" && cur()!.text === "-") {
      advance(); // consume "-"
    }
    const s = cur();
    if (!s || s.role !== "token") {
      fail("expected field name in fields");
      return;
    }
    advance(); // consume field name
    while (cur()?.role === "comma") {
      advance(); // consume ","
      const f = cur();
      if (!f || f.role !== "token") {
        fail("expected field name in fields");
        return;
      }
      advance(); // consume field name
    }
  }

  parsePipeline();

  if (errorAt < 0 && pos < indices.length) {
    errorAt = indices[pos]!;
    errorMessage = `unexpected '${spans[indices[pos]!]!.text}'`;
  }

  if (errorAt < 0) return { spans, errorMessage: null };

  return {
    spans: spans.map((span, i) =>
      i >= errorAt && span.role !== "whitespace"
        ? { ...span, role: "error" as HighlightRole }
        : span,
    ),
    errorMessage,
  };
}

interface TokenizeResult {
  spans: HighlightSpan[];
  hasErrors: boolean;
  hasPipeline: boolean;
  errorMessage: string | null;
}

export function tokenize(input: string, syntax: SyntaxSets = DEFAULT_SYNTAX): TokenizeResult {
  const { spans, errorMessage } = validate(classify(lex(input), syntax));
  return {
    spans,
    hasErrors: spans.some((s) => s.role === "error"),
    hasPipeline: hasPipeOutsideQuotes(input),
    errorMessage,
  };
}

// Check if the input contains a `|` outside of quoted strings.
export function hasPipeOutsideQuotes(input: string): boolean {
  let inQuote: string | null = null;
  for (let i = 0; i < input.length; i++) {
    const ch = input[i]!;
    if (inQuote) {
      if (ch === "\\" && i + 1 < input.length) {
        i++; // skip escaped char
      } else if (ch === inQuote) {
        inQuote = null;
      }
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
    } else if (ch === "|") {
      return true;
    }
  }
  return false;
}
