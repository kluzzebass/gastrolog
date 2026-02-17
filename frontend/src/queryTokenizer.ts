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
  | "star"
  | "regex" // /pattern/
  | "whitespace"
  | "error"; // unterminated quote or regex

// After the post-pass, words in key=value positions get reclassified.
export type HighlightRole =
  | "operator" // AND, OR, NOT
  | "directive-key" // key portion of a control arg (last, reverse, ...)
  | "key" // key in key=value predicate
  | "eq" // = sign
  | "value" // value in key=value predicate
  | "token" // bare search term
  | "quoted" // quoted string (standalone or as value)
  | "regex" // /pattern/ regex literal
  | "paren" // ( or )
  | "star" // *
  | "whitespace"
  | "error";

interface QueryToken {
  text: string;
  pos: number;
  kind: QueryTokenKind;
}

export interface HighlightSpan {
  text: string;
  role: HighlightRole;
}

export const DIRECTIVES = new Set([
  "reverse",
  "start",
  "end",
  "last",
  "limit",
  "pos",
  "source_start",
  "source_end",
  "ingest_start",
  "ingest_end",
]);

// Phase 1: Raw lexing — character scan producing tokens with whitespace preserved.
function lex(input: string): QueryToken[] {
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
    if (ch === "*") {
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

    // Regex literal: /pattern/
    if (ch === "/") {
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

    // Bareword
    const start = pos;
    while (pos < input.length && isBarewordChar(input[pos]!)) {
      pos++;
    }
    const lit = input.slice(start, pos);
    const upper = lit.toUpperCase();
    const kind: QueryTokenKind =
      upper === "AND" || upper === "OR" || upper === "NOT"
        ? "operator"
        : "word";
    tokens.push({ text: lit, pos: start, kind });
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
    ch !== '"' &&
    ch !== "'" &&
    ch !== "/"
  );
}

// Phase 2: Post-pass — detect key=value sequences and classify roles.
function classify(raw: QueryToken[]): HighlightSpan[] {
  const spans: HighlightSpan[] = [];

  let i = 0;
  while (i < raw.length) {
    const tok = raw[i]!;

    // Detect word = (word|star|quoted) patterns (with no whitespace around =)
    if (
      (tok.kind === "word" || tok.kind === "star") &&
      i + 2 < raw.length &&
      raw[i + 1]!.kind === "eq" &&
      (raw[i + 2]!.kind === "word" ||
        raw[i + 2]!.kind === "star" ||
        raw[i + 2]!.kind === "quoted")
    ) {
      const key = tok.text;
      const eq = raw[i + 1]!;
      const val = raw[i + 2]!;

      if (tok.kind === "word" && DIRECTIVES.has(key.toLowerCase())) {
        // Directive: italic key, dim =, normal value
        spans.push({ text: key, role: "directive-key" });
        spans.push({ text: eq.text, role: "eq" });
        spans.push({
          text: val.text,
          role:
            val.kind === "quoted"
              ? "quoted"
              : val.kind === "star"
                ? "star"
                : "value",
        });
      } else {
        // key=value predicate
        spans.push({ text: key, role: tok.kind === "star" ? "star" : "key" });
        spans.push({ text: eq.text, role: "eq" });
        spans.push({
          text: val.text,
          role:
            val.kind === "quoted"
              ? "quoted"
              : val.kind === "star"
                ? "star"
                : "value",
        });
      }
      i += 3;
      continue;
    }

    // Map remaining tokens to roles
    switch (tok.kind) {
      case "operator":
        spans.push({ text: tok.text, role: "operator" });
        break;
      case "quoted":
        spans.push({ text: tok.text, role: "quoted" });
        break;
      case "lparen":
      case "rparen":
        spans.push({ text: tok.text, role: "paren" });
        break;
      case "eq":
        spans.push({ text: tok.text, role: "eq" });
        break;
      case "star":
        spans.push({ text: tok.text, role: "star" });
        break;
      case "regex":
        spans.push({ text: tok.text, role: "regex" });
        break;
      case "whitespace":
        spans.push({ text: tok.text, role: "whitespace" });
        break;
      case "error":
        spans.push({ text: tok.text, role: "error" });
        break;
      default:
        spans.push({ text: tok.text, role: "token" });
        break;
    }
    i++;
  }

  return spans;
}

// Phase 3: Validate via recursive descent parser mirroring backend grammar.
// The source of truth is backend/internal/querylang/parser.go — changes
// there must be reflected here.
// Finds the first span index where parsing fails; marks it and everything
// after it as "error".
//
// Grammar (matches backend/internal/querylang/parser.go):
//   query      = or_expr EOF
//   or_expr    = and_expr ( "OR" and_expr )*
//   and_expr   = unary_expr ( [ "AND" ] unary_expr )*
//   unary_expr = "NOT" unary_expr | primary
//   primary    = "(" or_expr ")" | predicate
//   predicate  = kv_triple | regex | token | quoted | star_pred
//   kv_triple  = (key|directive-key) eq (value|quoted|star)
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

  // Is the current span a predicate start? (token, quoted, key, directive-key, star, or kv triple)
  function isPredStart(): boolean {
    const s = cur();
    if (!s) return false;
    return (
      s.role === "token" ||
      s.role === "quoted" ||
      s.role === "regex" ||
      s.role === "key" ||
      s.role === "directive-key" ||
      s.role === "star" ||
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

    if (s.role === "key" || s.role === "directive-key") {
      advance(); // consume key
      if (cur()?.role !== "eq") {
        fail("expected '=' after key");
        return false;
      }
      advance(); // consume eq
      const v = cur();
      if (
        !v ||
        (v.role !== "value" && v.role !== "quoted" && v.role !== "star")
      ) {
        fail("expected value after '='");
        return false;
      }
      advance(); // consume value
      return true;
    }

    if (s.role === "star") {
      const saved = pos;
      advance(); // consume *
      if (cur()?.role === "eq") {
        advance(); // consume eq
        const v = cur();
        if (
          !v ||
          (v.role !== "value" && v.role !== "quoted" && v.role !== "star")
        ) {
          fail("expected value after '*='");
          return false;
        }
        advance(); // consume value
        return true;
      }
      pos = saved;
      fail("'*' must be followed by '='");
      return false;
    }

    if (s.role === "token" || s.role === "quoted" || s.role === "regex") {
      advance();
      return true;
    }

    fail(`unexpected '${s.text}'`);
    return false;
  }

  parseOrExpr();

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
  errorMessage: string | null;
}

export function tokenize(input: string): TokenizeResult {
  const { spans, errorMessage } = validate(classify(lex(input)));
  return {
    spans,
    hasErrors: spans.some((s) => s.role === "error"),
    errorMessage,
  };
}
