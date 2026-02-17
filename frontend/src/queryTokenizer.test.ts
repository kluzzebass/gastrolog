import { describe, expect, test } from "bun:test";
import { tokenize, DIRECTIVES } from "./queryTokenizer";

/** Helper: tokenize and return non-whitespace spans as [text, role] tuples. */
function spans(input: string): [string, string][] {
  return tokenize(input)
    .spans.filter((s) => s.role !== "whitespace")
    .map((s) => [s.text, s.role]);
}

/** Helper: tokenize and return the error message (or null). */
function _errorMsg(input: string): string | null {
  return tokenize(input).errorMessage;
}

/** Helper: verify that concatenating all span texts reproduces input. */
function roundtrip(input: string) {
  const result = tokenize(input);
  const reconstructed = result.spans.map((s) => s.text).join("");
  expect(reconstructed).toBe(input);
}

describe("tokenize roundtrip (text preservation)", () => {
  const cases = [
    "",
    "hello",
    "key=value",
    "  spaces  between  ",
    '  key="quoted value"  ',
    "a AND b OR NOT c",
    "(a OR b) AND c",
    "last=5m reverse=true level=error",
    "key=*",
    "*=value",
    'msg="hello \\"world\\""',
  ];
  for (const input of cases) {
    test(JSON.stringify(input), () => roundtrip(input));
  }
});

describe("simple tokens", () => {
  test("bare word", () => {
    expect(spans("hello")).toEqual([["hello", "token"]]);
  });

  test("two words (implicit AND)", () => {
    expect(spans("hello world")).toEqual([
      ["hello", "token"],
      ["world", "token"],
    ]);
  });

  test("quoted string", () => {
    expect(spans('"hello world"')).toEqual([['"hello world"', "quoted"]]);
  });

  test("single-quoted string", () => {
    expect(spans("'hello world'")).toEqual([["'hello world'", "quoted"]]);
  });
});

describe("operators", () => {
  test("AND", () => {
    expect(spans("a AND b")).toEqual([
      ["a", "token"],
      ["AND", "operator"],
      ["b", "token"],
    ]);
  });

  test("OR", () => {
    expect(spans("a OR b")).toEqual([
      ["a", "token"],
      ["OR", "operator"],
      ["b", "token"],
    ]);
  });

  test("NOT", () => {
    expect(spans("NOT a")).toEqual([
      ["NOT", "operator"],
      ["a", "token"],
    ]);
  });

  test("case insensitive operators", () => {
    // Bare operators are invalid (nothing to operate on), so they get
    // reclassified as errors by the validator. Check they're recognized
    // as operators when used correctly.
    expect(spans("a and b")[1]).toEqual(["and", "operator"]);
    expect(spans("a or b")[1]).toEqual(["or", "operator"]);
    expect(spans("not a")[0]).toEqual(["not", "operator"]);
  });
});

describe("key=value predicates", () => {
  test("simple key=value", () => {
    expect(spans("level=error")).toEqual([
      ["level", "key"],
      ["=", "eq"],
      ["error", "value"],
    ]);
  });

  test("key=quoted", () => {
    expect(spans('msg="hello"')).toEqual([
      ["msg", "key"],
      ["=", "eq"],
      ['"hello"', "quoted"],
    ]);
  });

  test("key=*", () => {
    expect(spans("level=*")).toEqual([
      ["level", "key"],
      ["=", "eq"],
      ["*", "star"],
    ]);
  });

  test("*=value", () => {
    expect(spans("*=error")).toEqual([
      ["*", "star"],
      ["=", "eq"],
      ["error", "value"],
    ]);
  });
});

describe("directives", () => {
  for (const dir of DIRECTIVES) {
    test(`${dir}= is directive`, () => {
      const result = spans(`${dir}=foo`);
      expect(result[0]).toEqual([dir, "directive-key"]);
      expect(result[1]).toEqual(["=", "eq"]);
      expect(result[2]).toEqual(["foo", "value"]);
    });
  }

  test("non-directive key", () => {
    expect(spans("custom=value")[0]).toEqual(["custom", "key"]);
  });
});

describe("parentheses", () => {
  test("grouped expression", () => {
    expect(spans("(a OR b)")).toEqual([
      ["(", "paren"],
      ["a", "token"],
      ["OR", "operator"],
      ["b", "token"],
      [")", "paren"],
    ]);
  });

  test("nested groups", () => {
    const result = spans("(a AND (b OR c))");
    expect(result.filter(([_, r]) => r === "paren")).toEqual([
      ["(", "paren"],
      ["(", "paren"],
      [")", "paren"],
      [")", "paren"],
    ]);
  });
});

describe("validation: valid queries", () => {
  const valid = [
    "",
    "hello",
    "hello world",
    "a AND b",
    "a OR b",
    "NOT a",
    "NOT NOT a",
    "(a OR b)",
    "(a OR b) AND c",
    "level=error",
    "level=error msg=hello",
    "last=5m level=error",
    "key=*",
    "*=value",
    '"quoted search"',
    "a AND NOT b",
    "(a AND b) OR (c AND d)",
    "NOT (a OR b)",
  ];

  for (const q of valid) {
    test(JSON.stringify(q), () => {
      const result = tokenize(q);
      expect(result.hasErrors).toBe(false);
      expect(result.errorMessage).toBeNull();
    });
  }
});

describe("validation: invalid queries", () => {
  test("unterminated double quote", () => {
    const r = tokenize('"hello');
    expect(r.hasErrors).toBe(true);
    expect(r.errorMessage).toBe("unterminated string");
  });

  test("unterminated single quote", () => {
    const r = tokenize("'hello");
    expect(r.hasErrors).toBe(true);
  });

  test("empty parens", () => {
    const r = tokenize("()");
    expect(r.hasErrors).toBe(true);
    expect(r.errorMessage).toContain("empty parentheses");
  });

  test("unmatched open paren", () => {
    const r = tokenize("(a OR b");
    expect(r.hasErrors).toBe(true);
    expect(r.errorMessage).toContain("unmatched");
  });

  test("unmatched close paren", () => {
    const r = tokenize("a OR b)");
    expect(r.hasErrors).toBe(true);
  });

  test("NOT at end", () => {
    const r = tokenize("a AND NOT");
    expect(r.hasErrors).toBe(true);
    expect(r.errorMessage).toContain("after NOT");
  });

  test("NOT followed by OR", () => {
    const r = tokenize("NOT OR a");
    expect(r.hasErrors).toBe(true);
    expect(r.errorMessage).toContain("after NOT");
  });

  test("NOT followed by )", () => {
    const r = tokenize("(NOT )");
    expect(r.hasErrors).toBe(true);
    expect(r.errorMessage).toContain("after NOT");
  });

  test("bare * without =", () => {
    const r = tokenize("*");
    expect(r.hasErrors).toBe(true);
  });

  test("trailing AND", () => {
    const r = tokenize("a AND");
    expect(r.hasErrors).toBe(true);
  });

  test("trailing OR", () => {
    const r = tokenize("a OR");
    expect(r.hasErrors).toBe(true);
  });

  test("double operator", () => {
    const r = tokenize("a AND OR b");
    expect(r.hasErrors).toBe(true);
  });
});

describe("regex literals", () => {
  test("basic regex", () => {
    expect(spans("/error\\d+/")).toEqual([["/error\\d+/", "regex"]]);
  });

  test("regex roundtrip", () => {
    roundtrip("/error\\d+/");
    roundtrip("/pattern/");
    roundtrip("/path\\/to\\/file/");
  });

  test("regex with escaped slash", () => {
    expect(spans("/path\\/to/")).toEqual([["/path\\/to/", "regex"]]);
  });

  test("regex AND token", () => {
    expect(spans("/timeout/ AND level=error")).toEqual([
      ["/timeout/", "regex"],
      ["AND", "operator"],
      ["level", "key"],
      ["=", "eq"],
      ["error", "value"],
    ]);
  });

  test("NOT regex", () => {
    expect(spans("NOT /debug/")).toEqual([
      ["NOT", "operator"],
      ["/debug/", "regex"],
    ]);
  });

  test("implicit AND with regex", () => {
    expect(spans("error /timeout/")).toEqual([
      ["error", "token"],
      ["/timeout/", "regex"],
    ]);
  });

  test("regex in parens", () => {
    const r = tokenize("(/error/ OR /warn/)");
    expect(r.hasErrors).toBe(false);
  });

  test("unterminated regex", () => {
    const r = tokenize("/unterminated");
    expect(r.hasErrors).toBe(true);
  });

  test("slash no longer in bareword", () => {
    // "path/to/" becomes: word "path", regex "to", rather than a single bareword
    const result = spans("path/to/");
    expect(result[0]).toEqual(["path", "token"]);
    expect(result[1]).toEqual(["/to/", "regex"]);
  });
});

describe("complex queries", () => {
  test("realistic search query", () => {
    const r = tokenize('last=5m reverse=true level=error msg="connection refused"');
    expect(r.hasErrors).toBe(false);
    const nonWs = r.spans.filter((s) => s.role !== "whitespace");
    // last=5m: directive-key eq value
    expect(nonWs[0]!.role).toBe("directive-key");
    // level=error: key eq value
    expect(nonWs[6]!.role).toBe("key");
    expect(nonWs[6]!.text).toBe("level");
  });

  test("mixed predicates and boolean", () => {
    const r = tokenize("level=error AND (msg=timeout OR msg=refused)");
    expect(r.hasErrors).toBe(false);
  });

  test("escape in quoted string", () => {
    const r = tokenize('msg="hello \\"world\\""');
    expect(r.hasErrors).toBe(false);
    roundtrip('msg="hello \\"world\\""');
  });
});
