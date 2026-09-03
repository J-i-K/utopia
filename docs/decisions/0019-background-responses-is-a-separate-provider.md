# 0019 · Background Responses is a separate provider

- **Status**: implemented in code · Chat Completions remains the default and rollback path · entitlement, provider authorization, deployment, readiness, and the live canary are separate gates
- **Written**: 2026-09-03 (conventions in the [README](README.md))
- **Related**: [0014](0014-identity-from-the-person-scope-from-the-token.md) keeps identity and scope at the boundary; [0016](0016-close-the-open-seams-before-cutting-new-ones.md) keeps provider contracts small and explicit

> A ChatGPT subscription is not an OpenAI-compatible API key. Utopia's existing Chat Completions client is the right default for interactive chat, tools, and embeddings; it is the wrong abstraction for a subscription-backed Codex Responses session. The two transports therefore remain separate, and only the two background text workloads may opt into the new one.

## The switch belongs to the deployment

The provider choice is `UTOPIA_BACKGROUND_LLM_PROVIDER`, with `chat_completions` as the default. `codex_responses` is a deployment-wide opt-in, not a workspace setting. The credential belongs to the deployment; exposing a selector to workspace administrators would turn a personal or single-deployment subscription into an implicit tenant capability.

The background seam returns only complete plain text and a non-secret provider/model identity. It does not expose tools, streamed interactive turns, provider-managed conversation state, images, or `previous_response_id`. Interactive chat continues to construct the existing Chat Completions client, and embeddings continue to use the independently configured `/embeddings` path.

## Responses is not a renamed Chat Completions endpoint

The selectively ported wire subset follows the pinned Codex reference revision `1d74c3ba1ee98be2025ab066dcc3fd654fe8a3b6`. The client posts to the fixed production path `https://chatgpt.com/backend-api/codex/responses`, accepts `text/event-stream`, and maps the existing two-message background shape as follows:

- the system message becomes `instructions`;
- the user message becomes one typed `message` input containing one `input_text` item;
- the configured model, `store: false`, and `stream: true` are explicit;
- the small set of contract fields established by the reference is sent without importing the Codex CLI/runtime workspace.

The parser is a state machine. It appends only non-empty `response.output_text.delta` values and returns success only after a valid `response.completed` event with non-whitespace output. Malformed recognized events, failed/incomplete responses, premature EOF, duplicate terminal events, oversized frames/output, and transport timeouts fail closed. Unknown well-formed events are ignored and counted without logging payloads.

A test-only `test-util` constructor can use a local mock endpoint. The `utopia-llm` build script rejects that feature under Cargo's release profile, so ordinary release artifacts cannot enable arbitrary inference or refresh hosts. Production HTTP clients do not follow redirects; a redirect is a typed failure.

## The credential is a managed, single-owner profile

Utopia reads only a dedicated file-backed Codex profile. The configured directory must be absolute, non-symlinked, mode `0700`, and contain a regular `auth.json` with mode `0600`. The process acquires a lifetime ownership lock, so the directory must not be used concurrently by the ordinary Codex CLI or another Utopia process.

The refresh endpoint and OAuth client identifier are selectively ported from the pinned reference; the identifier is routing metadata, not a credential. Refresh is single-flight and re-reads the file after acquiring the mutex so concurrent callers do not reuse a stale refresh token. It runs with bounded connect/read/whole-request timeouts, accepts rotated access/refresh/ID tokens, preserves unknown JSON fields, and writes through temp-file sync, atomic rename, and directory sync. Rename is the in-process commit point: any persistence uncertainty disables dispatch and requires reauthentication rather than pretending the old refresh token remains safe.

The application never treats unverified token claims as authorization. Parsed expiry is only a refresh scheduling hint. Account identity is checked for consistency, unsupported FedRAMP profiles are rejected, and permanent refresh/authentication failures are distinct from transient transport or provider failures. An inference `401` permits one forced refresh and one replay; there is no general auth retry loop.

## Background lifecycle remains authoritative

Extraction still owns the existing capped rate-limit backoff. The model permit is held only across the provider request and is released before sleeping. A failed, malformed, incomplete, or unauthorized Responses call leaves the chunk unextracted and preserves visible document incompleteness. Permanent background authentication failures cross the application terminal boundary and receive a redacted alert instead of consuming ordinary retries forever.

Adjudication still validates a complete verdict batch before writing any verdict or performing an automatic merge. Indices must cover the requested batch exactly once and stay in range. The existing confidence thresholds, review escalation, conflict handling, audit records, and durable retry budget remain in force. The persisted model identity retains the raw Chat Completions model value and uses `codex_responses:<model>` for Codex results. Cache keys remain provider-agnostic so existing verdicts can be reused after a provider switch; only new Codex writes receive qualified provenance.

There is no automatic cross-provider fallback within a job. Mixing providers silently would make provenance, retry accounting, and operator decisions ambiguous. Rollback is an explicit configuration action: remove the Codex overlay or set `UTOPIA_BACKGROUND_LLM_PROVIDER=chat_completions`, restart only under authorization, and verify background Chat Completions, interactive chat, and embeddings separately.

## Security boundary and open gate

The default Compose deployment does not mount a Codex profile and remains Chat Completions-based. The explicit `docker-compose.codex.yml` overlay mounts one dedicated host directory read-write because refresh rotation must persist; invoke it through `bash scripts/utopia-codex-compose.sh`, which rejects relative and repository-internal host paths before Compose. The current image does not declare a non-root runtime user, so a compromised Utopia process can read the mounted credentials. This design reduces profile collision and deployment scope; it does not provide secret isolation from the application process or host administrator.

Repository tests use synthetic credentials and local mock servers only. They prove request mapping, authentication lifecycle, redirect refusal, bounded parsing, error classification, routing, provenance, and rollback shape. The current binary-only server test target has no no-dependency `AppState`/database harness; durable extraction/adjudication lifecycle acceptance must therefore run in an authorized environment with `UTOPIA_DATABASE_URL`. The local unit suite does not claim persistence, no-merge, or terminal-job acceptance. Repository tests also do not prove account entitlement, provider permission to reuse the Codex OAuth flow, current backend stability, container readiness, deployment, or a user-visible live result. Those remain explicit operator/provider gates and require separate authorization.
