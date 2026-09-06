# Background ChatGPT subscription authentication

**Status: Proposed — awaiting discussion in [#435](https://github.com/deeplethe/utopia/issues/435); not accepted or implemented upstream**

## Context

Some operators want to use an existing ChatGPT subscription for background extraction and entity adjudication. Utopia currently uses OpenAI-compatible API access. Subscription access would introduce a different authentication lifecycle and credential authority, not merely another API key.

A local prototype exists, but this proposal submits no implementation. Passing local tests does not establish provider permission, protocol support, entitlement, quota, or live compatibility. Those questions remain part of the acceptance decision.

## Proposed decision

Make subscription access an explicit, deployment-enabled option for background extraction and adjudication. Keep interactive chat and embeddings on their existing API paths. Do not silently fall back between subscription and API access.

Deployment administrators explicitly start, inspect, and cancel a bounded native device-code flow. Workspace selection never starts authentication. Only temporary verification details and redacted status reach the browser; tokens and the Proof Key for Code Exchange (PKCE) verifier stay server-side. Concurrent starts, cancellation during the initial request, and stale completion must be fenced through credential publication.

Use two mutually exclusive credential sources:

- **Internal:** Utopia owns a private directory below its absolute deployment data directory. Credential replacement is atomic and durable, with directory mode `0700` and file mode `0600`. Refresh and reauthentication stay within that authority.
- **External preauthenticated:** the operator explicitly selects an existing credential directory. Utopia reads it without creating locks, refreshing, starting authentication, or persisting anything there. Its external owner performs rotation. Missing or unusable credentials fail closed.

Preserve upstream's sealed API-key storage and API-model concurrency controls. Integrate a separate, bounded subscription transport through the current background runtime rather than restoring removed background machinery. Keep provider endpoints fixed in ordinary builds and confine alternate endpoints to test tooling.

## Alternatives

- **API access only:** the smallest supported surface; retain this if maintainers decline the proposal or provider compatibility cannot be established.
- **Automatic login on workspace selection:** rejected because a workspace operation must not initiate deployment-wide credential changes.
- **External CLI as runtime authority:** not proposed; it adds another process and lifecycle dependency. Explicit read-only credentials offer a narrower compatibility path.
- **Reuse database API-key fields for subscription tokens:** rejected because token rotation and external ownership differ from the API-key contract.

## Acceptance before implementation lands

Maintainers must agree on feature scope, provider-policy/support requirements, and credential ownership. A subsequent implementation PR must demonstrate read-only external access, private durable internal writes, race-safe cancellation, no silent fallback, bounded transport and concurrency, and readiness checks that do not issue subscription inference.

Require synthetic-provider regression tests, authenticated route and UI coverage, database-backed migration and sealed-secret tests, and the repository's lint/build gates. Report live-provider verification separately. This ADR does not authorize a deployment or claim that provider approval has been obtained.
