# Security

*[中文版](SECURITY.zh-CN.md)*

Utopia is at v0.1. Below are the **known, unresolved** limits — not a vulnerability report,
but the places the design has not reached yet.

## Before you put this on a public network

**Credentials are stored in the clear.** LLM API keys and Ask-the-Data connection strings
are plain text in Postgres (`llm_settings.chat_api_key`, `data_sources.conn_string`). Anyone
who can read the database can read them. Encryption at rest is a 1.0 item; until then, keep
the system and its database inside a trusted network.

**The default database password is `utopia`.** By default the port binds to loopback
(`127.0.0.1:1517`), so nothing outside the host can connect. If you change `UTOPIA_DB_BIND`
to expose it, change `UTOPIA_DB_PASSWORD` in `.env` first.

**A data source is only as safe as its grants.** Registering one is a deployment-level
action, but the connection string reaches every workspace the source is granted to. Grant it
only where that database should be visible, and use a read-only database role in the string
itself — the SQL gate below is defence in depth, not a substitute for least privilege at the
source.

## What is in place

- **JWT signing key generated on first start** — 32 bytes from a CSPRNG, stored in the
  database. No deployment shares a default key.
- **`Secure` on session cookies behind TLS** — decided from `X-Forwarded-Proto`, so local
  HTTP development still works. Force it with `UTOPIA_COOKIE_SECURE=true` if your proxy
  omits the header.
- **Database port bound to loopback** — `127.0.0.1:1517`; the app reaches the database over
  the compose network.
- **Optional least-privilege runtime role** — set `UTOPIA_APP_DB_PASSWORD` and
  `UTOPIA_MIGRATION_URL`, and the app connects as a role that can only read and write
  business tables and append to the ledger, while migrations run as the owner.
- **Data sources reach only granted workspaces** — a registered database is mounted into a
  knowledge base only where an explicit grant exists. Before this, any base admin could
  mount any registered source, which crossed tenants.
- **Read-only gate on Ask-the-Data** — parser allowlist, read-only transaction, enforced row
  limit; three layers, so a statement past the parser still cannot write.
- **Accounts are deactivated, not deleted** — `users.deactivated_at` blocks sign-in while the
  ledger keeps that person's decisions attributable.
- **Passwords hashed with argon2.**

## Optional ChatGPT subscription transport

Background extraction and entity adjudication may be configured to use the Codex Responses transport, but it is **disabled by default**. Interactive chat, chat tools, ontology/type-resolution work, and embeddings remain on their existing Chat Completions or embeddings paths.

This mode is a deployment-wide choice. It is not a per-workspace setting, because the subscription credential belongs to the deployment and must not become an implicit workspace-admin capability. Enable it only with the explicit `docker-compose.codex.yml` overlay through `bash scripts/utopia-codex-compose.sh`; the wrapper rejects relative host paths and paths resolving inside the repository before invoking Compose. Native Compose only provides the nonempty-variable check. Use a dedicated Utopia-owned `CODEX_HOME` outside the repository. Do not mount an operator's normal `~/.codex` profile or put token material in `.env`, Postgres, logs, backups, or source control.

The credential directory must be mode `0700` and contain a file-backed `auth.json` with mode `0600`. The application needs read-write access because refresh-token rotation is persisted atomically. Utopia takes a process-lifetime ownership lock; do not use the directory concurrently with another Codex process. Exclude the directory from ordinary backups and re-provision it through the approved device-login procedure if access is revoked or refresh persistence becomes uncertain.

The current container image does not declare a non-root runtime user. A compromised application process can therefore read the mounted access and refresh credentials. The dedicated directory limits accidental profile collision and credential scope; it is not isolation from a compromised Utopia process or host administrator.

The production Responses and refresh hosts are compiled/fixed, HTTPS-only, and redirects are not followed. Alternate hosts are available only through the test-only Rust feature. Startup validates the local model, credential shape, path, permissions, ownership lock, and concurrency bound without making a remote model call. Refresh is single-flight, bounded, and disabled after permanent authentication or durability failures. Failed or incomplete Responses streams do not count as model output, and rate limits retain the existing bounded retry owner.

A successful device login is not proof that the account is entitled to this server-side use, nor proof that the provider permits reuse of the OAuth client flow by Utopia. Those are separate operator and provider gates. No live subscription request or deployment acceptance is claimed by the repository tests.

## Reporting a vulnerability

Email **security@deeplethe.com** rather than opening a public issue. Include the affected
version or commit, the endpoint or component, and steps to reproduce. You will get an
acknowledgement within a few days, and the release that carries the fix names you unless you
ask otherwise.
