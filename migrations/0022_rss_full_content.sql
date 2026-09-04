-- Full-content RSS activation and hydration ledger.
-- Documents remain the authority for accepted content; these tables retain discovery,
-- baseline, retry, and terminal state after an entry leaves the feed window.

CREATE TABLE rss_full_content_sources (
    source_id             UUID PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    activation_generation INTEGER NOT NULL DEFAULT 1
        CHECK (activation_generation >= 1),
    activation_state      TEXT NOT NULL
        CHECK (activation_state IN ('pending', 'active', 'disabled')),
    activation_at         TIMESTAMPTZ,
    baseline_count        INTEGER NOT NULL DEFAULT 0
        CHECK (baseline_count >= 0),
    last_discovery_at     TIMESTAMPTZ,
    last_discovery_count  INTEGER NOT NULL DEFAULT 0
        CHECK (last_discovery_count >= 0),
    last_queued_count     INTEGER NOT NULL DEFAULT 0
        CHECK (last_queued_count >= 0),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (activation_state = 'active' AND activation_at IS NOT NULL)
        OR (activation_state = 'pending' AND activation_at IS NULL)
        OR activation_state = 'disabled'
    )
);

CREATE TABLE rss_full_content_entries (
    id                    UUID PRIMARY KEY,
    source_id             UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    activation_generation INTEGER NOT NULL CHECK (activation_generation >= 1),
    external_key          TEXT NOT NULL,
    title                 TEXT NOT NULL,
    article_url           TEXT,
    summary               TEXT NOT NULL DEFAULT '',
    embedded_html         TEXT,
    doc_time              TIMESTAMPTZ,
    state                 TEXT NOT NULL
        CHECK (state IN (
            'baseline', 'pending', 'queued', 'hydrating',
            'retry_wait', 'complete', 'terminal'
        )),
    hydration_job_id      BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    attempt_count         INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    content_source        TEXT CHECK (
        content_source IS NULL OR content_source IN ('feed', 'web')
    ),
    final_url             TEXT,
    content_sha256        TEXT,
    document_id           UUID REFERENCES documents(id) ON DELETE SET NULL,
    error_code            TEXT,
    error_detail          TEXT,
    first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ,
    UNIQUE (source_id, activation_generation, external_key),
    CHECK (octet_length(external_key) <= 4096),
    CHECK (length(title) > 0 AND octet_length(title) <= 2048),
    CHECK (article_url IS NULL OR octet_length(article_url) <= 8192),
    CHECK (final_url IS NULL OR octet_length(final_url) <= 8192),
    CHECK (octet_length(summary) <= 16384),
    CHECK (embedded_html IS NULL OR octet_length(embedded_html) <= 2097152),
    CHECK (error_code IS NULL OR octet_length(error_code) <= 64),
    CHECK (error_detail IS NULL OR octet_length(error_detail) <= 2048),
    CHECK (
        (state = 'complete'
         AND content_source IS NOT NULL
         AND content_sha256 ~ '^[0-9a-f]{64}$'
         AND document_id IS NOT NULL
         AND completed_at IS NOT NULL)
        OR state <> 'complete'
    )
);

CREATE INDEX rss_full_content_entries_pending_idx
    ON rss_full_content_entries(source_id, activation_generation, state, first_seen_at)
    WHERE state IN ('pending', 'retry_wait');

CREATE INDEX rss_full_content_entries_document_idx
    ON rss_full_content_entries(document_id)
    WHERE document_id IS NOT NULL;
