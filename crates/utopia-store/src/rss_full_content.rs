//! Durable discovery and hydration state for opt-in full-content RSS sources.
//!
//! The ledger is deliberately separate from `documents`: a feed item can be
//! discovered before it has acceptable content, and it must remain hydratable
//! after it leaves the publisher's sliding feed window.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use utopia_core::{AppError, AppResult};
use uuid::Uuid;

pub const HYDRATION_JOB_KIND: &str = "hydrate_rss_entry";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Activation {
    pub source_id: Uuid,
    pub activation_generation: i32,
    pub activation_state: String,
    pub activation_at: Option<DateTime<Utc>>,
    pub baseline_count: i32,
    pub last_discovery_at: Option<DateTime<Utc>>,
    pub last_discovery_count: i32,
    pub last_queued_count: i32,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Entry {
    pub id: Uuid,
    pub source_id: Uuid,
    pub activation_generation: i32,
    pub external_key: String,
    pub title: String,
    pub article_url: Option<String>,
    pub summary: String,
    pub embedded_html: Option<String>,
    pub doc_time: Option<DateTime<Utc>>,
    pub state: String,
    pub hydration_job_id: Option<i64>,
    pub attempt_count: i32,
    pub content_source: Option<String>,
    pub final_url: Option<String>,
    pub content_sha256: Option<String>,
    pub document_id: Option<Uuid>,
    pub error_code: Option<String>,
    pub error_detail: Option<String>,
    pub first_seen_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Values obtained from one bounded feed response. The database remains the
/// authority for lifecycle state; `has_usable_source` only controls the first
/// state for a newly discovered row.
#[derive(Debug, Clone)]
pub struct NewEntry {
    pub external_key: String,
    pub title: String,
    pub article_url: Option<String>,
    pub summary: String,
    pub embedded_html: Option<String>,
    pub doc_time: Option<DateTime<Utc>>,
    pub has_usable_source: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DiscoveryStats {
    pub discovered: usize,
    pub terminal: usize,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Counts {
    pub activation_state: String,
    pub activation_generation: i32,
    pub baseline_count: i32,
    pub last_discovery_at: Option<DateTime<Utc>>,
    pub last_discovery_count: i32,
    pub last_queued_count: i32,
    pub pending_count: i64,
    pub queued_count: i64,
    pub hydrating_count: i64,
    pub retrying_count: i64,
    pub complete_count: i64,
    pub terminal_count: i64,
}

pub async fn initialize_source(
    tx: &mut Transaction<'_, Postgres>,
    source_id: Uuid,
) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO rss_full_content_sources (source_id, activation_generation, activation_state)
         VALUES ($1, 1, 'pending')
         ON CONFLICT (source_id) DO NOTHING",
    )
    .bind(source_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Begin a new activation generation. Older rows and jobs remain for audit,
/// but cannot be mistaken for observations in this activation.
pub async fn enable_source(tx: &mut Transaction<'_, Postgres>, source_id: Uuid) -> AppResult<()> {
    sqlx::query(
        "INSERT INTO rss_full_content_sources
             (source_id, activation_generation, activation_state)
         VALUES ($1, 1, 'pending')
         ON CONFLICT (source_id) DO UPDATE SET
             activation_generation = rss_full_content_sources.activation_generation + 1,
             activation_state = 'pending',
             activation_at = NULL,
             baseline_count = 0,
             last_discovery_at = NULL,
             last_discovery_count = 0,
             last_queued_count = 0,
             updated_at = now()",
    )
    .bind(source_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn disable_source(tx: &mut Transaction<'_, Postgres>, source_id: Uuid) -> AppResult<()> {
    sqlx::query(
        "UPDATE rss_full_content_sources
            SET activation_state = 'disabled', updated_at = now()
          WHERE source_id = $1",
    )
    .bind(source_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn get_activation(pool: &PgPool, source_id: Uuid) -> AppResult<Option<Activation>> {
    Ok(sqlx::query_as(
        "SELECT source_id, activation_generation, activation_state, activation_at,
                baseline_count, last_discovery_at, last_discovery_count, last_queued_count
           FROM rss_full_content_sources
          WHERE source_id = $1",
    )
    .bind(source_id)
    .fetch_optional(pool)
    .await?)
}

/// Insert the first successful response for a pending generation and activate
/// it atomically. No document or hydration job is created in this operation.
pub async fn record_baseline(
    pool: &PgPool,
    source_id: Uuid,
    generation: i32,
    entries: &[NewEntry],
) -> AppResult<usize> {
    let mut tx = pool.begin().await?;
    let activation: Option<(String, i32)> = sqlx::query_as(
        "SELECT activation_state, activation_generation
           FROM rss_full_content_sources
          WHERE source_id = $1
          FOR UPDATE",
    )
    .bind(source_id)
    .fetch_optional(&mut *tx)
    .await?;
    if activation != Some(("pending".to_string(), generation)) {
        return Err(AppError::Conflict(
            "RSS full-content activation changed while recording baseline".into(),
        ));
    }

    let mut inserted = 0usize;
    for entry in entries {
        let row: Option<(Uuid,)> = sqlx::query_as(
            "INSERT INTO rss_full_content_entries
                 (id, source_id, activation_generation, external_key, title,
                  article_url, summary, embedded_html, doc_time, state)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'baseline')
             ON CONFLICT (source_id, activation_generation, external_key) DO NOTHING
             RETURNING id",
        )
        .bind(Uuid::now_v7())
        .bind(source_id)
        .bind(generation)
        .bind(&entry.external_key)
        .bind(&entry.title)
        .bind(&entry.article_url)
        .bind(&entry.summary)
        .bind(&entry.embedded_html)
        .bind(entry.doc_time)
        .fetch_optional(&mut *tx)
        .await?;
        inserted += usize::from(row.is_some());
    }

    let result = sqlx::query(
        "UPDATE rss_full_content_sources
            SET activation_state = 'active', activation_at = now(),
                baseline_count = $2, last_discovery_at = now(),
                last_discovery_count = $3, last_queued_count = 0, updated_at = now()
          WHERE source_id = $1 AND activation_generation = $4
            AND activation_state = 'pending'",
    )
    .bind(source_id)
    .bind(inserted as i32)
    .bind(entries.len() as i32)
    .bind(generation)
    .execute(&mut *tx)
    .await?;
    if result.rows_affected() != 1 {
        return Err(AppError::Conflict(
            "RSS full-content activation changed while activating baseline".into(),
        ));
    }
    tx.commit().await?;
    Ok(inserted)
}

/// Upsert observations for an active generation. Existing lifecycle state is
/// preserved except a prior no-source terminal row can become pending when a
/// later feed response supplies a usable body or link.
pub async fn discover(
    pool: &PgPool,
    source_id: Uuid,
    generation: i32,
    entries: &[NewEntry],
) -> AppResult<DiscoveryStats> {
    let mut tx = pool.begin().await?;
    let activation: Option<(String, i32)> = sqlx::query_as(
        "SELECT activation_state, activation_generation
           FROM rss_full_content_sources
          WHERE source_id = $1
          FOR UPDATE",
    )
    .bind(source_id)
    .fetch_optional(&mut *tx)
    .await?;
    if activation != Some(("active".to_string(), generation)) {
        return Err(AppError::Conflict(
            "RSS full-content source is not active for this generation".into(),
        ));
    }

    let mut stats = DiscoveryStats::default();
    for entry in entries {
        let state = if entry.has_usable_source {
            "pending"
        } else {
            "terminal"
        };
        let error_code = if entry.has_usable_source {
            None
        } else {
            Some("no_usable_content_source")
        };
        let row: Option<(Uuid,)> = sqlx::query_as(
            "INSERT INTO rss_full_content_entries
                 (id, source_id, activation_generation, external_key, title,
                  article_url, summary, embedded_html, doc_time, state, error_code)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (source_id, activation_generation, external_key) DO NOTHING
             RETURNING id",
        )
        .bind(Uuid::now_v7())
        .bind(source_id)
        .bind(generation)
        .bind(&entry.external_key)
        .bind(&entry.title)
        .bind(&entry.article_url)
        .bind(&entry.summary)
        .bind(&entry.embedded_html)
        .bind(entry.doc_time)
        .bind(state)
        .bind(error_code)
        .fetch_optional(&mut *tx)
        .await?;
        if row.is_some() {
            stats.discovered += 1;
            stats.terminal += usize::from(!entry.has_usable_source);
            continue;
        }

        sqlx::query(
            "UPDATE rss_full_content_entries
                SET title = $4, article_url = $5, summary = $6,
                    embedded_html = $7, doc_time = $8,
                    state = CASE
                        WHEN state = 'terminal'
                         AND error_code IN ('no_usable_content_source', 'document_deleted')
                         AND $9 THEN 'pending'
                        ELSE state END,
                    error_code = CASE
                        WHEN state = 'terminal'
                         AND error_code IN ('no_usable_content_source', 'document_deleted')
                         AND $9 THEN NULL
                        ELSE error_code END,
                    error_detail = CASE
                        WHEN state = 'terminal'
                         AND error_code IN ('no_usable_content_source', 'document_deleted')
                         AND $9 THEN NULL
                        ELSE error_detail END,
                    updated_at = now()
              WHERE source_id = $1 AND activation_generation = $2
                AND external_key = $3",
        )
        .bind(source_id)
        .bind(generation)
        .bind(&entry.external_key)
        .bind(&entry.title)
        .bind(&entry.article_url)
        .bind(&entry.summary)
        .bind(&entry.embedded_html)
        .bind(entry.doc_time)
        .bind(entry.has_usable_source)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        "UPDATE rss_full_content_sources
            SET last_discovery_at = now(), last_discovery_count = $2,
                updated_at = now()
          WHERE source_id = $1 AND activation_generation = $3
            AND activation_state = 'active'",
    )
    .bind(source_id)
    .bind(entries.len() as i32)
    .bind(generation)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(stats)
}

/// Claim pending entries and create their jobs in one transaction. Locking the
/// activation row serializes capacity calculation for one source while
/// `SKIP LOCKED` keeps separate sources independent.
pub async fn claim_pending_and_enqueue(
    pool: &PgPool,
    source_id: Uuid,
    generation: i32,
    max_inflight: i64,
    max_attempts: i32,
) -> AppResult<usize> {
    let mut tx = pool.begin().await?;
    let activation: Option<(String, i32)> = sqlx::query_as(
        "SELECT activation_state, activation_generation
           FROM rss_full_content_sources
          WHERE source_id = $1
          FOR UPDATE",
    )
    .bind(source_id)
    .fetch_optional(&mut *tx)
    .await?;
    if activation != Some(("active".to_string(), generation)) {
        tx.commit().await?;
        return Ok(0);
    }

    let (live,): (i64,) = sqlx::query_as(
        "SELECT count(*)
           FROM rss_full_content_entries
          WHERE source_id = $1 AND activation_generation = $2
            AND state IN ('queued', 'hydrating', 'retry_wait')",
    )
    .bind(source_id)
    .bind(generation)
    .fetch_one(&mut *tx)
    .await?;
    let capacity = max_inflight.saturating_sub(live);
    if capacity == 0 {
        tx.commit().await?;
        return Ok(0);
    }

    let ids: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT id
           FROM rss_full_content_entries
          WHERE source_id = $1 AND activation_generation = $2
            AND state = 'pending'
          ORDER BY first_seen_at, id
          FOR UPDATE SKIP LOCKED
          LIMIT $3",
    )
    .bind(source_id)
    .bind(generation)
    .bind(capacity)
    .fetch_all(&mut *tx)
    .await?;

    let mut queued = 0usize;
    for (entry_id,) in ids {
        let job_id = crate::jobs::enqueue_with_max_attempts_tx(
            &mut tx,
            HYDRATION_JOB_KIND,
            serde_json::json!({ "rss_entry_id": entry_id, "source_id": source_id }),
            max_attempts,
        )
        .await?;
        sqlx::query(
            "UPDATE rss_full_content_entries
                SET state = 'queued', hydration_job_id = $2, updated_at = now()
              WHERE id = $1 AND state = 'pending'",
        )
        .bind(entry_id)
        .bind(job_id)
        .execute(&mut *tx)
        .await?;
        queued += 1;
    }

    sqlx::query(
        "UPDATE rss_full_content_sources
            SET last_queued_count = $2, updated_at = now()
          WHERE source_id = $1 AND activation_generation = $3",
    )
    .bind(source_id)
    .bind(queued as i32)
    .bind(generation)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(queued)
}

pub async fn get_entry(pool: &PgPool, entry_id: Uuid) -> AppResult<Option<Entry>> {
    Ok(sqlx::query_as(
        "SELECT id, source_id, activation_generation, external_key, title,
                article_url, summary, embedded_html, doc_time, state,
                hydration_job_id, attempt_count, content_source, final_url,
                content_sha256, document_id, error_code, error_detail,
                first_seen_at, updated_at, completed_at
           FROM rss_full_content_entries
          WHERE id = $1",
    )
    .bind(entry_id)
    .fetch_optional(pool)
    .await?)
}

/// Mark the start of one handler attempt. The queue job ID is the durable
/// claim fence: a replay or stale worker cannot take over a newer claim.
pub async fn mark_hydrating(
    pool: &PgPool,
    entry_id: Uuid,
    hydration_job_id: i64,
) -> AppResult<Option<i32>> {
    let row: Option<(i32,)> = sqlx::query_as(
        "UPDATE rss_full_content_entries AS e
            SET state = 'hydrating', attempt_count = attempt_count + 1,
                updated_at = now()
          WHERE e.id = $1
            AND e.hydration_job_id = $2
            AND e.state IN ('queued', 'retry_wait', 'hydrating')
            AND EXISTS (
                SELECT 1 FROM rss_full_content_sources s
                 WHERE s.source_id = e.source_id
                   AND s.activation_generation = e.activation_generation
                   AND s.activation_state = 'active'
            )
          RETURNING e.attempt_count",
    )
    .bind(entry_id)
    .bind(hydration_job_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(attempt_count,)| attempt_count))
}

/// Retire a queued/in-flight claim after its source generation is no longer
/// current. The job ID fence makes this a no-op for a superseded worker.
pub async fn retire_stale_claim(
    pool: &PgPool,
    entry_id: Uuid,
    hydration_job_id: i64,
    source_id: Uuid,
    generation: i32,
) -> AppResult<bool> {
    let result = sqlx::query(
        "UPDATE rss_full_content_entries AS e
            SET state = 'terminal', error_code = 'stale_activation',
                error_detail = 'RSS full-content activation is no longer current',
                completed_at = NULL, updated_at = now()
          WHERE e.id = $1
            AND e.source_id = $3
            AND e.activation_generation = $4
            AND e.hydration_job_id = $2
            AND e.state IN ('queued', 'retry_wait', 'hydrating')
            AND NOT EXISTS (
                SELECT 1 FROM rss_full_content_sources s
                 WHERE s.source_id = e.source_id
                   AND s.activation_generation = e.activation_generation
                   AND s.activation_state = 'active'
            )",
    )
    .bind(entry_id)
    .bind(hydration_job_id)
    .bind(source_id)
    .bind(generation)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Commit a fetched artifact and its document projection only while the
/// activation and job claim are still current. Document creation/version/job
/// insertion and ledger completion share one transaction, so a stale worker
/// cannot publish a document before discovering that its claim was revoked.
#[allow(clippy::too_many_arguments)]
pub async fn complete_hydration(
    pool: &PgPool,
    entry_id: Uuid,
    hydration_job_id: i64,
    source_id: Uuid,
    generation: i32,
    kb_id: Uuid,
    external_key: &str,
    filename: &str,
    mime: &str,
    size_bytes: i64,
    content_sha256: &str,
    doc_time: Option<DateTime<Utc>>,
    content_source: &str,
    final_url: Option<&str>,
) -> AppResult<Option<Uuid>> {
    let mut tx = pool.begin().await?;
    let activation: Option<(String, i32)> = sqlx::query_as(
        "SELECT activation_state, activation_generation
           FROM rss_full_content_sources
          WHERE source_id = $1
          FOR UPDATE",
    )
    .bind(source_id)
    .fetch_optional(&mut *tx)
    .await?;
    if activation != Some(("active".to_string(), generation)) {
        let _ = retire_stale_claim_tx(&mut tx, entry_id, hydration_job_id, source_id, generation)
            .await?;
        tx.commit().await?;
        return Ok(None);
    }

    let claim: Option<(Uuid, i32, String, Option<i64>)> = sqlx::query_as(
        "SELECT source_id, activation_generation, state, hydration_job_id
           FROM rss_full_content_entries
          WHERE id = $1
          FOR UPDATE",
    )
    .bind(entry_id)
    .fetch_optional(&mut *tx)
    .await?;
    let current = claim.is_some_and(|(actual_source, actual_generation, state, job_id)| {
        actual_source == source_id
            && actual_generation == generation
            && state == "hydrating"
            && job_id == Some(hydration_job_id)
    });
    if !current {
        tx.commit().await?;
        return Ok(None);
    }

    let document = crate::documents::upsert_source_document_tx(
        &mut tx,
        kb_id,
        source_id,
        external_key,
        filename,
        mime,
        size_bytes,
        content_sha256,
        doc_time,
    )
    .await?;
    let result = sqlx::query(
        "UPDATE rss_full_content_entries
            SET state = 'complete', content_source = $2, final_url = $3,
                content_sha256 = $4, document_id = $5, error_code = NULL,
                error_detail = NULL, completed_at = now(), updated_at = now()
          WHERE id = $1 AND source_id = $6 AND activation_generation = $7
            AND state = 'hydrating' AND hydration_job_id = $8",
    )
    .bind(entry_id)
    .bind(content_source)
    .bind(final_url)
    .bind(content_sha256)
    .bind(document.id)
    .bind(source_id)
    .bind(generation)
    .bind(hydration_job_id)
    .execute(&mut *tx)
    .await?;
    if result.rows_affected() != 1 {
        tx.rollback().await?;
        return Ok(None);
    }
    tx.commit().await?;
    Ok(Some(document.id))
}

async fn retire_stale_claim_tx(
    tx: &mut Transaction<'_, Postgres>,
    entry_id: Uuid,
    hydration_job_id: i64,
    source_id: Uuid,
    generation: i32,
) -> AppResult<bool> {
    let result = sqlx::query(
        "UPDATE rss_full_content_entries AS e
            SET state = 'terminal', error_code = 'stale_activation',
                error_detail = 'RSS full-content activation is no longer current',
                completed_at = NULL, updated_at = now()
          WHERE e.id = $1
            AND e.source_id = $3
            AND e.activation_generation = $4
            AND e.hydration_job_id = $2
            AND e.state IN ('queued', 'retry_wait', 'hydrating')",
    )
    .bind(entry_id)
    .bind(hydration_job_id)
    .bind(source_id)
    .bind(generation)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn mark_retry_wait(
    pool: &PgPool,
    entry_id: Uuid,
    hydration_job_id: i64,
    error_code: &str,
    error_detail: &str,
) -> AppResult<bool> {
    set_failure(
        pool,
        entry_id,
        hydration_job_id,
        "retry_wait",
        error_code,
        error_detail,
    )
    .await
}

pub async fn mark_terminal(
    pool: &PgPool,
    entry_id: Uuid,
    hydration_job_id: i64,
    error_code: &str,
    error_detail: &str,
) -> AppResult<bool> {
    set_failure(
        pool,
        entry_id,
        hydration_job_id,
        "terminal",
        error_code,
        error_detail,
    )
    .await
}

async fn set_failure(
    pool: &PgPool,
    entry_id: Uuid,
    hydration_job_id: i64,
    state: &str,
    error_code: &str,
    error_detail: &str,
) -> AppResult<bool> {
    let result = sqlx::query(
        "UPDATE rss_full_content_entries AS e
            SET state = $2, error_code = $3, error_detail = $4,
                updated_at = now()
          WHERE e.id = $1 AND e.hydration_job_id = $5
            AND e.state IN ('queued', 'retry_wait', 'hydrating')
            AND EXISTS (
                SELECT 1 FROM rss_full_content_sources s
                 WHERE s.source_id = e.source_id
                   AND s.activation_generation = e.activation_generation
                   AND s.activation_state = 'active'
            )",
    )
    .bind(entry_id)
    .bind(state)
    .bind(bound(error_code, 64))
    .bind(bound(error_detail, 2048))
    .bind(hydration_job_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Preserve the discovery ledger when a linked document is deleted. A complete
/// row cannot keep the `complete` invariant with a NULL document_id, so it is
/// explicitly downgraded to a visible terminal outcome before the FK action.
pub(crate) async fn detach_document_tx(
    tx: &mut Transaction<'_, Postgres>,
    document_id: Uuid,
) -> AppResult<u64> {
    let result = sqlx::query(
        "UPDATE rss_full_content_entries
            SET state = 'terminal', document_id = NULL, completed_at = NULL,
                error_code = 'document_deleted',
                error_detail = 'accepted document was deleted; hydration can be retried',
                updated_at = now()
          WHERE document_id = $1",
    )
    .bind(document_id)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected())
}

pub async fn list_current_entries(
    pool: &PgPool,
    source_id: Uuid,
    limit: i64,
) -> AppResult<Vec<Entry>> {
    let limit = limit.clamp(1, 100);
    Ok(sqlx::query_as::<_, Entry>(
        r#"
        SELECT id, source_id, activation_generation, external_key, title,
               article_url, summary, embedded_html, doc_time, state,
               hydration_job_id, attempt_count, content_source, final_url,
               content_sha256, document_id, error_code, error_detail,
               first_seen_at, updated_at, completed_at
        FROM rss_full_content_entries
        WHERE source_id = $1
          AND activation_generation = (
              SELECT activation_generation
              FROM rss_full_content_sources
              WHERE source_id = $1
          )
        ORDER BY updated_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(source_id)
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

pub async fn counts(pool: &PgPool, source_id: Uuid) -> AppResult<Option<Counts>> {
    Ok(sqlx::query_as(
        "SELECT s.activation_state, s.activation_generation, s.baseline_count,
                s.last_discovery_at, s.last_discovery_count, s.last_queued_count,
                count(e.id) FILTER (WHERE e.state = 'pending'),
                count(e.id) FILTER (WHERE e.state = 'queued'),
                count(e.id) FILTER (WHERE e.state = 'hydrating'),
                count(e.id) FILTER (WHERE e.state = 'retry_wait'),
                count(e.id) FILTER (WHERE e.state = 'complete'),
                count(e.id) FILTER (WHERE e.state = 'terminal')
           FROM rss_full_content_sources s
           LEFT JOIN rss_full_content_entries e
             ON e.source_id = s.source_id
            AND e.activation_generation = s.activation_generation
          WHERE s.source_id = $1
          GROUP BY s.activation_state, s.activation_generation, s.baseline_count,
                   s.last_discovery_at, s.last_discovery_count, s.last_queued_count",
    )
    .bind(source_id)
    .fetch_optional(pool)
    .await?)
}

fn bound(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::bound;

    #[test]
    fn error_text_is_bounded_without_splitting_utf8() {
        let text = "é".repeat(2_000);
        let bounded = bound(&text, 2_047);
        assert!(bounded.len() <= 2_047);
        assert!(bounded.is_char_boundary(bounded.len()));
    }
}
