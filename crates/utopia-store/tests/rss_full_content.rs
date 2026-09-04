use sqlx::PgPool;
use uuid::Uuid;

async fn seed_source(pool: &PgPool) -> anyhow::Result<(Uuid, Uuid, Uuid, Uuid)> {
    let org_id = Uuid::now_v7();
    let workspace_id = Uuid::now_v7();
    let kb_id = Uuid::now_v7();
    let source_id = Uuid::now_v7();
    sqlx::query("INSERT INTO organizations (id, name) VALUES ($1, $2)")
        .bind(org_id)
        .bind(format!("rss-full-content-test-{org_id}"))
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO workspaces (id, org_id, name) VALUES ($1, $2, $3)")
        .bind(workspace_id)
        .bind(org_id)
        .bind("rss-full-content-test")
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO knowledge_bases (id, workspace_id, name) VALUES ($1, $2, $3)")
        .bind(kb_id)
        .bind(workspace_id)
        .bind("rss-full-content-test")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO sources (id, kb_id, kind, name, config)
         VALUES ($1, $2, 'rss', $3, '{\"feed_url\":\"https://example.com/feed\",\"content_mode\":\"full_new_items\"}'::jsonb)",
    )
    .bind(source_id)
    .bind(kb_id)
    .bind("rss-full-content-test")
    .execute(pool)
    .await?;
    Ok((org_id, kb_id, source_id, workspace_id))
}

fn entry(key: impl Into<String>) -> utopia_store::rss_full_content::NewEntry {
    utopia_store::rss_full_content::NewEntry {
        external_key: key.into(),
        title: "Test entry".into(),
        article_url: Some("https://example.com/article".into()),
        summary: "A useful summary".into(),
        embedded_html: None,
        doc_time: None,
        has_usable_source: true,
    }
}

async fn cleanup(pool: &PgPool, org_id: Uuid) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(org_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn baseline_activation_creates_no_documents_or_hydration_jobs() -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    let pool = PgPool::connect(&url).await?;
    let (org_id, _kb_id, source_id, _workspace_id) = seed_source(&pool).await?;
    sqlx::query(
        "INSERT INTO rss_full_content_sources (source_id, activation_generation, activation_state)
         VALUES ($1, 1, 'pending')",
    )
    .bind(source_id)
    .execute(&pool)
    .await?;

    let entries = vec![entry("baseline-1"), entry("baseline-2")];
    let inserted =
        utopia_store::rss_full_content::record_baseline(&pool, source_id, 1, &entries).await?;
    assert_eq!(inserted, 2);

    let (state, baseline): (String, i32) = sqlx::query_as(
        "SELECT activation_state, baseline_count
           FROM rss_full_content_sources WHERE source_id = $1",
    )
    .bind(source_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(state, "active");
    assert_eq!(baseline, 2);
    let (documents,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM documents WHERE source_id = $1")
            .bind(source_id)
            .fetch_one(&pool)
            .await?;
    let (jobs,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM jobs WHERE kind = 'hydrate_rss_entry' AND payload->>'source_id' = $1",
    )
    .bind(source_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(documents, 0);
    assert_eq!(jobs, 0);
    cleanup(&pool, org_id).await?;
    Ok(())
}

#[tokio::test]
async fn repeated_discovery_is_idempotent_and_claiming_is_capped_at_twenty_five(
) -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    let pool = PgPool::connect(&url).await?;
    let (org_id, _kb_id, source_id, _workspace_id) = seed_source(&pool).await?;
    sqlx::query(
        "INSERT INTO rss_full_content_sources
            (source_id, activation_generation, activation_state, activation_at)
         VALUES ($1, 1, 'active', now())",
    )
    .bind(source_id)
    .execute(&pool)
    .await?;

    let entries: Vec<_> = (0..30)
        .map(|index| entry(format!("entry-{index}")))
        .collect();
    let first = utopia_store::rss_full_content::discover(&pool, source_id, 1, &entries).await?;
    assert_eq!(first.discovered, 30);
    assert_eq!(first.terminal, 0);
    let second = utopia_store::rss_full_content::discover(&pool, source_id, 1, &entries).await?;
    assert_eq!(second.discovered, 0);

    let queued =
        utopia_store::rss_full_content::claim_pending_and_enqueue(&pool, source_id, 1, 25, 5)
            .await?;
    assert_eq!(queued, 25);
    let repeated_claim =
        utopia_store::rss_full_content::claim_pending_and_enqueue(&pool, source_id, 1, 25, 5)
            .await?;
    assert_eq!(repeated_claim, 0);

    let (pending, queued_rows): (i64, i64) = sqlx::query_as(
        "SELECT
            count(*) FILTER (WHERE state = 'pending'),
            count(*) FILTER (WHERE state = 'queued')
         FROM rss_full_content_entries
         WHERE source_id = $1 AND activation_generation = 1",
    )
    .bind(source_id)
    .fetch_one(&pool)
    .await?;
    let (jobs,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM jobs
         WHERE kind = 'hydrate_rss_entry' AND payload->>'source_id' = $1",
    )
    .bind(source_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(pending, 5);
    assert_eq!(queued_rows, 25);
    assert_eq!(jobs, 25);
    cleanup(&pool, org_id).await?;
    Ok(())
}

#[tokio::test]
async fn content_replacement_preserves_document_identity_and_queues_processing_atomically(
) -> anyhow::Result<()> {
    let Some(url) = utopia_store::test_db::url() else {
        return Ok(());
    };
    let pool = PgPool::connect(&url).await?;
    let (org_id, kb_id, source_id, _workspace_id) = seed_source(&pool).await?;
    let document_id = Uuid::now_v7();
    let old_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let new_sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    sqlx::query(
        "INSERT INTO documents
            (id, kb_id, source_id, filename, mime, size_bytes, sha256, external_key)
         VALUES ($1, $2, $3, 'entry.md', 'text/markdown', 3, $4, 'rss-key')",
    )
    .bind(document_id)
    .bind(kb_id)
    .bind(source_id)
    .bind(old_sha)
    .execute(&pool)
    .await?;

    utopia_store::documents::replace_content_and_enqueue_processing(
        &pool,
        document_id,
        "entry.md",
        "text/markdown",
        11,
        new_sha,
        None,
    )
    .await?;

    let (actual_id, actual_sha): (Uuid, String) =
        sqlx::query_as("SELECT id, sha256 FROM documents WHERE id = $1")
            .bind(document_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(actual_id, document_id);
    assert_eq!(actual_sha, new_sha);

    let (versions,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM document_versions WHERE document_id = $1 AND sha256 = $2",
    )
    .bind(document_id)
    .bind(new_sha)
    .fetch_one(&pool)
    .await?;
    assert_eq!(versions, 1);

    let (jobs,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM jobs
         WHERE kind = 'process_document'
           AND payload->>'document_id' = $1",
    )
    .bind(document_id.to_string())
    .fetch_one(&pool)
    .await?;
    assert_eq!(jobs, 1);
    cleanup(&pool, org_id).await?;
    Ok(())
}
