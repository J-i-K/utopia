-- Keep the existing workspace model selector authoritative while allowing its OpenAI
-- entry to choose deployment-mounted ChatGPT/Codex access for background text work.
ALTER TABLE llm_settings
    ADD COLUMN chat_access_mode TEXT NOT NULL DEFAULT 'api'
        CHECK (chat_access_mode IN ('api', 'subscription'));
