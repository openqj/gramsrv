ALTER TABLE private_agent_sessions
    ADD COLUMN title TEXT NOT NULL DEFAULT '',
    ADD COLUMN title_event_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN title_update_delivered BOOLEAN NOT NULL DEFAULT FALSE;
