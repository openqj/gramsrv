CREATE SEQUENCE private_agent_peer_id_seq
    START WITH 4000000000000
    INCREMENT BY 1
    MINVALUE 4000000000000
    MAXVALUE 4000009999999
    NO CYCLE;

CREATE TABLE private_agent_sessions (
    owner_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    peer_id BIGINT NOT NULL UNIQUE DEFAULT nextval('private_agent_peer_id_seq'),
    gateway_session_id TEXT NOT NULL,
    create_idempotency_key TEXT NOT NULL,
    after_version BIGINT NOT NULL DEFAULT 0 CHECK (after_version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_user_id, gateway_session_id),
    UNIQUE (owner_user_id, peer_id),
    UNIQUE (owner_user_id, create_idempotency_key),
    CHECK (peer_id BETWEEN 4000000000000 AND 4000009999999),
    CHECK (gateway_session_id <> ''),
    CHECK (create_idempotency_key <> '')
);

CREATE INDEX private_agent_sessions_owner_activity_idx
    ON private_agent_sessions (owner_user_id, last_activity_at DESC, created_at DESC);

CREATE TABLE private_agent_tasks (
    owner_user_id BIGINT NOT NULL,
    gateway_session_id TEXT NOT NULL,
    gateway_task_id TEXT NOT NULL,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_user_id, gateway_session_id, gateway_task_id),
    UNIQUE (owner_user_id, idempotency_key),
    FOREIGN KEY (owner_user_id, gateway_session_id)
        REFERENCES private_agent_sessions (owner_user_id, gateway_session_id) ON DELETE CASCADE,
    CHECK (gateway_task_id <> '')
);

CREATE TABLE private_agent_message_links (
    owner_user_id BIGINT NOT NULL,
    gateway_session_id TEXT NOT NULL,
    peer_id BIGINT NOT NULL,
    message_box_id INTEGER NOT NULL,
    gateway_task_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    event_type TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_user_id, gateway_session_id, event_id),
    UNIQUE (owner_user_id, message_box_id),
    FOREIGN KEY (owner_user_id, gateway_session_id)
        REFERENCES private_agent_sessions (owner_user_id, gateway_session_id) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id, gateway_session_id, gateway_task_id)
        REFERENCES private_agent_tasks (owner_user_id, gateway_session_id, gateway_task_id) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id, message_box_id)
        REFERENCES message_boxes (owner_user_id, box_id) ON DELETE CASCADE,
    CHECK (peer_id BETWEEN 4000000000000 AND 4000009999999),
    CHECK (event_id <> '')
);

CREATE INDEX private_agent_message_links_task_idx
    ON private_agent_message_links (owner_user_id, gateway_session_id, gateway_task_id, role, message_box_id);
