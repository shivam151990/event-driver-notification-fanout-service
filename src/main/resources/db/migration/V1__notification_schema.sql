CREATE TABLE subscriptions (
    id UUID PRIMARY KEY,
    seq BIGSERIAL UNIQUE NOT NULL,
    webhook_url TEXT NOT NULL,
    filter JSONB NOT NULL,
    ordered BOOLEAN NOT NULL DEFAULT FALSE,
    signing_secret TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);
CREATE TABLE events (
    id UUID PRIMARY KEY,
    seq BIGSERIAL UNIQUE NOT NULL,
    type VARCHAR(128) NOT NULL,
    source VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    idempotency_key VARCHAR(128) UNIQUE,
    request_hash VARCHAR(64) NOT NULL,
    fanout_completed BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX events_source_seq_idx ON events(source, seq) WHERE NOT fanout_completed;
CREATE INDEX events_accepted_at_idx ON events(accepted_at, seq);
CREATE TABLE event_subscriptions (
    event_id UUID NOT NULL REFERENCES events(id),
    subscription_id UUID NOT NULL REFERENCES subscriptions(id),
    PRIMARY KEY(event_id, subscription_id)
);
CREATE INDEX event_subscriptions_sub_idx ON event_subscriptions(subscription_id, event_id);
CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE REFERENCES events(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox(id) WHERE published_at IS NULL;
CREATE TABLE replays (
    id UUID PRIMARY KEY,
    subscription_id UUID NOT NULL REFERENCES subscriptions(id),
    idempotency_key VARCHAR(128) NOT NULL,
    from_time TIMESTAMPTZ NOT NULL,
    to_time TIMESTAMPTZ NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(subscription_id, idempotency_key)
);
CREATE TABLE deliveries (
    id UUID PRIMARY KEY,
    seq BIGSERIAL UNIQUE NOT NULL,
    event_id UUID NOT NULL REFERENCES events(id),
    subscription_id UUID NOT NULL REFERENCES subscriptions(id),
    replay_id UUID REFERENCES replays(id),
    generation UUID NOT NULL,
    source VARCHAR(128) NOT NULL,
    event_sequence BIGINT NOT NULL,
    queue_sequence BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_until TIMESTAMPTZ,
    lease_token UUID,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    delivered_at TIMESTAMPTZ,
    UNIQUE(event_id, subscription_id, generation),
    CHECK ((lease_until IS NULL) = (lease_token IS NULL))
);
CREATE INDEX deliveries_due_idx ON deliveries(next_attempt_at, seq) WHERE status = 'PENDING';
CREATE INDEX deliveries_order_idx ON deliveries(subscription_id, source, queue_sequence, event_sequence, seq) WHERE status = 'PENDING';
CREATE INDEX deliveries_event_idx ON deliveries(event_id, seq);
CREATE INDEX deliveries_subscription_idx ON deliveries(subscription_id, seq);
CREATE TABLE delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    delivery_id UUID NOT NULL REFERENCES deliveries(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    http_status INTEGER,
    error TEXT,
    outcome VARCHAR(16) NOT NULL CHECK (outcome IN ('IN_PROGRESS', 'DELIVERED', 'RETRY', 'FAILED', 'UNKNOWN')),
    UNIQUE(delivery_id, attempt_number)
);
CREATE INDEX delivery_attempts_history_idx ON delivery_attempts(delivery_id, id);
