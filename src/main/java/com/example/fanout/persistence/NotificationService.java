package com.example.fanout.persistence;

import com.example.fanout.delivery.WebhookPolicy;
import com.example.fanout.domain.EventRequest;
import com.example.fanout.domain.FilterRule;
import com.example.fanout.domain.ReplayRequest;
import com.example.fanout.domain.SubscriptionRequest;
import com.example.fanout.filter.FilterMatcher;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class NotificationService {
    // Serializes only acceptance/registration/replay boundaries across service replicas.
    private static final long ACCEPTANCE_LOCK = 8042026111801L;
    public static final UUID ORIGINAL_GENERATION = new UUID(0, 0);
    private final JdbcTemplate jdbc;
    private final JsonCodec json;
    private final FilterMatcher matcher;
    private final WebhookPolicy webhookPolicy;

    public NotificationService(JdbcTemplate jdbc, JsonCodec json, FilterMatcher matcher, WebhookPolicy webhookPolicy) {
        this.jdbc = jdbc; this.json = json; this.matcher = matcher; this.webhookPolicy = webhookPolicy;
    }

    @Transactional
    public Map<String, Object> ingest(EventRequest request, String idempotencyKey) {
        validateKey(idempotencyKey, false);
        json.validate(request);
        acceptanceLock();
        String hash = json.hash(request);
        if (idempotencyKey != null) {
            var existing = jdbc.queryForList("SELECT id, request_hash FROM events WHERE idempotency_key = ?", idempotencyKey);
            if (!existing.isEmpty()) {
                if (!hash.equals(existing.getFirst().get("request_hash"))) conflict("Idempotency-Key already used for a different event");
                return getEvent((UUID) existing.getFirst().get("id"));
            }
        }
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO events(id, type, source, payload, idempotency_key, request_hash)
                VALUES (?, ?, ?, ?::jsonb, ?, ?)
                """, id, request.type(), request.source(), json.write(request.payload()), idempotencyKey, hash);
        jdbc.update("""
                INSERT INTO event_subscriptions(event_id, subscription_id)
                SELECT ?, id FROM subscriptions WHERE deleted_at IS NULL
                """, id);
        jdbc.update("INSERT INTO outbox(event_id) VALUES (?)", id);
        return getEvent(id);
    }

    @Transactional
    public Map<String, Object> createSubscription(SubscriptionRequest request) {
        json.validate(request);
        webhookPolicy.validate(request.webhookUrl());
        matcher.validate(request.filter());
        acceptanceLock();
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO subscriptions(id, webhook_url, filter, ordered, signing_secret)
                VALUES (?, ?, ?::jsonb, ?, ?)
                """, id, request.webhookUrl(), json.write(request.filter()), request.ordered(), request.signingSecret());
        return one("SELECT * FROM subscriptions WHERE id = ?", this::subscriptionRow, id, "Subscription");
    }

    public Map<String, Object> getSubscription(UUID id) {
        return one("SELECT * FROM subscriptions WHERE id = ?", this::subscriptionRow, id, "Subscription");
    }

    public List<Map<String, Object>> listSubscriptions(int limit, long after) {
        page(limit, after);
        return jdbc.query("SELECT * FROM subscriptions WHERE deleted_at IS NULL AND seq > ? ORDER BY seq LIMIT ?",
                this::subscriptionRow, after, limit);
    }

    @Transactional
    public void deleteSubscription(UUID id) {
        acceptanceLock();
        if (jdbc.update("UPDATE subscriptions SET deleted_at = clock_timestamp() WHERE id = ? AND deleted_at IS NULL", id) == 0) {
            requireExists("subscriptions", id, "Subscription");
        }
    }

    public Map<String, Object> getEvent(UUID id) {
        return one("SELECT * FROM events WHERE id = ?", this::eventRow, id, "Event");
    }
    public Map<String, Object> getDelivery(UUID id) {
        return one("SELECT * FROM deliveries WHERE id = ?", this::deliveryRow, id, "Delivery");
    }
    public List<Map<String, Object>> listDeliveries(UUID eventId, UUID subscriptionId, int limit, long after) {
        page(limit, after);
        if (eventId != null) requireExists("events", eventId, "Event");
        if (subscriptionId != null) requireExists("subscriptions", subscriptionId, "Subscription");
        StringBuilder sql = new StringBuilder("SELECT * FROM deliveries WHERE seq > ?");
        List<Object> args = new ArrayList<>(); args.add(after);
        if (eventId != null) { sql.append(" AND event_id = ?"); args.add(eventId); }
        if (subscriptionId != null) { sql.append(" AND subscription_id = ?"); args.add(subscriptionId); }
        sql.append(" ORDER BY seq LIMIT ?"); args.add(limit);
        return jdbc.query(sql.toString(), this::deliveryRow, args.toArray());
    }
    public List<Map<String, Object>> attempts(UUID deliveryId, int limit, long after) {
        page(limit, after);
        requireExists("deliveries", deliveryId, "Delivery");
        return jdbc.query("SELECT * FROM delivery_attempts WHERE delivery_id = ? AND id > ? ORDER BY id LIMIT ?",
                (rs, row) -> fields(rs, "id", "delivery_id", "attempt_number", "started_at", "finished_at", "http_status", "error", "outcome"),
                deliveryId, after, limit);
    }

    @Transactional
    public Map<String, Object> replay(UUID subscriptionId, ReplayRequest request, String idempotencyKey) {
        validateKey(idempotencyKey, true);
        if (request.from() == null || request.to() == null || !request.from().isBefore(request.to())
                || Duration.between(request.from(), request.to()).compareTo(Duration.ofDays(31)) > 0) {
            throw new IllegalArgumentException("Replay requires from < to and a range of at most 31 days");
        }
        // Stored timestamps are discrete microseconds; round bounds UP to preserve [from,to).
        Instant from = ceilMicros(request.from());
        Instant to = ceilMicros(request.to());
        if (!from.isBefore(to)) throw new IllegalArgumentException("Replay range must be at least one microsecond");
        acceptanceLock();
        var previous = jdbc.query("SELECT * FROM replays WHERE subscription_id = ? AND idempotency_key = ?",
                this::replayRow, subscriptionId, idempotencyKey);
        if (!previous.isEmpty()) {
            var existing = previous.getFirst();
            if (!from.equals(existing.get("from")) || !to.equals(existing.get("to"))) conflict("Idempotency-Key already used for another replay range");
            return existing;
        }
        var subscriptions = jdbc.queryForList("SELECT filter FROM subscriptions WHERE id = ? AND deleted_at IS NULL", subscriptionId);
        if (subscriptions.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Active subscription not found");
        FilterRule filter = json.read(subscriptions.getFirst().get("filter").toString(), FilterRule.class);
        var candidates = jdbc.query("""
                SELECT e.* FROM events e JOIN event_subscriptions es ON es.event_id = e.id
                WHERE es.subscription_id = ? AND e.accepted_at >= ? AND e.accepted_at < ?
                ORDER BY e.seq LIMIT 10001
                """, this::eventRow, subscriptionId, Timestamp.from(from), Timestamp.from(to));
        if (candidates.size() > 10000) throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                "Replay exceeds 10000 eligible events; use a smaller time range");
        UUID replayId = UUID.randomUUID();
        Long queueSequence = jdbc.queryForObject("SELECT nextval('events_seq_seq')", Long.class);
        jdbc.update("INSERT INTO replays(id,subscription_id,idempotency_key,from_time,to_time) VALUES (?,?,?,?,?)",
                replayId, subscriptionId, idempotencyKey, Timestamp.from(from), Timestamp.from(to));
        int count = 0;
        for (var event : candidates) {
            if (matcher.matches(filter, (String) event.get("type"), (String) event.get("source"),
                    (com.fasterxml.jackson.databind.JsonNode) event.get("payload"))) {
                insertDelivery((UUID) event.get("id"), subscriptionId, replayId, (String) event.get("source"),
                        ((Number) event.get("sequence")).longValue(), queueSequence);
                count++;
            }
        }
        jdbc.update("UPDATE replays SET delivery_count = ? WHERE id = ?", count, replayId);
        return one("SELECT * FROM replays WHERE id = ?", this::replayRow, replayId, "Replay");
    }

    void insertDelivery(UUID eventId, UUID subscriptionId, UUID replayId, String source, long eventSequence, long queueSequence) {
        jdbc.update("""
                INSERT INTO deliveries(id,event_id,subscription_id,replay_id,generation,source,event_sequence,queue_sequence)
                VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (event_id,subscription_id,generation) DO NOTHING
                """, UUID.randomUUID(), eventId, subscriptionId, replayId,
                replayId == null ? ORIGINAL_GENERATION : replayId, source, eventSequence, queueSequence);
    }
    private void acceptanceLock() { jdbc.queryForList("SELECT pg_advisory_xact_lock(?)", ACCEPTANCE_LOCK); }
    private void requireExists(String table, UUID id, String name) {
        // Table names are internal constants, never caller input.
        Boolean exists = jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM " + table + " WHERE id = ?)", Boolean.class, id);
        if (!Boolean.TRUE.equals(exists)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, name + " not found");
    }
    private Map<String, Object> one(String sql, RowMapper<Map<String, Object>> mapper, UUID id, String name) {
        var rows = jdbc.query(sql, mapper, id);
        if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, name + " not found");
        return rows.getFirst();
    }
    private static Instant ceilMicros(Instant value) {
        Instant floor = value.truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        try { return floor.equals(value) ? floor : floor.plusNanos(1000); }
        catch (java.time.DateTimeException e) { throw new IllegalArgumentException("Replay timestamp is outside the supported range"); }
    }
    private static void conflict(String reason) { throw new ResponseStatusException(HttpStatus.CONFLICT, reason); }
    private static void page(int limit, long after) {
        if (limit < 1 || limit > 200 || after < 0) throw new IllegalArgumentException("limit must be 1..200 and after must be non-negative");
    }
    private static void validateKey(String key, boolean required) {
        if (key == null && !required) return;
        if (key == null || key.isBlank() || key.length() > 128 || key.chars().anyMatch(c -> c < 33 || c > 126))
            throw new IllegalArgumentException("Idempotency-Key must contain 1..128 printable non-space ASCII characters");
    }
    private Map<String, Object> eventRow(ResultSet rs, int row) throws SQLException {
        var result = fields(rs, "id", "type", "source", "accepted_at", "fanout_completed");
        result.put("sequence", rs.getLong("seq")); result.put("payload", json.read(rs.getString("payload"))); return result;
    }
    private Map<String, Object> subscriptionRow(ResultSet rs, int row) throws SQLException {
        var result = fields(rs, "id", "webhook_url", "ordered", "created_at", "deleted_at");
        result.put("sequence", rs.getLong("seq")); result.put("filter", json.read(rs.getString("filter"))); return result;
    }
    private Map<String, Object> deliveryRow(ResultSet rs, int row) throws SQLException {
        var result = fields(rs, "id", "event_id", "subscription_id", "replay_id", "source", "event_sequence", "queue_sequence", "status",
                "attempt_count", "next_attempt_at", "last_error", "created_at", "updated_at", "delivered_at");
        result.put("sequence", rs.getLong("seq")); return result;
    }
    private Map<String, Object> replayRow(ResultSet rs, int row) throws SQLException {
        var result = fields(rs, "id", "subscription_id", "delivery_count", "created_at");
        result.put("from", rs.getTimestamp("from_time").toInstant()); result.put("to", rs.getTimestamp("to_time").toInstant()); return result;
    }
    private static Map<String, Object> fields(ResultSet rs, String... columns) throws SQLException {
        var result = new LinkedHashMap<String, Object>();
        for (String column : columns) {
            StringBuilder key = new StringBuilder(); boolean upper = false;
            for (char c : column.toCharArray()) {
                if (c == '_') upper = true;
                else { key.append(upper ? Character.toUpperCase(c) : c); upper = false; }
            }
            Object value = rs.getObject(column);
            if (value instanceof Timestamp timestamp) value = timestamp.toInstant();
            result.put(key.toString(), value);
        }
        return result;
    }
}
