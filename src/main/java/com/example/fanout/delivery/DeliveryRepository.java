package com.example.fanout.delivery;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Short database transactions lease work; no transaction is held while a webhook is called. */
@Repository
public class DeliveryRepository {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final DeliveryProperties properties;
    private final RetryPolicy retryPolicy;

    public DeliveryRepository(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
            DeliveryProperties properties, RetryPolicy retryPolicy) {
        this.jdbc = jdbc;
        this.transactions = new TransactionTemplate(transactionManager);
        this.properties = properties;
        this.retryPolicy = retryPolicy;
    }

    public Optional<ClaimedDelivery> claim() {
        return transactions.execute(status -> {
            List<ClaimedDelivery> due = jdbc.query("""
                    SELECT d.id, d.event_id, d.subscription_id, d.replay_id, d.event_sequence,
                           d.attempt_count, d.lease_token, e.type, e.source, e.payload::text AS payload,
                           e.accepted_at, s.webhook_url, s.signing_secret
                    FROM deliveries d
                    JOIN events e ON e.id = d.event_id
                    JOIN subscriptions s ON s.id = d.subscription_id
                    WHERE d.status = 'PENDING' AND d.next_attempt_at <= now()
                      AND (d.lease_until IS NULL OR d.lease_until < now())
                      AND (NOT s.ordered OR (
                        NOT EXISTS (
                          SELECT 1 FROM deliveries previous
                          WHERE previous.subscription_id = d.subscription_id AND previous.source = d.source
                            AND previous.status = 'PENDING'
                            AND (previous.queue_sequence, previous.event_sequence, previous.seq)
                                < (d.queue_sequence, d.event_sequence, d.seq)
                        )
                        AND NOT EXISTS (
                          SELECT 1 FROM events earlier
                          JOIN event_subscriptions snapshot ON snapshot.event_id = earlier.id
                          WHERE snapshot.subscription_id = d.subscription_id AND earlier.source = d.source
                            AND earlier.seq < d.queue_sequence AND NOT earlier.fanout_completed
                        )
                      ))
                    ORDER BY d.queue_sequence, d.event_sequence, d.seq
                    LIMIT 1 FOR UPDATE OF d SKIP LOCKED
                    """, (rs, row) -> new ClaimedDelivery(rs.getObject("id", UUID.class),
                    rs.getObject("event_id", UUID.class), rs.getObject("subscription_id", UUID.class),
                    rs.getObject("replay_id", UUID.class), rs.getLong("event_sequence"),
                    rs.getInt("attempt_count"), rs.getObject("lease_token", UUID.class),
                    rs.getString("type"), rs.getString("source"), rs.getString("payload"),
                    rs.getTimestamp("accepted_at").toInstant(), rs.getString("webhook_url"),
                    rs.getString("signing_secret")));
            if (due.isEmpty()) {
                return Optional.empty();
            }
            ClaimedDelivery previous = due.getFirst();
            if (previous.leaseToken() != null) {
                jdbc.update("""
                        UPDATE delivery_attempts SET outcome = 'UNKNOWN', finished_at = now(),
                            error = 'Worker lease expired; remote outcome is unknown'
                        WHERE delivery_id = ? AND attempt_number = ? AND outcome = 'IN_PROGRESS'
                        """, previous.id(), previous.attempt());
            }
            if (previous.attempt() >= properties.getMaxAttempts()) {
                jdbc.update("""
                        UPDATE deliveries SET status = 'FAILED', lease_token = NULL, lease_until = NULL,
                            updated_at = now(), last_error = 'Retry limit exhausted after an uncertain attempt'
                        WHERE id = ?
                        """, previous.id());
                return Optional.empty();
            }
            UUID token = UUID.randomUUID();
            int attempt = previous.attempt() + 1;
            jdbc.update("""
                    UPDATE deliveries SET attempt_count = ?, lease_token = ?,
                        lease_until = now() + (? * interval '1 millisecond'), updated_at = now()
                    WHERE id = ?
                    """, attempt, token, properties.getLease().toMillis(), previous.id());
            jdbc.update("""
                    INSERT INTO delivery_attempts(delivery_id, attempt_number, started_at, outcome)
                    VALUES (?, ?, now(), 'IN_PROGRESS')
                    """, previous.id(), attempt);
            return Optional.of(previous.withLease(attempt, token));
        });
    }

    /** Returns false for a stale worker whose lease has already been reclaimed. */
    public boolean complete(ClaimedDelivery delivery, WebhookTransport.Result result) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            List<Instant> ownership = jdbc.query("""
                    SELECT now() AS database_now FROM deliveries
                    WHERE id = ? AND lease_token = ? AND status = 'PENDING' FOR UPDATE
                    """, (rs, row) -> rs.getTimestamp("database_now").toInstant(), delivery.id(), delivery.leaseToken());
            if (ownership.isEmpty()) {
                return false;
            }
            boolean retry = !result.delivered()
                    && retryPolicy.shouldRetry(delivery.attempt(), result.httpStatus(), result.permanentFailure());
            String nextStatus = result.delivered() ? "DELIVERED" : retry ? "PENDING" : "FAILED";
            String outcome = result.delivered() ? "DELIVERED" : retry ? "RETRY" : "FAILED";
            long delayMillis = retry ? retryPolicy.delay(delivery.attempt(), result.retryAfter(), ownership.getFirst()).toMillis() : 0;
            jdbc.update("""
                    UPDATE delivery_attempts SET finished_at = now(), http_status = ?, error = ?, outcome = ?
                    WHERE delivery_id = ? AND attempt_number = ? AND outcome = 'IN_PROGRESS'
                    """, result.httpStatus(), result.error(), outcome, delivery.id(), delivery.attempt());
            jdbc.update("""
                    UPDATE deliveries SET status = ?, next_attempt_at = now() + (? * interval '1 millisecond'),
                        lease_token = NULL, lease_until = NULL, last_error = ?, updated_at = now(),
                        delivered_at = CASE WHEN ? = 'DELIVERED' THEN now() ELSE NULL END
                    WHERE id = ? AND lease_token = ?
                    """, nextStatus, delayMillis, result.error(), nextStatus, delivery.id(), delivery.leaseToken());
            return true;
        }));
    }

    public record ClaimedDelivery(UUID id, UUID eventId, UUID subscriptionId, UUID replayId,
            long eventSequence, int attempt, UUID leaseToken, String type, String source, String payload,
            Instant acceptedAt, String webhookUrl, String signingSecret) {
        ClaimedDelivery withLease(int newAttempt, UUID newToken) {
            return new ClaimedDelivery(id, eventId, subscriptionId, replayId, eventSequence, newAttempt,
                    newToken, type, source, payload, acceptedAt, webhookUrl, signingSecret);
        }

        @Override
        public String toString() {
            return "ClaimedDelivery[id=" + id + ", eventId=" + eventId + ", attempt=" + attempt + "]";
        }
    }
}
