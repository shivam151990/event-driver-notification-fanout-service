package com.example.fanout.broker;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
@ConditionalOnProperty(name = "fanout.broker.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final long RELAY_LOCK = 8042026111802L;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final KafkaTemplate<String, String> kafka;
    private final MeterRegistry metrics;
    private final String topic;
    private final int batchSize;
    public OutboxRelay(JdbcTemplate jdbc, TransactionTemplate transactions, KafkaTemplate<String, String> kafka,
                       MeterRegistry metrics, @Value("${fanout.broker.topic}") String topic,
                       @Value("${fanout.broker.batch-size:50}") int batchSize) {
        if (batchSize < 1 || batchSize > 1000) throw new IllegalArgumentException("Broker batch-size must be 1..1000");
        this.jdbc = jdbc; this.transactions = transactions; this.kafka = kafka;
        this.metrics = metrics; this.topic = topic; this.batchSize = batchSize;
    }
    @Scheduled(fixedDelayString = "${fanout.broker.poll-delay-ms:500}")
    public void publishBatch() {
        try {
            transactions.executeWithoutResult(status -> {
                // One relay at a time preserves publication order across replicas. This lock
                // is separate from acceptance, so broker downtime does not block ingestion.
                Boolean acquired = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean.class, RELAY_LOCK);
                if (!Boolean.TRUE.equals(acquired)) return;
                // Reconcile published-but-unprocessed events if broker retention/data loss or
                // a consumer outage outlives the message. PostgreSQL remains authoritative.
                jdbc.update("""
                        UPDATE outbox o SET published_at = NULL FROM events e
                        WHERE o.event_id = e.id AND NOT e.fanout_completed
                          AND o.published_at < clock_timestamp() - interval '5 minutes'
                        """);
                var rows = jdbc.queryForList("""
                        SELECT o.id, o.event_id, e.source FROM outbox o JOIN events e ON e.id = o.event_id
                        WHERE o.published_at IS NULL ORDER BY o.id LIMIT ? FOR UPDATE OF o
                        """, batchSize);
                for (var row : rows) {
                    try {
                        kafka.send(topic, (String) row.get("source"), row.get("event_id").toString()).get(20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); throw new IllegalStateException("Outbox publish interrupted", e);
                    } catch (Exception e) { throw new IllegalStateException("Outbox publish failed", e); }
                    jdbc.update("UPDATE outbox SET published_at = clock_timestamp() WHERE id = ?", row.get("id"));
                    metrics.counter("fanout.outbox.published").increment();
                }
            });
        } catch (RuntimeException e) {
            metrics.counter("fanout.outbox.failures").increment();
            log.warn("Outbox publication failed; durable rows will be retried ({})", e.getClass().getSimpleName());
        }
    }
}
