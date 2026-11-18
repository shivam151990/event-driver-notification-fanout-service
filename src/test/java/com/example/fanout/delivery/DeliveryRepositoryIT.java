package com.example.fanout.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class DeliveryRepositoryIT {
    @Container
    private static final PostgreSQLContainer<?> DATABASE = new PostgreSQLContainer<>("postgres:17.6");
    private static JdbcTemplate jdbc;
    private static DataSourceTransactionManager transactionManager;
    private DeliveryRepository repository;
    private DeliveryProperties properties;

    @BeforeAll
    static void database() {
        var dataSource = new DriverManagerDataSource(DATABASE.getJdbcUrl(), DATABASE.getUsername(), DATABASE.getPassword());
        // Some VM-backed Docker runtimes expose their forwarded port shortly after the
        // container readiness log. Wait for the actual host JDBC path before migration.
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().until(() -> {
            try (var connection = dataSource.getConnection()) { return connection.isValid(2); }
        });
        Flyway.configure().dataSource(dataSource).load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        transactionManager = new DataSourceTransactionManager(dataSource);
    }

    @BeforeEach
    void reset() {
        jdbc.execute("TRUNCATE delivery_attempts, deliveries, replays, outbox, event_subscriptions, events, subscriptions RESTART IDENTITY CASCADE");
        properties = new DeliveryProperties();
        properties.setMaxAttempts(3);
        properties.setTimeout(Duration.ofMillis(100));
        properties.setLease(Duration.ofSeconds(6));
        repository = new DeliveryRepository(jdbc, transactionManager, properties,
                new RetryPolicy(Duration.ofMillis(1), Duration.ofSeconds(1), properties.getMaxAttempts()));
    }

    @Test
    void concurrentWorkersClaimEveryUnorderedDeliveryExactlyOnce() throws Exception {
        UUID subscriber = subscription(false);
        for (int index = 0; index < 12; index++) {
            Event event = event("source", true);
            delivery(event, subscriber, event.sequence());
        }
        List<DeliveryRepository.ClaimedDelivery> claimed = concurrentClaims(12);
        assertThat(claimed).hasSize(12);
        assertThat(claimed).extracting(DeliveryRepository.ClaimedDelivery::id).doesNotHaveDuplicates();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_attempts WHERE outcome = 'IN_PROGRESS'", Integer.class))
                .isEqualTo(12);
        assertThat(repository.claim()).isEmpty();
    }

    @Test
    void concurrentWorkersCannotOvertakeAnOrderedInFlightPredecessor() throws Exception {
        UUID subscriber = subscription(true);
        List<UUID> expected = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            Event event = event("source", true);
            expected.add(delivery(event, subscriber, event.sequence()));
        }
        List<DeliveryRepository.ClaimedDelivery> claimed = concurrentClaims(8);
        assertThat(claimed).hasSize(1);
        assertThat(claimed.getFirst().id()).isEqualTo(expected.getFirst());
        assertThat(repository.complete(claimed.getFirst(), success())).isTrue();
        assertThat(repository.claim()).get().extracting(DeliveryRepository.ClaimedDelivery::id).isEqualTo(expected.get(1));
    }

    @Test
    void pendingBackoffBlocksOnlyTheSameSubscriberAndSource() {
        UUID subscriber = subscription(true);
        Event first = event("source-a", true);
        UUID firstDelivery = delivery(first, subscriber, first.sequence());
        Event second = event("source-a", true);
        delivery(second, subscriber, second.sequence());
        Event independent = event("source-b", true);
        UUID independentDelivery = delivery(independent, subscriber, independent.sequence());
        jdbc.update("UPDATE deliveries SET next_attempt_at = now() + interval '1 hour' WHERE id = ?", firstDelivery);
        assertThat(repository.claim()).get().extracting(DeliveryRepository.ClaimedDelivery::id).isEqualTo(independentDelivery);
        assertThat(repository.claim()).isEmpty();
    }

    @Test
    void unfinishedEarlierFanoutBlocksLaterDeliveryEvenBeforeEarlierDeliveryExists() {
        UUID subscriber = subscription(true);
        Event earlier = event("source", false);
        jdbc.update("INSERT INTO event_subscriptions(event_id, subscription_id) VALUES (?, ?)", earlier.id(), subscriber);
        Event later = event("source", true);
        UUID laterDelivery = delivery(later, subscriber, later.sequence());
        assertThat(repository.claim()).isEmpty();
        jdbc.update("UPDATE events SET fanout_completed = TRUE WHERE id = ?", earlier.id());
        assertThat(repository.claim()).get().extracting(DeliveryRepository.ClaimedDelivery::id).isEqualTo(laterDelivery);
    }

    @Test
    void eventSequenceOrdersOriginalsEvenWhenFanoutInsertionWasReversed() {
        UUID subscriber = subscription(true);
        Event first = event("source", true);
        Event second = event("source", true);
        UUID secondDelivery = delivery(second, subscriber, second.sequence());
        UUID firstDelivery = delivery(first, subscriber, first.sequence());
        var claimedFirst = repository.claim().orElseThrow();
        assertThat(claimedFirst.id()).isEqualTo(firstDelivery);
        repository.complete(claimedFirst, success());
        assertThat(repository.claim()).get().extracting(DeliveryRepository.ClaimedDelivery::id).isEqualTo(secondDelivery);
    }

    @Test
    void replayQueueSequenceAppendsOldEventsAndBlocksOnEarlierUnfinishedOriginals() {
        UUID subscriber = subscription(true);
        Event old = event("source", true);
        Event pendingFanout = event("source", false);
        jdbc.update("INSERT INTO event_subscriptions(event_id, subscription_id) VALUES (?, ?)", pendingFanout.id(), subscriber);
        long replayQueue = jdbc.queryForObject("SELECT nextval('events_seq_seq')", Long.class);
        UUID replayDelivery = delivery(old, subscriber, replayQueue);
        Event newer = event("source", true);
        UUID newerDelivery = delivery(newer, subscriber, newer.sequence());
        assertThat(repository.claim()).isEmpty();
        UUID previouslyAcceptedDelivery = delivery(pendingFanout, subscriber, pendingFanout.sequence());
        jdbc.update("UPDATE events SET fanout_completed = TRUE WHERE id = ?", pendingFanout.id());
        var firstClaim = repository.claim().orElseThrow();
        assertThat(firstClaim.id()).isEqualTo(previouslyAcceptedDelivery);
        repository.complete(firstClaim, success());
        var replayClaim = repository.claim().orElseThrow();
        assertThat(replayClaim.id()).isEqualTo(replayDelivery);
        repository.complete(replayClaim, success());
        assertThat(repository.claim()).get().extracting(DeliveryRepository.ClaimedDelivery::id).isEqualTo(newerDelivery);
    }

    @Test
    void expiredLeaseRecordsUncertaintyAndFencesTheOldWorker() {
        UUID subscriber = subscription(false);
        Event event = event("source", true);
        UUID deliveryId = delivery(event, subscriber, event.sequence());
        var oldClaim = repository.claim().orElseThrow();
        expire(deliveryId);
        var replacement = repository.claim().orElseThrow();
        assertThat(replacement.attempt()).isEqualTo(2);
        assertThat(replacement.leaseToken()).isNotEqualTo(oldClaim.leaseToken());
        assertThat(repository.complete(oldClaim, success())).isFalse();
        assertThat(jdbc.queryForObject("SELECT outcome FROM delivery_attempts WHERE delivery_id = ? AND attempt_number = 1",
                String.class, deliveryId)).isEqualTo("UNKNOWN");
        assertThat(repository.complete(replacement, success())).isTrue();
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, deliveryId)).isEqualTo("DELIVERED");
        assertThat(jdbc.queryForObject("SELECT outcome FROM delivery_attempts WHERE delivery_id = ? AND attempt_number = 2",
                String.class, deliveryId)).isEqualTo("DELIVERED");
    }

    @Test
    void repeatedCrashesStillRespectTheAttemptLimitAndUnblockOrderedQueue() {
        UUID subscriber = subscription(true);
        Event first = event("source", true);
        UUID firstDelivery = delivery(first, subscriber, first.sequence());
        Event second = event("source", true);
        UUID secondDelivery = delivery(second, subscriber, second.sequence());
        for (int attempt = 1; attempt <= 3; attempt++) {
            var claim = repository.claim().orElseThrow();
            assertThat(claim.id()).isEqualTo(firstDelivery);
            assertThat(claim.attempt()).isEqualTo(attempt);
            expire(firstDelivery);
        }
        assertThat(repository.claim()).isEmpty();
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, firstDelivery)).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM delivery_attempts WHERE delivery_id = ? AND outcome = 'UNKNOWN'",
                Integer.class, firstDelivery)).isEqualTo(3);
        assertThat(repository.claim()).get().extracting(DeliveryRepository.ClaimedDelivery::id).isEqualTo(secondDelivery);
    }

    @Test
    void completesRetryFailureAndSuccessWithDurableStatusAndHttpCodes() {
        UUID subscriber = subscription(false);
        Event event = event("source", true);
        UUID deliveryId = delivery(event, subscriber, event.sequence());
        for (int attempt = 1; attempt <= 3; attempt++) {
            jdbc.update("UPDATE deliveries SET next_attempt_at = now() WHERE id = ?", deliveryId);
            var claim = repository.claim().orElseThrow();
            assertThat(repository.complete(claim, new WebhookTransport.Result(503, "Webhook returned HTTP 503", "1", false))).isTrue();
        }
        assertThat(jdbc.queryForList("SELECT outcome FROM delivery_attempts WHERE delivery_id = ? ORDER BY attempt_number",
                String.class, deliveryId)).containsExactly("RETRY", "RETRY", "FAILED");
        assertThat(jdbc.queryForList("SELECT http_status FROM delivery_attempts WHERE delivery_id = ? ORDER BY attempt_number",
                Integer.class, deliveryId)).containsExactly(503, 503, 503);
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, deliveryId)).isEqualTo("FAILED");
        assertThat(repository.claim()).isEmpty();
    }

    @Test
    void policyRejectionIsTerminalAndDeletingSubscriptionDoesNotCancelCapturedWork() {
        UUID subscriber = subscription(false);
        Event event = event("source", true);
        UUID deliveryId = delivery(event, subscriber, event.sequence());
        jdbc.update("UPDATE subscriptions SET deleted_at = now() WHERE id = ?", subscriber);
        var claim = repository.claim().orElseThrow();
        repository.complete(claim, new WebhookTransport.Result(null, "Blocked endpoint", null, true));
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, deliveryId)).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT attempt_count FROM deliveries WHERE id = ?", Integer.class, deliveryId)).isEqualTo(1);
    }

    private List<DeliveryRepository.ClaimedDelivery> concurrentClaims(int count) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(count)) {
            List<Future<Optional<DeliveryRepository.ClaimedDelivery>>> futures = new ArrayList<>();
            for (int index = 0; index < count; index++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return repository.claim();
                }));
            }
            start.countDown();
            List<DeliveryRepository.ClaimedDelivery> result = new ArrayList<>();
            for (var future : futures) {
                future.get(10, TimeUnit.SECONDS).ifPresent(result::add);
            }
            return result;
        }
    }

    private UUID subscription(boolean ordered) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO subscriptions(id, webhook_url, filter, ordered, signing_secret)
                VALUES (?, 'https://example.com/webhook', '{}'::jsonb, ?, 'test-signing-secret')
                """, id, ordered);
        return id;
    }

    private Event event(String source, boolean fanoutCompleted) {
        UUID id = UUID.randomUUID();
        long sequence = jdbc.queryForObject("""
                INSERT INTO events(id, type, source, payload, request_hash, fanout_completed)
                VALUES (?, 'example.created', ?, '{"value":1}'::jsonb, ?, ?) RETURNING seq
                """, Long.class, id, source, "0".repeat(64), fanoutCompleted);
        return new Event(id, sequence, source);
    }

    private UUID delivery(Event event, UUID subscriber, long queueSequence) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO deliveries(id, event_id, subscription_id, generation, source, event_sequence, queue_sequence)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, id, event.id(), subscriber, UUID.randomUUID(), event.source(), event.sequence(), queueSequence);
        return id;
    }

    private void expire(UUID deliveryId) {
        jdbc.update("UPDATE deliveries SET lease_until = now() - interval '1 second' WHERE id = ?", deliveryId);
    }

    private static WebhookTransport.Result success() {
        return new WebhookTransport.Result(204, null, null, false);
    }

    private record Event(UUID id, long sequence, String source) { }
}
