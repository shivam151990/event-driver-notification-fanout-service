package com.example.fanout.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "fanout.api-key=integration-test-api-key-01234567890123456789",
        "fanout.delivery.allow-http=true",
        "fanout.delivery.allow-private-addresses=true",
        "fanout.delivery.initial-backoff=150ms",
        "fanout.delivery.max-backoff=300ms",
        "fanout.delivery.max-attempts=3",
        "fanout.delivery.timeout=2s",
        "fanout.delivery.lease=10s",
        "fanout.delivery.poll-delay=25ms",
        "fanout.broker.poll-delay-ms=25"
})
class NotificationServiceIT {
    private static final String API_KEY = "integration-test-api-key-01234567890123456789";
    private static final String SECRET = "integration-test-signing-secret-0123456789";

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("fanout.events.v1", 3, (short) 1))).all().get();
        } catch (Exception exception) {
            throw new IllegalStateException("Could not provision the integration test Kafka topic", exception);
        }
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    private static HttpServer webhook;
    private static ExecutorService webhookExecutor;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    @LocalServerPort
    private int port;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private KafkaTemplate<String, String> kafka;
    @Autowired
    private JdbcTemplate jdbc;

    @BeforeAll
    static void startWebhook() throws Exception {
        webhook = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        webhookExecutor = Executors.newVirtualThreadPerTaskExecutor();
        webhook.setExecutor(webhookExecutor);
        webhook.start();
    }

    @AfterAll
    static void stopWebhook() {
        webhook.stop(0);
        webhookExecutor.close();
    }

    @Test
    void requiresAuthenticationAndRejectsMalformedRequests() throws Exception {
        HttpResponse<String> unauthenticated = client.send(HttpRequest.newBuilder(uri("/subscriptions"))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(unauthenticated.statusCode()).isEqualTo(401);
        assertThat(post("/events", Map.of("type", "", "source", "s", "payload", Map.of()), null).status()).isEqualTo(400);
        assertThat(post("/events", Map.of("type", "t", "source", "s", "payload", Map.of(), "unexpected", true), null).status()).isEqualTo(400);
        assertThat(post("/subscriptions", Map.of("webhookUrl", "file:///etc/passwd", "signingSecret", SECRET,
                "filter", Map.of()), null).status()).isEqualTo(400);
        assertThat(post("/subscriptions", Map.of("webhookUrl", "http://127.0.0.1:1234/hook", "signingSecret", SECRET,
                "filter", Map.of("conditions", List.of(Map.of("path", "/amount", "operator", "GT", "value", "invalid")))), null).status()).isEqualTo(400);
    }

    @Test
    void routesMatchingEventsAndSignsTheExactBodyAndAuditsSuccess() throws Exception {
        String type = unique("matching");
        Endpoint target = endpoint(204);
        JsonNode subscription = subscribe(type, target, false, List.of(Map.of("path", "/amount", "operator", "GTE", "value", 100)));
        JsonNode event = ingest(type, "checkout", Map.of("amount", 149.5), null);
        JsonNode delivery = waitForDelivery(event, subscription, "DELIVERED");

        assertThat(delivery.path("attemptCount").asInt()).isEqualTo(1);
        JsonNode attempts = get("/deliveries/" + delivery.path("id").asText() + "/attempts").body();
        assertThat(attempts.size()).isEqualTo(1);
        assertThat(attempts.get(0).path("httpStatus").asInt()).isEqualTo(204);
        assertThat(attempts.get(0).path("startedAt").asText()).isNotBlank();
        assertThat(attempts.get(0).path("finishedAt").asText()).isNotBlank();
        Captured request = target.requests().getFirst();
        assertThat(request.deliveryId()).isEqualTo(delivery.path("id").asText());
        assertThat(request.eventId()).isEqualTo(event.path("id").asText());
        assertThat(request.signature()).isEqualTo(signature(request.timestamp(), request.body()));
        assertThat(mapper.readTree(request.body()).path("payload").path("amount").asDouble()).isEqualTo(149.5);

        JsonNode nonmatch = ingest(type, "checkout", Map.of("amount", 10), null);
        waitForFanout(nonmatch);
        assertThat(deliveries(nonmatch, subscription).size()).isZero();
    }

    @Test
    void concurrentIdempotentIngestCreatesOneEventAndConflictingReuseFails() throws Exception {
        String key = unique("idempotency");
        Map<String, Object> body = Map.of("type", unique("type"), "source", "producer", "payload", Map.of("n", 1));
        List<String> ids = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var calls = new ArrayList<java.util.concurrent.Callable<Response>>();
            for (int i = 0; i < 8; i++) {
                calls.add(() -> post("/events", body, key));
            }
            for (var future : executor.invokeAll(calls)) {
                Response response = future.get();
                assertThat(response.status()).isEqualTo(202);
                ids.add(response.body().path("id").asText());
            }
        }
        assertThat(ids).doesNotContain("").containsOnly(ids.getFirst());
        assertThat(post("/events", Map.of("type", "different", "source", "producer", "payload", Map.of()), key).status()).isEqualTo(409);
    }

    @Test
    void retriesFailuresWithStableDeliveryIdAndPreservesEveryHttpAttempt() throws Exception {
        String type = unique("retry");
        Endpoint target = endpoint(503, 429, 200);
        JsonNode subscription = subscribe(type, target, false, List.of());
        JsonNode event = ingest(type, unique("source"), Map.of(), null);
        JsonNode delivery = waitForDelivery(event, subscription, "DELIVERED");
        assertThat(delivery.path("attemptCount").asInt()).isEqualTo(3);
        JsonNode attempts = get("/deliveries/" + delivery.path("id").asText() + "/attempts").body();
        assertThat(attempts).hasSize(3);
        assertThat(attempts.get(0).path("httpStatus").asInt()).isEqualTo(503);
        assertThat(attempts.get(1).path("httpStatus").asInt()).isEqualTo(429);
        assertThat(attempts.get(2).path("httpStatus").asInt()).isEqualTo(200);
        assertThat(target.requests()).extracting(Captured::deliveryId).containsOnly(delivery.path("id").asText());
        assertThat(target.requests()).extracting(Captured::attempt).containsExactly("1", "2", "3");
        assertThat(Instant.parse(attempts.get(1).path("startedAt").asText()))
                .isAfter(Instant.parse(attempts.get(0).path("finishedAt").asText()));
    }

    @Test
    void exhaustsRetriesAndKeepsTerminalFailureQueryable() throws Exception {
        String type = unique("exhaust");
        Endpoint target = endpoint(400);
        JsonNode subscription = subscribe(type, target, false, List.of());
        JsonNode event = ingest(type, "producer", Map.of(), null);
        JsonNode delivery = waitForDelivery(event, subscription, "FAILED");
        assertThat(delivery.path("attemptCount").asInt()).isEqualTo(3);
        assertThat(delivery.path("lastError").asText()).isNotBlank();
        JsonNode attempts = get("/deliveries/" + delivery.path("id").asText() + "/attempts").body();
        assertThat(attempts).hasSize(3);
        for (JsonNode attempt : attempts) {
            assertThat(attempt.path("httpStatus").asInt()).isEqualTo(400);
        }
    }

    @Test
    void deletionDoesNotEraseAcceptedWorkAndExcludesFutureEvents() throws Exception {
        String type = unique("delete");
        Endpoint target = endpoint(503, 200);
        JsonNode subscription = subscribe(type, target, false, List.of());
        JsonNode before = ingest(type, "producer", Map.of(), null);
        assertThat(request("DELETE", "/subscriptions/" + subscription.path("id").asText(), null, null).status()).isEqualTo(204);
        waitForDelivery(before, subscription, "DELIVERED");
        JsonNode after = ingest(type, "producer", Map.of(), null);
        waitForFanout(after);
        assertThat(deliveries(after, subscription)).isEmpty();
        assertThat(get("/subscriptions?limit=100").body().findValuesAsText("id"))
                .doesNotContain(subscription.path("id").asText());
    }

    @Test
    void replaysEligibleRetainedEventsWithIdempotencyAndPreservesOriginalHistory() throws Exception {
        String type = unique("replay");
        JsonNode beforeSubscription = ingest(type, "historical", Map.of("excluded", true), null);
        waitForFanout(beforeSubscription);
        Endpoint target = endpoint(503, 503, 503, 200);
        JsonNode subscription = subscribe(type, target, false, List.of());
        JsonNode event = ingest(type, "historical", Map.of("recover", true), null);
        JsonNode original = waitForDelivery(event, subscription, "FAILED");
        Instant accepted = Instant.parse(event.path("acceptedAt").asText());
        Map<String, String> range = Map.of("from", Instant.parse(beforeSubscription.path("acceptedAt").asText()).minusSeconds(1).toString(), "to", accepted.plusSeconds(1).toString());
        String path = "/subscriptions/" + subscription.path("id").asText() + "/replays";
        String key = unique("recovery");
        Response replay = post(path, range, key);
        assertThat(replay.status()).isEqualTo(202);
        assertThat(replay.body().path("deliveryCount").asInt()).isEqualTo(1);
        Response duplicate = post(path, range, key);
        assertThat(duplicate.status()).isEqualTo(202);
        assertThat(duplicate.body().path("id")).isEqualTo(replay.body().path("id"));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            JsonNode history = deliveries(event, subscription);
            assertThat(history).hasSize(2);
            assertThat(history.get(0).path("id")).isEqualTo(original.path("id"));
            assertThat(history.get(0).path("status").asText()).isEqualTo("FAILED");
            assertThat(history.get(1).path("replayId")).isEqualTo(replay.body().path("id"));
            assertThat(history.get(1).path("status").asText()).isEqualTo("DELIVERED");
        });
        assertThat(deliveries(beforeSubscription, subscription)).isEmpty();
        assertThat(post(path, Map.of("from", accepted.minusSeconds(2).toString(), "to", accepted.plusSeconds(2).toString()), key).status()).isEqualTo(409);
    }

    @Test
    void orderedSubscriptionDoesNotOvertakeEarlierRetryInTheSameSource() throws Exception {
        String type = unique("ordering");
        String source = unique("source");
        Endpoint target = endpoint(503, 200, 200);
        JsonNode subscription = subscribe(type, target, true, List.of());
        JsonNode first = ingest(type, source, Map.of("number", 1), null);
        JsonNode second = ingest(type, source, Map.of("number", 2), null);
        waitForDelivery(first, subscription, "DELIVERED");
        waitForDelivery(second, subscription, "DELIVERED");
        assertThat(target.requests()).extracting(Captured::eventId)
                .containsExactly(first.path("id").asText(), first.path("id").asText(), second.path("id").asText());
    }

    @Test
    void duplicateKafkaRecordsDoNotCreateDuplicateOriginalDeliveries() throws Exception {
        String type = unique("broker-duplicate");
        String source = unique("source");
        Endpoint target = endpoint(200);
        JsonNode subscription = subscribe(type, target, false, List.of());
        JsonNode event = ingest(type, source, Map.of(), null);
        waitForDelivery(event, subscription, "DELIVERED");
        kafka.send("fanout.events.v1", source, event.path("id").asText()).get();
        kafka.send("fanout.events.v1", source, event.path("id").asText()).get();
        // A sentinel on the same source/partition proves earlier duplicate records were consumed.
        JsonNode sentinel = ingest(type, source, Map.of("sentinel", true), null);
        waitForDelivery(sentinel, subscription, "DELIVERED");
        assertThat(deliveries(event, subscription)).hasSize(1);
        assertThat(target.requests().stream().filter(r -> r.eventId().equals(event.path("id").asText()))).hasSize(1);
    }

    @Test
    void acceptsDurablyDuringBrokerOutageAndDeliversAfterRecovery() throws Exception {
        String type = unique("broker-outage");
        Endpoint target = endpoint(200);
        JsonNode subscription = subscribe(type, target, false, List.of());
        JsonNode event;
        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
        try {
            event = ingest(type, "producer", Map.of("survives", true), unique("outage"));
            UUID id = UUID.fromString(event.path("id").asText());
            assertThat(jdbc.queryForObject("SELECT count(*) FROM events WHERE id = ?", Integer.class, id)).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE event_id = ? AND published_at IS NULL", Integer.class, id)).isEqualTo(1);
            assertThat(get("/events/" + id).body().path("fanoutCompleted").asBoolean()).isFalse();
        } finally {
            KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }
        waitForDelivery(event, subscription, "DELIVERED");
    }

    @Test
    void rollsBackTheEventIfItsOutboxInsertCannotCommit() throws Exception {
        jdbc.execute("""
                CREATE FUNCTION test_reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF EXISTS (SELECT 1 FROM events WHERE id = NEW.event_id AND type = 'reject-durable-outbox') THEN
                    RAISE EXCEPTION 'injected outbox persistence failure';
                  END IF;
                  RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER test_reject_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION test_reject_outbox()");
        String key = unique("rollback");
        try {
            Response response = post("/events", Map.of("type", "reject-durable-outbox", "source", "producer", "payload", Map.of()), key);
            assertThat(response.status()).isGreaterThanOrEqualTo(500);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM events WHERE idempotency_key = ?", Integer.class, key)).isZero();
        } finally {
            jdbc.execute("DROP TRIGGER test_reject_outbox ON outbox");
            jdbc.execute("DROP FUNCTION test_reject_outbox()");
        }
        // A producer retry can safely accept the same key once persistence is healthy.
        assertThat(post("/events", Map.of("type", "reject-durable-outbox", "source", "producer", "payload", Map.of()), key).status()).isEqualTo(202);
    }

    private JsonNode subscribe(String type, Endpoint target, boolean ordered, List<Map<String, Object>> conditions) throws Exception {
        Response response = post("/subscriptions", Map.of("webhookUrl", target.url(), "signingSecret", SECRET,
                "ordered", ordered, "filter", Map.of("eventTypes", List.of(type), "conditions", conditions)), null);
        assertThat(response.status()).describedAs(response.raw()).isEqualTo(201);
        assertThat(response.body().has("signingSecret")).isFalse();
        return response.body();
    }

    private JsonNode ingest(String type, String source, Map<String, ?> payload, String key) throws Exception {
        Response response = post("/events", Map.of("type", type, "source", source, "payload", payload), key);
        assertThat(response.status()).describedAs(response.raw()).isEqualTo(202);
        return response.body();
    }

    private JsonNode waitForDelivery(JsonNode event, JsonNode subscription, String status) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            JsonNode result = deliveries(event, subscription);
            assertThat(result).hasSize(1);
            assertThat(result.get(0).path("status").asText()).isEqualTo(status);
        });
        try {
            return deliveries(event, subscription).get(0);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void waitForFanout(JsonNode event) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(get("/events/" + event.path("id").asText()).body().path("fanoutCompleted").asBoolean()).isTrue());
    }

    private JsonNode deliveries(JsonNode event, JsonNode subscription) throws Exception {
        return get("/deliveries?eventId=" + event.path("id").asText() + "&subscriptionId=" + subscription.path("id").asText()).body();
    }

    private Endpoint endpoint(int... statuses) {
        String path = "/" + UUID.randomUUID();
        List<Captured> requests = new CopyOnWriteArrayList<>();
        AtomicInteger index = new AtomicInteger();
        webhook.createContext(path, exchange -> {
            try (exchange) {
                byte[] body = exchange.getRequestBody().readAllBytes();
                var headers = exchange.getRequestHeaders();
                requests.add(new Captured(new String(body, StandardCharsets.UTF_8), headers.getFirst("X-Event-Id"),
                        headers.getFirst("X-Delivery-Id"), headers.getFirst("X-Delivery-Attempt"),
                        headers.getFirst("X-Webhook-Timestamp"), headers.getFirst("X-Webhook-Signature")));
                int response = statuses[Math.min(index.getAndIncrement(), statuses.length - 1)];
                exchange.sendResponseHeaders(response, -1);
            }
        });
        return new Endpoint("http://127.0.0.1:" + webhook.getAddress().getPort() + path, requests);
    }

    private Response get(String path) throws Exception {
        Response response = request("GET", path, null, null);
        assertThat(response.status()).describedAs(response.raw()).isEqualTo(200);
        return response;
    }

    private Response post(String path, Object body, String key) throws Exception {
        return request("POST", path, body, key);
    }

    private Response request(String method, String path, Object body, String key) throws Exception {
        var request = HttpRequest.newBuilder(uri(path)).timeout(Duration.ofSeconds(15))
                .header("X-API-Key", API_KEY).header("Content-Type", "application/json");
        if (key != null) request.header("Idempotency-Key", key);
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() :
                HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)));
        HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body().isBlank() ? mapper.nullNode() : mapper.readTree(response.body()), response.body());
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + "/api/v1" + path);
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private String signature(String timestamp, String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return "v1=" + HexFormat.of().formatHex(mac.doFinal((timestamp + "." + body).getBytes(StandardCharsets.UTF_8)));
    }

    private record Response(int status, JsonNode body, String raw) {}
    private record Endpoint(String url, List<Captured> requests) {}
    private record Captured(String body, String eventId, String deliveryId, String attempt, String timestamp, String signature) {}
}
