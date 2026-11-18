package com.example.fanout.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpWebhookTransportTest {
    private HttpServer server;
    private HttpWebhookTransport transport;
    private String baseUrl;
    private final UUID eventId = UUID.randomUUID();
    private final UUID deliveryId = UUID.randomUUID();
    private final byte[] body = "{\"hello\":\"world\"}".getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        DeliveryProperties properties = new DeliveryProperties();
        properties.setAllowHttp(true);
        properties.setAllowPrivateAddresses(true);
        properties.setTimeout(Duration.ofSeconds(2));
        transport = new HttpWebhookTransport(new WebhookPolicy(true, true), properties);
    }

    @AfterEach
    void stop() {
        if (transport != null) {
            transport.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void postsSignedJsonWithStableIdentityAndDoesNotReadResponseBody() {
        AtomicReference<byte[]> received = new AtomicReference<>();
        AtomicReference<String> receivedSignature = new AtomicReference<>();
        AtomicReference<String> receivedTimestamp = new AtomicReference<>();
        AtomicReference<String> receivedDelivery = new AtomicReference<>();
        CountDownLatch release = new CountDownLatch(1);
        server.createContext("/webhook", exchange -> {
            received.set(exchange.getRequestBody().readAllBytes());
            receivedSignature.set(exchange.getRequestHeaders().getFirst("X-Webhook-Signature"));
            receivedTimestamp.set(exchange.getRequestHeaders().getFirst("X-Webhook-Timestamp"));
            receivedDelivery.set(exchange.getRequestHeaders().getFirst("X-Delivery-Id"));
            exchange.sendResponseHeaders(200, 1_000_000);
            exchange.getResponseBody().write(0);
            exchange.getResponseBody().flush();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        try {
            WebhookTransport.Result result = send("/webhook");
            assertThat(result.delivered()).as("HTTP result: %s", result).isTrue();
            assertThat(received.get()).isEqualTo(body);
            assertThat(receivedDelivery.get()).isEqualTo(deliveryId.toString());
            assertThat(receivedSignature.get()).isEqualTo(WebhookSigner.sign("test-secret",
                    Long.parseLong(receivedTimestamp.get()), body));
        } finally {
            release.countDown();
        }
    }

    @Test
    void capturesStatusAndRetryAfterWithoutAutomaticallyRetrying() {
        AtomicInteger calls = new AtomicInteger();
        server.createContext("/busy", exchange -> {
            calls.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("Retry-After", "30");
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        WebhookTransport.Result result = send("/busy");
        assertThat(result.httpStatus()).isEqualTo(503);
        assertThat(result.retryAfter()).isEqualTo("30");
        assertThat(result.permanentFailure()).isFalse();
        assertThat(calls).hasValue(1);
    }

    @Test
    void neverFollowsRedirects() {
        AtomicInteger followed = new AtomicInteger();
        server.createContext("/redirect", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().set("Location", baseUrl + "/private");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/private", exchange -> {
            followed.incrementAndGet();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        assertThat(send("/redirect").httpStatus()).isEqualTo(302);
        assertThat(followed).hasValue(0);
    }

    @Test
    void boundsTheWholeHttpAttemptWhenServerNeverSendsHeaders() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        server.createContext("/slow", exchange -> {
            exchange.getRequestBody().readAllBytes();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        transport.close();
        DeliveryProperties properties = new DeliveryProperties();
        properties.setTimeout(Duration.ofMillis(150));
        transport = new HttpWebhookTransport(new WebhookPolicy(true, true), properties);
        long started = System.nanoTime();
        try {
            WebhookTransport.Result result = send("/slow");
            assertThat(result.httpStatus()).isNull();
            assertThat(result.permanentFailure()).isFalse();
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
        } finally {
            release.countDown();
        }
    }

    @Test
    void pinsTheValidatedDnsAnswerToTheConnection() throws Exception {
        AtomicInteger dnsCalls = new AtomicInteger();
        InetAddress loopback = InetAddress.getByName("127.0.0.1");
        WebhookPolicy policy = new WebhookPolicy(true, true, host -> {
            dnsCalls.incrementAndGet();
            return new InetAddress[] {loopback};
        });
        transport.close();
        transport = new HttpWebhookTransport(policy, new DeliveryProperties());
        server.createContext("/pinned", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        String url = "http://does-not-resolve.invalid:" + server.getAddress().getPort() + "/pinned";
        assertThat(transport.send(url, eventId, deliveryId, 1, "test-secret", body).delivered()).isTrue();
        assertThat(dnsCalls).hasValue(1);
    }

    @Test
    void rejectsPrivateAddressBeforeSendingAnyRequest() {
        transport.close();
        transport = new HttpWebhookTransport(new WebhookPolicy(true, false), new DeliveryProperties());
        WebhookTransport.Result result = send("/never-sent");
        assertThat(result.permanentFailure()).isTrue();
        assertThat(result.httpStatus()).isNull();
    }

    @Test
    void boundsResponseHeaderParsing() {
        server.createContext("/too-many-headers", exchange -> {
            exchange.getRequestBody().readAllBytes();
            for (int index = 0; index < 100; index++) {
                exchange.getResponseHeaders().add("X-Filler-" + index, "value");
            }
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        WebhookTransport.Result result = send("/too-many-headers");
        assertThat(result.httpStatus()).isNull();
        assertThat(result.permanentFailure()).isFalse();
    }

    private WebhookTransport.Result send(String path) {
        return transport.send(baseUrl + path, eventId, deliveryId, 1, "test-secret", body);
    }
}
