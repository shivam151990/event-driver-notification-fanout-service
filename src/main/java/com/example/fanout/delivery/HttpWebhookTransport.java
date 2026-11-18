package com.example.fanout.delivery;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;
import org.springframework.stereotype.Component;

/** Every attempt resolves, validates and pins DNS; redirects, cookies and hidden retries are disabled. */
@Component
public final class HttpWebhookTransport implements WebhookTransport {
    private final WebhookPolicy policy;
    private final DeliveryProperties properties;
    private final ScheduledExecutorService deadlines = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("webhook-deadline-", 0).factory());
    private final ThreadPoolExecutor dnsExecutor;

    public HttpWebhookTransport(WebhookPolicy policy, DeliveryProperties properties) {
        this.policy = policy;
        this.properties = properties;
        this.dnsExecutor = new ThreadPoolExecutor(0, properties.getConcurrency(), 30, TimeUnit.SECONDS,
                new SynchronousQueue<>(), Thread.ofPlatform().daemon().name("webhook-dns-", 0).factory());
    }

    @Override
    public Result send(String url, UUID eventId, UUID deliveryId, int attempt, String secret, byte[] body) {
        long deadline = System.nanoTime() + properties.getTimeout().toNanos();
        HttpPost post = null;
        ScheduledFuture<?> cancellation = null;
        CloseableHttpClient client = null;
        try {
            URI uri = policy.validate(url);
            InetAddress[] addresses = resolve(uri, deadline);
            long remainingMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
            Timeout timeout = Timeout.ofMilliseconds(remainingMillis);
            var manager = PoolingHttpClientConnectionManagerBuilder.create()
                    .setDnsResolver(new PinnedResolver(uri.getHost(), addresses))
                    .setConnectionFactory(ManagedHttpClientConnectionFactory.builder()
                            .http1Config(Http1Config.custom().setMaxHeaderCount(64).setMaxLineLength(8192).build())
                            .build())
                    .setMaxConnTotal(1).setMaxConnPerRoute(1)
                    .setDefaultConnectionConfig(ConnectionConfig.custom()
                            .setConnectTimeout(timeout).setSocketTimeout(timeout).build())
                    .build();
            client = HttpClients.custom().setConnectionManager(manager)
                    .disableRedirectHandling().disableAutomaticRetries().disableCookieManagement()
                    .disableContentCompression().disableAuthCaching()
                    .setConnectionReuseStrategy((request, response, context) -> false)
                    .setDefaultRequestConfig(RequestConfig.custom()
                            .setConnectionRequestTimeout(timeout).setResponseTimeout(timeout).build())
                    .build();
            post = new HttpPost(uri);
            long timestamp = Instant.now().getEpochSecond();
            post.setHeader("User-Agent", "notification-fanout/1.0");
            post.setHeader("X-Delivery-Id", deliveryId.toString());
            post.setHeader("X-Event-Id", eventId.toString());
            post.setHeader("X-Delivery-Attempt", Integer.toString(attempt));
            post.setHeader("X-Webhook-Timestamp", Long.toString(timestamp));
            post.setHeader("X-Webhook-Signature", WebhookSigner.sign(secret, timestamp, body));
            post.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
            HttpPost cancellable = post;
            cancellation = deadlines.schedule(cancellable::cancel,
                    Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            // Close the connection immediately after headers: response bodies are never buffered or audited.
            CloseableHttpResponse response = client.execute(post);
            try {
                int code = response.getCode();
                Header header = response.getFirstHeader("Retry-After");
                String retryAfter = header == null ? null : header.getValue();
                if (retryAfter != null && retryAfter.length() > 128) {
                    retryAfter = null;
                }
                return new Result(code, code >= 200 && code < 300 ? null : "Webhook returned HTTP " + code,
                        retryAfter, false);
            } finally {
                response.close(CloseMode.IMMEDIATE);
            }
        } catch (IllegalArgumentException exception) {
            return new Result(null, "Webhook endpoint rejected by outbound URL policy", null, true);
        } catch (TimeoutException exception) {
            return new Result(null, "Webhook request exceeded timeout", null, false);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return new Result(null, "Webhook request interrupted", null, false);
        } catch (IOException | RejectedExecutionException exception) {
            return new Result(null, "Webhook transport failure: " + exception.getClass().getSimpleName(), null, false);
        } finally {
            if (cancellation != null) {
                cancellation.cancel(false);
            }
            if (post != null) {
                post.cancel();
            }
            if (client != null) {
                client.close(CloseMode.IMMEDIATE);
            }
        }
    }

    private InetAddress[] resolve(URI uri, long deadline)
            throws InterruptedException, TimeoutException, UnknownHostException {
        Future<InetAddress[]> future = dnsExecutor.submit(() -> policy.validateAndResolve(uri));
        try {
            return future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof IllegalArgumentException rejected) {
                throw rejected;
            }
            throw new UnknownHostException("Webhook DNS resolution failed");
        } finally {
            future.cancel(true);
        }
    }

    @PreDestroy
    public void close() {
        deadlines.shutdownNow();
        dnsExecutor.shutdownNow();
    }

    private record PinnedResolver(String expectedHost, InetAddress[] addresses) implements DnsResolver {
        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            if (!normalize(expectedHost).equalsIgnoreCase(normalize(host))) {
                throw new UnknownHostException("Unexpected webhook host");
            }
            return addresses.clone();
        }

        @Override
        public String resolveCanonicalHostname(String host) throws UnknownHostException {
            resolve(host);
            return expectedHost;
        }

        private static String normalize(String host) {
            return host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        }
    }
}
