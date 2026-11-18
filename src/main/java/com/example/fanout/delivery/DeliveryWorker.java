package com.example.fanout.delivery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public final class DeliveryWorker {
    private static final Logger log = LoggerFactory.getLogger(DeliveryWorker.class);
    private final DeliveryRepository repository;
    private final WebhookTransport transport;
    private final ObjectMapper mapper;
    private final DeliveryProperties properties;
    private final MeterRegistry metrics;
    private final ThreadPoolExecutor workers;
    private final AtomicInteger active = new AtomicInteger();

    public DeliveryWorker(DeliveryRepository repository, WebhookTransport transport, ObjectMapper mapper,
            DeliveryProperties properties, MeterRegistry metrics) {
        this.repository = repository;
        this.transport = transport;
        this.mapper = mapper;
        this.properties = properties;
        this.metrics = metrics;
        this.workers = new ThreadPoolExecutor(0, properties.getConcurrency(), 30, TimeUnit.SECONDS,
                new SynchronousQueue<>(), Thread.ofPlatform().name("webhook-worker-", 0).factory());
        metrics.gauge("fanout.delivery.active", active);
    }

    @Scheduled(fixedDelayString = "${fanout.delivery.poll-delay:250ms}")
    public void poll() {
        if (!properties.isEnabled() || workers.isShutdown()) {
            return;
        }
        for (int index = 0; index < properties.getConcurrency(); index++) {
            if (active.get() >= properties.getConcurrency()) {
                break;
            }
            active.incrementAndGet();
            try {
                workers.execute(() -> {
                    try {
                        processOne();
                    } catch (Exception exception) {
                        // Database failures leave the durable lease and IN_PROGRESS audit for recovery.
                        log.error("Webhook worker failed; any claimed delivery will recover after lease expiry", exception);
                        metrics.counter("fanout.delivery.worker.errors").increment();
                    } finally {
                        active.decrementAndGet();
                    }
                });
            } catch (RejectedExecutionException exception) {
                active.decrementAndGet();
                break;
            }
        }
    }

    public boolean processOne() throws Exception {
        var claim = repository.claim();
        if (claim.isEmpty()) {
            return false;
        }
        var delivery = claim.get();
        Timer.Sample timer = Timer.start(metrics);
        byte[] body = mapper.writeValueAsBytes(new WebhookEvent(delivery.eventId(), delivery.type(),
                delivery.source(), mapper.readTree(delivery.payload()), delivery.acceptedAt(),
                delivery.eventSequence(), delivery.id(), delivery.subscriptionId(), delivery.replayId()));
        WebhookTransport.Result result = transport.send(delivery.webhookUrl(), delivery.eventId(), delivery.id(),
                delivery.attempt(), delivery.signingSecret(), body);
        boolean recorded = repository.complete(delivery, result);
        String outcome = result.delivered() ? "delivered" : "unsuccessful";
        timer.stop(metrics.timer("fanout.delivery.duration", "outcome", outcome));
        metrics.counter("fanout.delivery.attempts", "outcome", outcome).increment();
        if (!recorded) {
            metrics.counter("fanout.delivery.stale.completions").increment();
            log.warn("Discarded stale webhook completion deliveryId={} eventId={} attempt={}",
                    delivery.id(), delivery.eventId(), delivery.attempt());
        } else {
            log.info("Webhook attempt finished deliveryId={} eventId={} subscriptionId={} attempt={} httpStatus={} outcome={}",
                    delivery.id(), delivery.eventId(), delivery.subscriptionId(), delivery.attempt(),
                    result.httpStatus(), outcome);
        }
        return true;
    }

    @PreDestroy
    public void close() {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(properties.getTimeout().plusSeconds(2).toMillis(), TimeUnit.MILLISECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException exception) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public record WebhookEvent(UUID id, String type, String source, JsonNode payload, Instant acceptedAt,
            long eventSequence, UUID deliveryId, UUID subscriptionId, UUID replayId) { }
}
