package com.example.fanout.delivery;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/** Capped exponential backoff with equal jitter and bounded Retry-After support. */
public final class RetryPolicy {
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final int maxAttempts;
    private final DoubleSupplier random;

    public RetryPolicy(Duration initialBackoff, Duration maxBackoff, int maxAttempts) {
        this(initialBackoff, maxBackoff, maxAttempts, () -> ThreadLocalRandom.current().nextDouble());
    }

    RetryPolicy(Duration initialBackoff, Duration maxBackoff, int maxAttempts, DoubleSupplier random) {
        if (initialBackoff.isNegative() || initialBackoff.isZero() || maxBackoff.compareTo(initialBackoff) < 0
                || maxAttempts < 1) {
            throw new IllegalArgumentException("Retry bounds must be positive and maxBackoff >= initialBackoff");
        }
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.maxAttempts = maxAttempts;
        this.random = random;
    }

    public boolean shouldRetry(int attempt, Integer statusCode, boolean permanentFailure) {
        return attempt < maxAttempts && !permanentFailure
                && (statusCode == null || statusCode < 200 || statusCode >= 300);
    }

    public Duration delay(int attempt, String retryAfter, Instant now) {
        long cap = maxBackoff.toMillis();
        long exponential = initialBackoff.toMillis();
        for (int index = 1; index < attempt && exponential < cap; index++) {
            exponential = exponential > cap / 2 ? cap : exponential * 2;
        }
        exponential = Math.min(exponential, cap);
        long jittered = Math.max(1, (long) (exponential / 2.0 + random.getAsDouble() * exponential / 2.0));
        return Duration.ofMillis(Math.min(cap, Math.max(jittered, retryAfterMillis(retryAfter, now, cap))));
    }

    private static long retryAfterMillis(String value, Instant now, long cap) {
        if (value == null || value.isBlank()) {
            return 0;
        }
        try {
            long seconds = Long.parseLong(value.strip());
            return seconds <= 0 ? 0 : seconds > cap / 1000 ? cap : seconds * 1000;
        } catch (NumberFormatException ignored) {
            try {
                Instant retryAt = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
                return Math.max(0, Math.min(cap, Duration.between(now, retryAt).toMillis()));
            } catch (DateTimeParseException | ArithmeticException ignoredDate) {
                return 0;
            }
        }
    }
}
