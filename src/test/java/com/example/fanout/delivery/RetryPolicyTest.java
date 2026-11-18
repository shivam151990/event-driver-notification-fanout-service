package com.example.fanout.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RetryPolicyTest {
    private final RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(60), 5, () -> 0.5);
    private final Instant now = Instant.parse("2026-01-01T00:00:00Z");

    @ParameterizedTest
    @ValueSource(ints = {301, 400, 401, 403, 404, 408, 425, 429, 500, 503})
    void retriesAllHttpFailuresUntilTheLimit(int code) {
        assertThat(policy.shouldRetry(1, code, false)).isTrue();
        assertThat(policy.shouldRetry(5, code, false)).isFalse();
    }

    @Test
    void excludesSuccessAndPermanentPolicyFailuresButIncludesNetworkFailures() {
        assertThat(policy.shouldRetry(1, 204, false)).isFalse();
        assertThat(policy.shouldRetry(1, null, false)).isTrue();
        assertThat(policy.shouldRetry(1, null, true)).isFalse();
    }

    @Test
    void appliesCappedExponentialBackoffWithJitterWithoutOverflow() {
        assertThat(policy.delay(1, null, now)).isEqualTo(Duration.ofMillis(750));
        assertThat(policy.delay(2, null, now)).isEqualTo(Duration.ofMillis(1500));
        assertThat(policy.delay(3, null, now)).isEqualTo(Duration.ofSeconds(3));
        assertThat(policy.delay(Integer.MAX_VALUE, null, now)).isEqualTo(Duration.ofSeconds(45));
    }

    @Test
    void honorsBoundedRetryAfterSecondsAndHttpDates() {
        assertThat(policy.delay(1, "10", now)).isEqualTo(Duration.ofSeconds(10));
        assertThat(policy.delay(1, "9223372036854775807", now)).isEqualTo(Duration.ofSeconds(60));
        assertThat(policy.delay(1, "Thu, 1 Jan 2026 00:00:15 GMT", now)).isEqualTo(Duration.ofSeconds(15));
        assertThat(policy.delay(1, "invalid", now)).isEqualTo(Duration.ofMillis(750));
        assertThat(policy.delay(1, "-10", now)).isEqualTo(Duration.ofMillis(750));
        assertThat(policy.delay(1, "Wed, 31 Dec 2025 23:00:00 GMT", now)).isEqualTo(Duration.ofMillis(750));
    }
}
