package com.example.fanout.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class WebhookSignerTest {
    @Test
    void matchesIndependentSha256HmacVectorAndSignsExactBytes() {
        byte[] body = "{\"id\":\"test\"}".getBytes(StandardCharsets.UTF_8);
        assertThat(WebhookSigner.sign("test-secret", 1700000000, body))
                .isEqualTo("v1=d68719e5f1c26585c54ca039657a4203af940056c6d510c71e1c38bbd459c51c");
        assertThat(WebhookSigner.sign("other-secret", 1700000000, body))
                .isNotEqualTo(WebhookSigner.sign("test-secret", 1700000000, body));
        assertThat(WebhookSigner.sign("test-secret", 1700000001, body))
                .isNotEqualTo(WebhookSigner.sign("test-secret", 1700000000, body));
    }
}
