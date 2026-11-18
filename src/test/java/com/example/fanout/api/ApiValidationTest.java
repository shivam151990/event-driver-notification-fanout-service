package com.example.fanout.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fanout.domain.EventRequest;
import com.example.fanout.domain.FilterRule;
import com.example.fanout.domain.Operator;
import com.example.fanout.domain.PayloadCondition;
import com.example.fanout.domain.ReplayRequest;
import com.example.fanout.domain.SubscriptionRequest;
import com.fasterxml.jackson.databind.node.NullNode;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

class ApiValidationTest {
    private static final ValidatorFactory FACTORY = Validation.buildDefaultValidatorFactory();

    @AfterAll
    static void closeFactory() {
        FACTORY.close();
    }

    @Test
    void eventRequiresBoundedLabelsAndAPayload() {
        assertThat(FACTORY.getValidator().validate(new EventRequest("", "a".repeat(129), null))).hasSize(3);
        assertThat(FACTORY.getValidator().validate(new EventRequest("order.created", "checkout", NullNode.instance))).isEmpty();
    }

    @Test
    void secretIsRequiredAndNeverAppearsInToString() {
        assertThat(FACTORY.getValidator().validate(new SubscriptionRequest("https://example.com/hook", null, false, "short"))).hasSize(1);
        String secret = "sensitive-signing-secret-at-least-32-chars";
        SubscriptionRequest request = new SubscriptionRequest("https://example.com/hook", null, false, secret);
        assertThat(FACTORY.getValidator().validate(request)).isEmpty();
        assertThat(request.filter()).isEqualTo(FilterRule.all());
        assertThat(request.toString()).doesNotContain(secret).contains("[REDACTED]");
    }

    @Test
    void filterValidationCascadesToNestedConditions() {
        FilterRule filter = new FilterRule(Set.of(" "), Set.of(), List.of(new PayloadCondition(null, Operator.EQ, NullNode.instance)));
        SubscriptionRequest request = new SubscriptionRequest("https://example.com/hook", filter, false, "a".repeat(32));
        assertThat(FACTORY.getValidator().validate(request)).hasSize(2);
    }

    @Test
    void replayRangeMustBePresentIncreasingAndBounded() {
        Instant from = Instant.parse("2026-01-01T00:00:00Z");
        assertThat(FACTORY.getValidator().validate(new ReplayRequest(null, null))).hasSize(2);
        assertThat(FACTORY.getValidator().validate(new ReplayRequest(from, from))).hasSize(1);
        assertThat(FACTORY.getValidator().validate(new ReplayRequest(from, from.minusSeconds(1)))).hasSize(1);
        assertThat(FACTORY.getValidator().validate(new ReplayRequest(from, from.plus(32, ChronoUnit.DAYS)))).hasSize(1);
        assertThat(FACTORY.getValidator().validate(new ReplayRequest(from, from.plus(31, ChronoUnit.DAYS)))).isEmpty();
    }
}
