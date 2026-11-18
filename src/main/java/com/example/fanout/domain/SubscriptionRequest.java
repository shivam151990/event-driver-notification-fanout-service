package com.example.fanout.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record SubscriptionRequest(
        @NotBlank @Size(max = 2048) String webhookUrl,
        @Valid FilterRule filter,
        boolean ordered,
        @NotBlank @Size(min = 32, max = 256) String signingSecret) {

    public SubscriptionRequest {
        filter = filter == null ? FilterRule.all() : filter;
    }

    @Override
    public String toString() {
        return "SubscriptionRequest[webhookUrl=" + webhookUrl + ", filter=" + filter
                + ", ordered=" + ordered + ", signingSecret=[REDACTED]]";
    }
}
