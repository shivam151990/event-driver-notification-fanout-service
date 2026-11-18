package com.example.fanout.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.time.Instant;

public record ReplayRequest(@NotNull Instant from, @NotNull Instant to) {

    @AssertTrue(message = "Replay range must have from < to and span no more than 31 days")
    @JsonIgnore
    public boolean isValidRange() {
        return from == null || to == null || (from.isBefore(to)
                && Duration.between(from, to).compareTo(Duration.ofDays(31)) <= 0);
    }
}
