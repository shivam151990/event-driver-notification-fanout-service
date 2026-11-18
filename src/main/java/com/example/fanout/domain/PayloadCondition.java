package com.example.fanout.domain;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record PayloadCondition(
        @NotNull @Size(max = 512) String path,
        @NotNull Operator operator,
        JsonNode value) {
}
