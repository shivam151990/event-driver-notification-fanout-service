package com.example.fanout.domain;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record EventRequest(
        @NotBlank @Size(max = 128) String type,
        @NotBlank @Size(max = 128) String source,
        @NotNull JsonNode payload) {
}
