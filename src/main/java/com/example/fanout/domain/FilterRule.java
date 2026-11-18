package com.example.fanout.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.Set;

/** Empty event type and source sets match every value. All conditions must match. */
public record FilterRule(
        @Size(max = 100) Set<@NotBlank @Size(max = 200) String> eventTypes,
        @Size(max = 100) Set<@NotBlank @Size(max = 200) String> sources,
        @Size(max = 20) List<@NotNull @Valid PayloadCondition> conditions) {

    public FilterRule {
        eventTypes = eventTypes == null ? Set.of() : Set.copyOf(eventTypes);
        sources = sources == null ? Set.of() : Set.copyOf(sources);
        conditions = conditions == null ? List.of() : List.copyOf(conditions);
    }

    public static FilterRule all() {
        return new FilterRule(Set.of(), Set.of(), List.of());
    }
}
