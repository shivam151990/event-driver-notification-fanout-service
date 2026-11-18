package com.example.fanout.filter;

import com.example.fanout.domain.FilterRule;
import com.example.fanout.domain.PayloadCondition;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

/** A deliberately bounded, non-executable filter language. No scripts or regular expressions. */
@Component
public class FilterMatcher {
    private static final java.util.regex.Pattern INVALID_ESCAPE = java.util.regex.Pattern.compile("~(?![01])");

    public boolean matches(FilterRule rule, String type, String source, JsonNode payload) {
        if (rule == null) {
            return true;
        }
        if (!rule.eventTypes().isEmpty() && !rule.eventTypes().contains(type)) {
            return false;
        }
        if (!rule.sources().isEmpty() && !rule.sources().contains(source)) {
            return false;
        }
        return rule.conditions().stream().allMatch(condition -> matchesCondition(payload, condition));
    }

    public void validate(FilterRule rule) {
        if (rule == null) {
            return;
        }
        if (rule.eventTypes().size() > 100 || rule.sources().size() > 100 || rule.conditions().size() > 20) {
            throw new IllegalArgumentException("A filter permits at most 100 event types, 100 sources and 20 conditions");
        }
        rule.eventTypes().forEach(FilterMatcher::validateLabel);
        rule.sources().forEach(FilterMatcher::validateLabel);
        for (PayloadCondition condition : rule.conditions()) {
            if (condition == null || condition.operator() == null || condition.path() == null) {
                throw new IllegalArgumentException("Each condition requires path and operator");
            }
            String path = condition.path();
            if (path.length() > 512 || (!path.isEmpty() && !path.startsWith("/"))
                    || INVALID_ESCAPE.matcher(path).find()) {
                throw new IllegalArgumentException("Condition paths must be RFC 6901 JSON pointers, at most 512 characters");
            }
            JsonPointer.compile(path);
            JsonNode value = condition.value();
            if (value == null) {
                throw new IllegalArgumentException("Each condition requires a value (JSON null is allowed for EQ and NE)");
            }
            switch (condition.operator()) {
                case EXISTS -> {
                    if (!value.isBoolean()) {
                        throw new IllegalArgumentException("EXISTS requires a boolean value");
                    }
                }
                case IN -> {
                    if (!value.isArray() || value.size() > 100) {
                        throw new IllegalArgumentException("IN requires an array containing at most 100 values");
                    }
                }
                case GT, GTE, LT, LTE -> {
                    if (!value.isNumber()) {
                        throw new IllegalArgumentException("Numeric comparison operators require a numeric value");
                    }
                }
                default -> { }
            }
        }
    }

    private static void validateLabel(String label) {
        if (label == null || label.isBlank() || label.length() > 200) {
            throw new IllegalArgumentException("Filter event types and sources must contain 1 to 200 nonblank characters");
        }
    }

    private boolean matchesCondition(JsonNode payload, PayloadCondition condition) {
        JsonNode actual = payload.at(condition.path());
        if (condition.operator() == com.example.fanout.domain.Operator.EXISTS) {
            return !actual.isMissingNode() == condition.value().booleanValue();
        }
        // Missing and explicit JSON null are different; NE never matches a missing path.
        if (actual.isMissingNode()) {
            return false;
        }
        JsonNode expected = condition.value();
        return switch (condition.operator()) {
            case EQ -> equal(actual, expected);
            case NE -> !equal(actual, expected);
            case IN -> {
                boolean found = false;
                for (JsonNode candidate : expected) {
                    if (equal(actual, candidate)) {
                        found = true;
                        break;
                    }
                }
                yield found;
            }
            case GT -> numeric(actual, expected) && compare(actual, expected) > 0;
            case GTE -> numeric(actual, expected) && compare(actual, expected) >= 0;
            case LT -> numeric(actual, expected) && compare(actual, expected) < 0;
            case LTE -> numeric(actual, expected) && compare(actual, expected) <= 0;
            case EXISTS -> throw new IllegalStateException("EXISTS is handled before value comparisons");
        };
    }

    private static boolean equal(JsonNode actual, JsonNode expected) {
        return actual.equals((left, right) -> numeric(left, right)
                ? compare(left, right) : left.equals(right) ? 0 : 1, expected);
    }

    private static boolean numeric(JsonNode actual, JsonNode expected) {
        return actual.isNumber() && expected.isNumber();
    }

    private static int compare(JsonNode actual, JsonNode expected) {
        return actual.decimalValue().compareTo(expected.decimalValue());
    }
}
