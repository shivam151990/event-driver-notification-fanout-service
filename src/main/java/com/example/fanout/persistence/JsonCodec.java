package com.example.fanout.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.TreeMap;

@Component
public class JsonCodec {
    private final ObjectMapper mapper;
    public JsonCodec(ObjectMapper mapper) { this.mapper = mapper; }
    public String write(Object value) {
        validate(value);
        try { return mapper.writeValueAsString(value); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("Invalid JSON", e); }
    }
    public JsonNode read(String value) {
        try { return mapper.readTree(value); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Stored JSON is invalid", e); }
    }
    public <T> T read(String value, Class<T> type) {
        try { return mapper.readValue(value, type); }
        catch (JsonProcessingException e) { throw new IllegalStateException("Stored JSON is invalid", e); }
    }
    /** Reject values PostgreSQL jsonb/text cannot represent before opening a transaction. */
    public void validate(Object value) {
        validateNode(mapper.valueToTree(value), 0);
    }
    private void validateNode(JsonNode node, int depth) {
        if (depth > 100) throw new IllegalArgumentException("JSON nesting exceeds 100 levels");
        if (node.isObject()) {
            node.fields().forEachRemaining(field -> {
                validateText(field.getKey()); validateNode(field.getValue(), depth + 1);
            });
        } else if (node.isArray()) {
            node.forEach(child -> validateNode(child, depth + 1));
        } else if (node.isTextual()) {
            validateText(node.textValue());
        } else if (node.isNumber()) {
            var number = node.decimalValue();
            if ((long) number.precision() - number.scale() > 131072 || number.scale() > 16383)
                throw new IllegalArgumentException("JSON number exceeds PostgreSQL numeric range");
            long integerDigits = Math.max(1L, (long) number.precision() - number.scale());
            long fractionDigits = Math.max(0L, number.scale());
            long expandedLength = integerDigits + fractionDigits + (fractionDigits > 0 ? 1 : 0)
                    + (number.signum() < 0 ? 1 : 0);
            // jsonb expands scientific notation. Keep persisted numbers readable by the same
            // bounded parser and prevent a tiny exponent token expanding into huge payloads.
            if (expandedLength > 100) throw new IllegalArgumentException("JSON numbers must expand to at most 100 decimal characters");
        }
    }
    private void validateText(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == 0) throw new IllegalArgumentException("JSON strings must not contain U+0000");
            if (Character.isHighSurrogate(c)) {
                if (++i >= text.length() || !Character.isLowSurrogate(text.charAt(i)))
                    throw new IllegalArgumentException("JSON strings must contain valid Unicode");
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalArgumentException("JSON strings must contain valid Unicode");
            }
        }
    }
    public String hash(Object value) {
        try {
            byte[] bytes = write(canonical(mapper.valueToTree(value))).getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    private JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            ObjectNode result = mapper.createObjectNode();
            var fields = new TreeMap<String, JsonNode>();
            node.fields().forEachRemaining(entry -> fields.put(entry.getKey(), entry.getValue()));
            fields.forEach((key, value) -> result.set(key, canonical(value)));
            return result;
        }
        if (node.isArray()) {
            ArrayNode result = mapper.createArrayNode();
            node.forEach(value -> result.add(canonical(value)));
            return result;
        }
        if (node.isNumber()) return mapper.getNodeFactory().numberNode(node.decimalValue().stripTrailingZeros());
        return node;
    }
}
