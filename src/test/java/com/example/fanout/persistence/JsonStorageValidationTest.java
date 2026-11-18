package com.example.fanout.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DecimalNode;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class JsonStorageValidationTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonCodec json = new JsonCodec(mapper);
    @Test void rejectsNullCodepointInKeysAndValues() {
        assertThatThrownBy(() -> json.write(mapper.createObjectNode().put("x", "a\u0000b"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> json.write(mapper.createObjectNode().put("a\u0000b", "x"))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void rejectsBrokenSurrogatesButPreservesSupplementaryCharacters() {
        for (String value : new String[]{"\uD800", "\uD800x", "\uDC00"}) {
            assertThatThrownBy(() -> json.write(mapper.getNodeFactory().textNode(value))).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatCode(() -> json.write(mapper.getNodeFactory().textNode("\uD83D\uDE80"))).doesNotThrowAnyException();
    }
    @Test void rejectsExtremeExponentsBeforeDatabaseInsertion() {
        assertThatThrownBy(() -> json.write(new DecimalNode(new BigDecimal("1e131072")))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> json.write(new DecimalNode(new BigDecimal("1e-16384")))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> json.write(new DecimalNode(new BigDecimal("1e100")))).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> json.write(new DecimalNode(new BigDecimal("1e-90")))).doesNotThrowAnyException();
    }
}
