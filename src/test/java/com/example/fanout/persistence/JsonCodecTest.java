package com.example.fanout.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.fanout.domain.EventRequest;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class JsonCodecTest {
    private final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final JsonCodec codec = new JsonCodec(mapper);

    @Test
    void hashesAreIndependentOfObjectFieldOrderAtEveryDepth() throws Exception {
        EventRequest first = new EventRequest("order", "checkout", mapper.readTree("{\"b\":2,\"a\":{\"z\":true,\"y\":[{\"b\":2,\"a\":1}]}}"));
        EventRequest second = new EventRequest("order", "checkout", mapper.readTree("{\"a\":{\"y\":[{\"a\":1,\"b\":2}],\"z\":true},\"b\":2}"));
        assertThat(codec.hash(first)).hasSize(64).isEqualTo(codec.hash(second));
    }

    @Test
    void numericallyEquivalentRepresentationsHaveTheSameHash() {
        assertThat(codec.hash(codec.read("{\"n\":1000}")))
                .isEqualTo(codec.hash(codec.read("{\"n\":1000.0000}")))
                .isEqualTo(codec.hash(codec.read("{\"n\":1e3}")));
        assertThat(codec.hash(codec.read("{\"n\":0}"))).isEqualTo(codec.hash(codec.read("{\"n\":-0.000}")));
    }

    @Test
    void distinctPayloadsArrayOrderLabelsAndNumericPrecisionChangeTheHash() {
        assertThat(codec.hash(codec.read("[1,2]"))).isNotEqualTo(codec.hash(codec.read("[2,1]")));
        assertThat(codec.hash(codec.read("{\"n\":1}"))).isNotEqualTo(codec.hash(codec.read("{\"n\":\"1\"}")));
        assertThat(codec.hash(codec.read("{\"n\":null}"))).isNotEqualTo(codec.hash(codec.read("{}")));
        assertThat(codec.hash(codec.read("0.123456789012345678901"))).isNotEqualTo(codec.hash(codec.read("0.123456789012345678902")));
        assertThat(codec.hash(new EventRequest("a", "source", codec.read("{}"))))
                .isNotEqualTo(codec.hash(new EventRequest("b", "source", codec.read("{}"))))
                .isNotEqualTo(codec.hash(new EventRequest("a", "other", codec.read("{}"))));
    }

    @Test
    void storedJsonFailuresAreInternalErrors() {
        assertThatThrownBy(() -> codec.read("{broken"))
                .isInstanceOf(IllegalStateException.class).hasMessage("Stored JSON is invalid");
        assertThatThrownBy(() -> codec.read("{broken", EventRequest.class))
                .isInstanceOf(IllegalStateException.class).hasMessage("Stored JSON is invalid");
    }
}
