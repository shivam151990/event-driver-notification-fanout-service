package com.example.fanout.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

class JsonConfigurationTest {
    @Test
    void rejectsExcessiveDepthAndNumberLengthWhileAcceptingOrdinaryPayloads() throws Exception {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        new JsonConfiguration().boundedJsonParser().customize(builder);
        ObjectMapper mapper = builder.build();

        assertThat(mapper.readTree("{\"nested\":{\"array\":[1,true,null]}}" ).at("/nested/array/0").intValue()).isEqualTo(1);
        assertThat(mapper.getFactory().streamReadConstraints().getMaxStringLength()).isEqualTo(1_048_576);
        assertThatThrownBy(() -> mapper.readTree("[".repeat(101) + "0" + "]".repeat(101)))
                .hasMessageContaining("nesting depth");
        assertThatThrownBy(() -> mapper.readTree("1".repeat(101)))
                .hasMessageContaining("Number value length");
    }
}
