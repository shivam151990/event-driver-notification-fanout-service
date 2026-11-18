package com.example.fanout.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.fanout.domain.FilterRule;
import com.example.fanout.domain.Operator;
import com.example.fanout.domain.PayloadCondition;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class FilterMatcherTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final FilterMatcher matcher = new FilterMatcher();

    @Test
    void emptyRulesMatchArbitraryJsonAndTypeAndSourceFiltersAreConjoined() throws Exception {
        assertThat(matcher.matches(FilterRule.all(), "order.created", "checkout", json("null"))).isTrue();
        FilterRule rule = new FilterRule(Set.of("order.created"), Set.of("checkout"), List.of());
        assertThat(matcher.matches(rule, "order.created", "checkout", json("[]"))).isTrue();
        assertThat(matcher.matches(rule, "order.deleted", "checkout", json("{}"))).isFalse();
        assertThat(matcher.matches(rule, "order.created", "inventory", json("{}"))).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"EQ,10,true", "EQ,10.0,true", "EQ,11,false", "NE,10,false", "NE,11,true",
            "GT,9,true", "GT,10,false", "GTE,10,true", "GTE,11,false",
            "LT,11,true", "LT,10,false", "LTE,10,true", "LTE,9,false"})
    void comparisonsUseExactDecimalNumbers(Operator operator, String operand, boolean expected) throws Exception {
        assertThat(matches("{\"amount\":10}", "/amount", operator, operand)).isEqualTo(expected);
    }

    @Test
    void conditionsAreAndCombinedAndUseJsonPointerEscapingAndArrays() throws Exception {
        FilterRule rule = new FilterRule(Set.of(), Set.of(), List.of(
                new PayloadCondition("/a~1b/~0name/0", Operator.EQ, json("\"premium\"")),
                new PayloadCondition("/amount", Operator.GTE, json("100"))));
        matcher.validate(rule);
        assertThat(matcher.matches(rule, "type", "source", json("{\"a/b\":{\"~name\":[\"premium\"]},\"amount\":101}"))).isTrue();
        assertThat(matcher.matches(rule, "type", "source", json("{\"a/b\":{\"~name\":[\"premium\"]},\"amount\":99}"))).isFalse();
    }

    @Test
    void missingDiffersFromExplicitNullAndNeverSatisfiesNe() throws Exception {
        assertThat(matches("{}", "/value", Operator.EQ, "null")).isFalse();
        assertThat(matches("{}", "/value", Operator.NE, "null")).isFalse();
        assertThat(matches("{}", "/value", Operator.EXISTS, "false")).isTrue();
        assertThat(matches("{\"value\":null}", "/value", Operator.EQ, "null")).isTrue();
        assertThat(matches("{\"value\":null}", "/value", Operator.EXISTS, "true")).isTrue();
    }

    @Test
    void inSupportsNumbersAndStructuredValuesAndDoesNotCoerceStrings() throws Exception {
        assertThat(matches("{\"n\":1}", "/n", Operator.IN, "[2,1.0]")).isTrue();
        assertThat(matches("{\"n\":\"1\"}", "/n", Operator.IN, "[1]")).isFalse();
        assertThat(matches("{\"n\":{\"a\":1}}", "/n", Operator.IN, "[{\"a\":1}]")).isTrue();
        assertThat(matches("{\"n\":{\"a\":[1,2]}}", "/n", Operator.EQ, "{\"a\":[1.0,2.00]}" )).isTrue();
        assertThat(matches("{\"n\":\"12\"}", "/n", Operator.GT, "10")).isFalse();
        assertThat(matches("{\"n\":true}", "/n", Operator.GT, "0")).isFalse();
    }

    @Test
    void supportsRootAndExactLargeNumbers() throws Exception {
        assertThat(matches("9007199254740993", "", Operator.GT, "9007199254740992")).isTrue();
        assertThat(matches("[1,2]", "", Operator.EQ, "[1,2]")).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo", "$.foo", "/foo~", "/foo~2bar", "/foo\n~7"})
    void rejectsInvalidJsonPointers(String path) throws Exception {
        FilterRule rule = rule(path, Operator.EQ, "1");
        assertThatThrownBy(() -> matcher.validate(rule)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"EXISTS,1", "IN,1", "GT,true", "GTE,null", "LT,[]", "LTE,{}"})
    void rejectsWrongOperandTypes(Operator operator, String operand) throws Exception {
        FilterRule rule = rule("/foo", operator, operand);
        assertThatThrownBy(() -> matcher.validate(rule)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissingOperandAndUnboundedConditions() throws Exception {
        assertThatThrownBy(() -> matcher.validate(new FilterRule(Set.of(), Set.of(),
                List.of(new PayloadCondition("/foo", Operator.EQ, null))))).isInstanceOf(IllegalArgumentException.class);
        PayloadCondition condition = new PayloadCondition("/foo", Operator.EQ, json("1"));
        assertThatThrownBy(() -> matcher.validate(new FilterRule(Set.of(), Set.of(),
                java.util.Collections.nCopies(21, condition)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> matcher.validate(new FilterRule(Set.of(" "), Set.of(), List.of())))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private boolean matches(String payload, String path, Operator operator, String operand) throws Exception {
        FilterRule rule = rule(path, operator, operand);
        matcher.validate(rule);
        return matcher.matches(rule, "type", "source", json(payload));
    }

    private FilterRule rule(String path, Operator operator, String operand) throws Exception {
        return new FilterRule(Set.of(), Set.of(), List.of(new PayloadCondition(path, operator, json(operand))));
    }

    private JsonNode json(String value) throws Exception {
        return mapper.readTree(value);
    }
}
