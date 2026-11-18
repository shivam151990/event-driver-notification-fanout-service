package com.example.fanout.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class RequestBoundaryFilterTest {
    private static final String KEY = "test-api-key-at-least-32-characters";
    private final ObjectMapper mapper = new ObjectMapper();
    private final RequestBoundaryFilter filter = new RequestBoundaryFilter(KEY, 1024, mapper);

    @Test
    void requiresAuthenticationAndNeverInvokesTheControllerWithoutIt() throws Exception {
        MockHttpServletRequest request = request("GET", "/api/v1/subscriptions", null);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean invoked = new AtomicBoolean();
        filter.doFilter(request, response, (req, res) -> invoked.set(true));
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getContentType()).startsWith("application/problem+json");
        assertThat(invoked).isFalse();
        assertThat(response.getHeader("X-Request-Id")).isNotBlank();
        assertThat(MDC.get("requestId")).isNull();
    }

    @Test
    void rejectsWrongKeyButAcceptsCorrectKeyAndCarriesRequestId() throws Exception {
        MockHttpServletRequest wrong = request("GET", "/api/v1/events", "wrong");
        MockHttpServletResponse wrongResponse = new MockHttpServletResponse();
        filter.doFilter(wrong, wrongResponse, (req, res) -> { throw new AssertionError("Must be rejected"); });
        assertThat(wrongResponse.getStatus()).isEqualTo(401);

        MockHttpServletRequest right = request("GET", "/api/v1/events", KEY);
        right.addHeader("X-Request-Id", "client-correlation-123");
        MockHttpServletResponse rightResponse = new MockHttpServletResponse();
        filter.doFilter(right, rightResponse, (req, res) -> assertThat(MDC.get("requestId")).isEqualTo("client-correlation-123"));
        assertThat(rightResponse.getStatus()).isEqualTo(200);
        assertThat(rightResponse.getHeader("X-Request-Id")).isEqualTo("client-correlation-123");
    }

    @Test
    void onlyExplicitHealthEndpointsArePublic() throws Exception {
        for (String path : new String[]{"/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness", "/actuator/info"}) {
            MockHttpServletResponse response = new MockHttpServletResponse();
            filter.doFilter(request("GET", path, null), response, (req, res) -> { });
            assertThat(response.getStatus()).as(path).isEqualTo(200);
        }
        for (String path : new String[]{"/actuator/prometheus", "/actuator/metrics", "/actuator/health/other"}) {
            MockHttpServletResponse response = new MockHttpServletResponse();
            filter.doFilter(request("GET", path, null), response, (req, res) -> { throw new AssertionError("Protected"); });
            assertThat(response.getStatus()).as(path).isEqualTo(401);
        }
    }

    @Test
    void requestBodyRemainsReadableAfterApplyingBound() throws Exception {
        MockHttpServletRequest request = request("POST", "/api/v1/events", KEY);
        request.setContent("{\"payload\":true}".getBytes(StandardCharsets.UTF_8));
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> {
            assertThat(new String(req.getInputStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("{\"payload\":true}");
            assertThat(((HttpServletRequest) req).getHeader("X-API-Key")).isEqualTo(KEY);
        });
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void rejectsOversizedChunkedBodyEvenWhenContentLengthIsUnknown() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/events") {
            @Override public long getContentLengthLong() { return -1; }
            @Override public int getContentLength() { return -1; }
        };
        request.addHeader("X-API-Key", KEY);
        request.setContent(new byte[1025]);
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> { throw new AssertionError("Too large"); });
        assertThat(response.getStatus()).isEqualTo(413);
    }

    @Test
    void rejectsCompressedBodiesAndInvalidConfiguration() throws Exception {
        MockHttpServletRequest request = request("POST", "/api/v1/events", KEY);
        request.addHeader("Content-Encoding", "gzip");
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> { throw new AssertionError("Compressed"); });
        assertThat(response.getStatus()).isEqualTo(415);
        assertThatThrownBy(() -> new RequestBoundaryFilter("short", 1024, mapper)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RequestBoundaryFilter(KEY, 0, mapper)).isInstanceOf(IllegalArgumentException.class);
    }

    private static MockHttpServletRequest request(String method, String uri, String key) {
        MockHttpServletRequest request = new MockHttpServletRequest(method, uri);
        if (key != null) {
            request.addHeader("X-API-Key", key);
        }
        return request;
    }
}
