package com.example.fanout.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Authenticates before reading any body, then bounds even requests using chunked transfer encoding. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RequestBoundaryFilter extends OncePerRequestFilter {
    private final byte[] expectedKey;
    private final int maxRequestBytes;
    private final ObjectMapper mapper;

    public RequestBoundaryFilter(@Value("${fanout.api-key}") String apiKey,
                                 @Value("${fanout.max-request-bytes:1048576}") int maxRequestBytes,
                                 ObjectMapper mapper) {
        if (apiKey == null || apiKey.isBlank() || apiKey.length() < 32) {
            throw new IllegalArgumentException("fanout.api-key must contain at least 32 characters");
        }
        if (maxRequestBytes < 1024 || maxRequestBytes > 10 * 1024 * 1024) {
            throw new IllegalArgumentException("fanout.max-request-bytes must be between 1024 and 10485760");
        }
        this.expectedKey = apiKey.getBytes(StandardCharsets.UTF_8);
        this.maxRequestBytes = maxRequestBytes;
        this.mapper = mapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String requestId = request.getHeader("X-Request-Id");
        if (requestId == null || !requestId.matches("[A-Za-z0-9._:-]{1,64}")) {
            requestId = UUID.randomUUID().toString();
        }
        response.setHeader("X-Request-Id", requestId);
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("Cache-Control", "no-store");
        MDC.put("requestId", requestId);
        try {
            String path = request.getRequestURI().substring(request.getContextPath().length());
            if (!isPublic(path)) {
                String supplied = request.getHeader("X-API-Key");
                if (supplied == null || !MessageDigest.isEqual(expectedKey, supplied.getBytes(StandardCharsets.UTF_8))) {
                    response.setHeader("WWW-Authenticate", "ApiKey");
                    problem(response, 401, "Unauthorized", "A valid X-API-Key header is required", requestId);
                    return;
                }
            }
            if (path.startsWith("/api/") && isBodyMethod(request.getMethod())) {
                String encoding = request.getHeader("Content-Encoding");
                if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
                    problem(response, 415, "Unsupported Media Type", "Compressed request bodies are not supported", requestId);
                    return;
                }
                if (request.getContentLengthLong() > maxRequestBytes) {
                    problem(response, 413, "Content Too Large", "Request body exceeds the configured limit", requestId);
                    return;
                }
                byte[] body = request.getInputStream().readNBytes(maxRequestBytes + 1);
                if (body.length > maxRequestBytes) {
                    problem(response, 413, "Content Too Large", "Request body exceeds the configured limit", requestId);
                    return;
                }
                chain.doFilter(new BoundedRequest(request, body), response);
            } else {
                chain.doFilter(request, response);
            }
        } finally {
            MDC.remove("requestId");
        }
    }

    private static boolean isPublic(String path) {
        return path.equals("/actuator/health") || path.equals("/actuator/health/liveness")
                || path.equals("/actuator/health/readiness") || path.equals("/actuator/info");
    }

    private static boolean isBodyMethod(String method) {
        return method.equals("POST") || method.equals("PUT") || method.equals("PATCH");
    }

    private void problem(HttpServletResponse response, int status, String title, String detail, String requestId)
            throws IOException {
        response.setStatus(status);
        response.setContentType("application/problem+json");
        mapper.writeValue(response.getOutputStream(), Map.of("type", "about:blank", "title", title,
                "status", status, "detail", detail, "requestId", requestId));
    }

    private static final class BoundedRequest extends HttpServletRequestWrapper {
        private final byte[] body;

        private BoundedRequest(HttpServletRequest request, byte[] body) {
            super(request);
            this.body = body;
        }

        @Override
        public ServletInputStream getInputStream() {
            ByteArrayInputStream input = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override public int read() { return input.read(); }
                @Override public int read(byte[] bytes, int offset, int length) { return input.read(bytes, offset, length); }
                @Override public boolean isFinished() { return input.available() == 0; }
                @Override public boolean isReady() { return true; }
                @Override public void setReadListener(ReadListener listener) {
                    throw new IllegalStateException("Asynchronous request reads are unsupported");
                }
            };
        }

        @Override
        public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
        }
    }
}
