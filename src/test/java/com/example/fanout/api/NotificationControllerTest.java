package com.example.fanout.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.fanout.persistence.NotificationService;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.server.ResponseStatusException;

class NotificationControllerTest {
    private final NotificationService service = mock(NotificationService.class);
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        mvc = MockMvcBuilders.standaloneSetup(new NotificationController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(mapper)).build();
    }

    @Test
    void ingestionReturnsAcceptedWithRetrievableLocation() throws Exception {
        UUID id = UUID.randomUUID();
        when(service.ingest(any(), eq("request-1"))).thenReturn(Map.of("id", id));
        mvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", "request-1")
                .content("{\"type\":\"order.created\",\"source\":\"checkout\",\"payload\":{}}"))
                .andExpect(status().isAccepted()).andExpect(header().string("Location", "/api/v1/events/" + id))
                .andExpect(jsonPath("$.id").value(id.toString()));
    }

    @Test
    void createdSubscriptionLocationCanBeFetched() throws Exception {
        UUID id = UUID.randomUUID();
        Map<String, Object> response = Map.of("id", id, "webhookUrl", "https://example.com/webhook");
        when(service.createSubscription(any())).thenReturn(response);
        when(service.getSubscription(id)).thenReturn(response);
        mvc.perform(post("/api/v1/subscriptions").contentType(MediaType.APPLICATION_JSON)
                .content("{\"webhookUrl\":\"https://example.com/webhook\",\"signingSecret\":\"a-signing-secret-with-at-least-32-characters\"}"))
                .andExpect(status().isCreated()).andExpect(header().string("Location", "/api/v1/subscriptions/" + id))
                .andExpect(jsonPath("$.signingSecret").doesNotExist());
        mvc.perform(get("/api/v1/subscriptions/{id}", id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()));
        verify(service).getSubscription(id);
    }

    @Test
    void rejectsInvalidMissingAndAmbiguousJsonBeforePersisting() throws Exception {
        for (String body : new String[]{
                "{\"type\":\"\",\"source\":\"checkout\",\"payload\":{}}",
                "{\"type\":\"order\",\"source\":\"checkout\"}",
                "{\"type\":\"order\",\"source\":\"checkout\",\"payload\":{},\"unknown\":true}",
                "{\"type\":\"order\",\"source\":\"checkout\",\"payload\":{}}{}",
                "{\"type\":\"order\",\"type\":\"other\",\"source\":\"checkout\",\"payload\":{}}"}) {
            mvc.perform(post("/api/v1/events").contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest()).andExpect(jsonPath("$.status").value(400));
        }
        verifyNoInteractions(service);
    }

    @Test
    void replayRequiresIdempotencyHeaderAndValidRange() throws Exception {
        UUID id = UUID.randomUUID();
        mvc.perform(post("/api/v1/subscriptions/{id}/replays", id).contentType(MediaType.APPLICATION_JSON)
                .content("{\"from\":\"2026-01-01T00:00:00Z\",\"to\":\"2026-01-02T00:00:00Z\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/api/v1/subscriptions/{id}/replays", id).contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", "replay-1")
                .content("{\"from\":\"2026-01-01T00:00:00Z\",\"to\":\"2026-03-01T00:00:00Z\"}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void returnsMeaningfulStatusesAndDoesNotExposeUnexpectedExceptions() throws Exception {
        UUID missing = UUID.randomUUID();
        when(service.getEvent(missing)).thenThrow(new ResponseStatusException(HttpStatus.NOT_FOUND, "Event not found"));
        mvc.perform(get("/api/v1/events/{id}", missing)).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value("Event not found"));
        mvc.perform(get("/api/v1/events/not-a-uuid")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/v1/events").contentType(MediaType.TEXT_PLAIN).content("invalid"))
                .andExpect(status().isUnsupportedMediaType());
        UUID broken = UUID.randomUUID();
        when(service.getEvent(broken)).thenThrow(new IllegalStateException("jdbc://private-database?secret=password"));
        mvc.perform(get("/api/v1/events/{id}", broken)).andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.detail").value("The request could not be completed"));
    }
}
