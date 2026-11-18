package com.example.fanout.api;

import com.example.fanout.domain.EventRequest;
import com.example.fanout.domain.ReplayRequest;
import com.example.fanout.domain.SubscriptionRequest;
import com.example.fanout.persistence.NotificationService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/v1", produces = MediaType.APPLICATION_JSON_VALUE)
@Validated
public class NotificationController {
    private final NotificationService service;

    public NotificationController(NotificationService service) {
        this.service = service;
    }

    @PostMapping(value = "/events", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> ingest(
            @Valid @RequestBody EventRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) @Size(min = 1, max = 128) String key) {
        validateOptionalKey(key);
        Map<String, Object> event = service.ingest(request, key);
        return ResponseEntity.accepted().location(URI.create("/api/v1/events/" + event.get("id"))).body(event);
    }

    @GetMapping("/events/{id}")
    public Map<String, Object> getEvent(@PathVariable UUID id) {
        return service.getEvent(id);
    }

    @PostMapping(value = "/subscriptions", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> createSubscription(@Valid @RequestBody SubscriptionRequest request) {
        Map<String, Object> subscription = service.createSubscription(request);
        return ResponseEntity.created(URI.create("/api/v1/subscriptions/" + subscription.get("id"))).body(subscription);
    }

    @GetMapping("/subscriptions")
    public List<Map<String, Object>> listSubscriptions(
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit,
            @RequestParam(defaultValue = "0") @Min(0) long after) {
        return service.listSubscriptions(limit, after);
    }

    @GetMapping("/subscriptions/{id}")
    public Map<String, Object> getSubscription(@PathVariable UUID id) {
        return service.getSubscription(id);
    }

    @DeleteMapping("/subscriptions/{id}")
    public ResponseEntity<Void> deleteSubscription(@PathVariable UUID id) {
        service.deleteSubscription(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/deliveries")
    public List<Map<String, Object>> listDeliveries(
            @RequestParam(required = false) UUID eventId,
            @RequestParam(required = false) UUID subscriptionId,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit,
            @RequestParam(defaultValue = "0") @Min(0) long after) {
        return service.listDeliveries(eventId, subscriptionId, limit, after);
    }

    @GetMapping("/deliveries/{id}")
    public Map<String, Object> getDelivery(@PathVariable UUID id) {
        return service.getDelivery(id);
    }

    @GetMapping("/deliveries/{id}/attempts")
    public List<Map<String, Object>> attempts(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit,
            @RequestParam(defaultValue = "0") @Min(0) long after) {
        return service.attempts(id, limit, after);
    }

    @PostMapping(value = "/subscriptions/{id}/replays", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> replay(
            @PathVariable UUID id,
            @Valid @RequestBody ReplayRequest request,
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 128) String key) {
        validateOptionalKey(key);
        return ResponseEntity.accepted().body(service.replay(id, request, key));
    }

    private static void validateOptionalKey(String key) {
        if (key != null && (key.isBlank() || key.chars().anyMatch(c -> c < 0x21 || c > 0x7e))) {
            throw new IllegalArgumentException("Idempotency-Key must contain only visible ASCII characters without spaces");
        }
    }
}
