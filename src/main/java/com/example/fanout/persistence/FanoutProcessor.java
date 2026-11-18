package com.example.fanout.persistence;

import com.example.fanout.domain.FilterRule;
import com.example.fanout.filter.FilterMatcher;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class FanoutProcessor {
    private final JdbcTemplate jdbc;
    private final JsonCodec json;
    private final FilterMatcher matcher;
    private final NotificationService service;
    private final MeterRegistry metrics;
    public FanoutProcessor(JdbcTemplate jdbc, JsonCodec json, FilterMatcher matcher, NotificationService service, MeterRegistry metrics) {
        this.jdbc = jdbc; this.json = json; this.matcher = matcher; this.service = service; this.metrics = metrics;
    }
    @Transactional
    public void process(UUID eventId) {
        var rows = jdbc.queryForList("SELECT * FROM events WHERE id = ? FOR UPDATE", eventId);
        if (rows.isEmpty()) throw new IllegalArgumentException("Broker event does not exist: " + eventId);
        var event = rows.getFirst();
        if (Boolean.TRUE.equals(event.get("fanout_completed"))) return;
        var payload = json.read(event.get("payload").toString());
        String type = (String) event.get("type"); String source = (String) event.get("source");
        long sequence = ((Number) event.get("seq")).longValue();
        var subscriptions = jdbc.queryForList("""
                SELECT s.id, s.filter FROM subscriptions s
                JOIN event_subscriptions es ON es.subscription_id = s.id
                WHERE es.event_id = ? ORDER BY s.seq
                """, eventId);
        for (var subscription : subscriptions) {
            FilterRule filter = json.read(subscription.get("filter").toString(), FilterRule.class);
            if (matcher.matches(filter, type, source, payload)) {
                service.insertDelivery(eventId, (UUID) subscription.get("id"), null, source, sequence, sequence);
            }
        }
        jdbc.update("UPDATE events SET fanout_completed = true WHERE id = ?", eventId);
        metrics.counter("fanout.events.processed").increment();
    }
}
