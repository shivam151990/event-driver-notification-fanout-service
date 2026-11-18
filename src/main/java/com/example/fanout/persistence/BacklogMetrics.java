package com.example.fanout.persistence;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class BacklogMetrics {
    public BacklogMetrics(JdbcTemplate jdbc, MeterRegistry metrics) {
        register(jdbc, metrics, "fanout.outbox.pending", "SELECT count(*) FROM outbox WHERE published_at IS NULL");
        register(jdbc, metrics, "fanout.outbox.oldest.seconds", "SELECT coalesce(extract(epoch FROM clock_timestamp()-min(created_at)),0) FROM outbox WHERE published_at IS NULL");
        register(jdbc, metrics, "fanout.events.unmatched", "SELECT count(*) FROM events WHERE NOT fanout_completed");
        register(jdbc, metrics, "fanout.deliveries.pending", "SELECT count(*) FROM deliveries WHERE status='PENDING'");
        register(jdbc, metrics, "fanout.deliveries.failed", "SELECT count(*) FROM deliveries WHERE status='FAILED'");
    }
    private void register(JdbcTemplate jdbc, MeterRegistry metrics, String name, String sql) {
        Gauge.builder(name, jdbc, db -> {
            try { Number value = db.queryForObject(sql, Number.class); return value == null ? 0 : value.doubleValue(); }
            catch (RuntimeException e) { return Double.NaN; }
        }).register(metrics);
    }
}
