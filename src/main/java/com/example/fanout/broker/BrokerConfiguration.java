package com.example.fanout.broker;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@ConditionalOnProperty(name = "fanout.broker.enabled", havingValue = "true", matchIfMissing = true)
public class BrokerConfiguration {
    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        // Never silently skip a record on exhausted consumer retries. Operational intervention
        // is required for poison records; the source event remains durable in PostgreSQL.
        var handler = new DefaultErrorHandler(new FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(java.util.Map.of(), true);
        return handler;
    }
}
