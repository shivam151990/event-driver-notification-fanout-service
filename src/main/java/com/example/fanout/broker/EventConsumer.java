package com.example.fanout.broker;

import com.example.fanout.persistence.FanoutProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
@ConditionalOnProperty(name = "fanout.broker.enabled", havingValue = "true", matchIfMissing = true)
public class EventConsumer {
    private final FanoutProcessor processor;
    public EventConsumer(FanoutProcessor processor) { this.processor = processor; }
    @KafkaListener(topics = "${fanout.broker.topic}")
    public void consume(String eventId) {
        // Listener acknowledges only after this separate bean's DB transaction commits.
        processor.process(UUID.fromString(eventId));
    }
}
