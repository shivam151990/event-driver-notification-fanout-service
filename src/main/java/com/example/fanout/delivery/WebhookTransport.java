package com.example.fanout.delivery;

import java.util.UUID;

public interface WebhookTransport {
    Result send(String url, UUID eventId, UUID deliveryId, int attempt, String secret, byte[] body);

    record Result(Integer httpStatus, String error, String retryAfter, boolean permanentFailure) {
        public boolean delivered() {
            return httpStatus != null && httpStatus >= 200 && httpStatus < 300;
        }
    }
}
