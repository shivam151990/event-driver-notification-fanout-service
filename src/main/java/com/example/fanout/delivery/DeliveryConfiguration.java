package com.example.fanout.delivery;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DeliveryProperties.class)
public class DeliveryConfiguration {
    @Bean
    WebhookPolicy webhookPolicy(DeliveryProperties properties) {
        return new WebhookPolicy(properties.isAllowHttp(), properties.isAllowPrivateAddresses());
    }

    @Bean
    RetryPolicy retryPolicy(DeliveryProperties properties) {
        return new RetryPolicy(properties.getInitialBackoff(), properties.getMaxBackoff(), properties.getMaxAttempts());
    }
}
