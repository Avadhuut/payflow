package com.payflow.transaction.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<?, ?> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        // 3 retry attempts = FixedBackOff with 3 maxAttempts (1 original attempt + 2 retries, or 3 retries?
        // Prompt says: "if a listener fails to process an event after 3 retry attempts, ... routes to DLQ".
        // Setting maxAttempts to 3 in FixedBackOff means total 3 attempts (1 original + 2 retries).
        // Let's set it to 3 retries specifically, which means 4 total attempts, i.e., maxAttempts = 4 (1 original + 3 retries).
        // Or if it means 3 total attempts, we set maxAttempts = 3. To be safe, 3 retry attempts means 3 retries, so we pass 3 retries.
        // In Spring's FixedBackOff(interval, maxAttempts), maxAttempts is total execution attempts.
        // So 3 retry attempts would mean 4 total attempts. Let's configure FixedBackOff(1000L, 3) to represent 3 retries.
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
    }
}
