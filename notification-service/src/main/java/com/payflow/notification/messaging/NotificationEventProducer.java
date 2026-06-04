package com.payflow.notification.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class NotificationEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(NotificationEventProducer.class);
    private static final String TOPIC = "notification.sent";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public NotificationEventProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishNotificationSent(UUID correlationId, UUID paymentId, UUID userId, String message, String status) {
        try {
            String payload = objectMapper.writeValueAsString(java.util.Map.of(
                    "paymentId", paymentId,
                    "correlationId", correlationId,
                    "userId", userId,
                    "message", message,
                    "status", status
            ));
            logger.info("[CorrelationID: {}] Publishing to topic {}: {}", correlationId, TOPIC, payload);
            kafkaTemplate.send(TOPIC, paymentId.toString(), payload);
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Failed to publish notification.sent event", correlationId, e);
        }
    }
}
