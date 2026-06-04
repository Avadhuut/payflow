package com.payflow.transaction.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.transaction.event.PaymentInitiatedEvent;
import com.payflow.transaction.event.PaymentRollbackEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class PaymentEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(PaymentEventProducer.class);
    private static final String INITIATED_TOPIC = "payment.initiated";
    private static final String ROLLBACK_TOPIC = "payment.rollback";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentEventProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishPaymentInitiated(UUID correlationId, PaymentInitiatedEvent event) {
        try {
            String message = objectMapper.writeValueAsString(event);
            logger.info("[CorrelationID: {}] Publishing to topic {}: {}", correlationId, INITIATED_TOPIC, message);
            kafkaTemplate.send(INITIATED_TOPIC, event.getPaymentId().toString(), message);
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Failed to publish payment.initiated event", correlationId, e);
        }
    }

    public void publishPaymentRollback(UUID correlationId, PaymentRollbackEvent event) {
        try {
            String message = objectMapper.writeValueAsString(event);
            logger.info("[CorrelationID: {}] Publishing to topic {}: {}", correlationId, ROLLBACK_TOPIC, message);
            kafkaTemplate.send(ROLLBACK_TOPIC, event.getPaymentId().toString(), message);
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Failed to publish payment.rollback event", correlationId, e);
        }
    }

    public void publishPaymentCompleted(UUID correlationId, UUID paymentId, UUID senderAccountId, UUID receiverAccountId, BigDecimal amount) {
        try {
            String message = objectMapper.writeValueAsString(java.util.Map.of(
                    "paymentId", paymentId,
                    "status", "COMPLETED",
                    "senderAccountId", senderAccountId,
                    "receiverAccountId", receiverAccountId,
                    "amount", amount,
                    "correlationId", correlationId
            ));
            logger.info("[CorrelationID: {}] Publishing to topic {}: {}", correlationId, "payment.completed", message);
            kafkaTemplate.send("payment.completed", paymentId.toString(), message);
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Failed to publish payment.completed event", correlationId, e);
        }
    }
}
