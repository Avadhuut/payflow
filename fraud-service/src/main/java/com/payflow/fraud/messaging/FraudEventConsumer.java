package com.payflow.fraud.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.fraud.entity.FraudCheck;
import com.payflow.fraud.event.FraudClearedEvent;
import com.payflow.fraud.event.FraudFlaggedEvent;
import com.payflow.fraud.event.PaymentInitiatedEvent;
import com.payflow.fraud.service.FraudScoringService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class FraudEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(FraudEventConsumer.class);
    private static final String INITIATED_TOPIC = "payment.initiated";
    private static final String CLEARED_TOPIC = "fraud.cleared";
    private static final String FLAGGED_TOPIC = "fraud.flagged";

    private final FraudScoringService scoringService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public FraudEventConsumer(
            FraudScoringService scoringService,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this.scoringService = scoringService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = INITIATED_TOPIC, groupId = "fraud-service-group")
    public void consumePaymentInitiated(String message) {
        logger.info("Fraud consumer received raw event: {}", message);
        try {
            PaymentInitiatedEvent event = objectMapper.readValue(message, PaymentInitiatedEvent.class);
            
            // Extract correlationId or fallback to a new one
            UUID correlationId = UUID.randomUUID();
            logger.info("[CorrelationID: {}] Consumed event for transaction: {}", correlationId, event.getPaymentId());

            // Run rule scoring engine
            FraudCheck check = scoringService.executeScoring(
                    correlationId,
                    event.getPaymentId(),
                    event.getAccountId(),
                    event.getAmount()
            );

            // Publish result to Kafka
            if ("CLEARED".equalsIgnoreCase(check.getDecision())) {
                FraudClearedEvent clearedEvent = FraudClearedEvent.builder()
                        .paymentId(event.getPaymentId())
                        .accountId(event.getAccountId())
                        .amount(event.getAmount())
                        .status("CLEARED")
                        .build();
                String outMsg = objectMapper.writeValueAsString(clearedEvent);
                logger.info("[CorrelationID: {}] Publishing to topic {}: {}", correlationId, CLEARED_TOPIC, outMsg);
                kafkaTemplate.send(CLEARED_TOPIC, event.getPaymentId().toString(), outMsg);
            } else {
                FraudFlaggedEvent flaggedEvent = FraudFlaggedEvent.builder()
                        .paymentId(event.getPaymentId())
                        .accountId(event.getAccountId())
                        .amount(event.getAmount())
                        .reason("Fraud score exceeded limit: " + check.getScore())
                        .build();
                String outMsg = objectMapper.writeValueAsString(flaggedEvent);
                logger.info("[CorrelationID: {}] Publishing to topic {}: {}", correlationId, FLAGGED_TOPIC, outMsg);
                kafkaTemplate.send(FLAGGED_TOPIC, event.getPaymentId().toString(), outMsg);
            }

        } catch (Exception e) {
            logger.error("Failed to process fraud check for message: {}", message, e);
        }
    }
}
