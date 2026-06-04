package com.payflow.ledger.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.ledger.entity.LedgerEntry;
import com.payflow.ledger.repository.LedgerEntryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class LedgerEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(LedgerEventConsumer.class);
    private final LedgerEntryRepository ledgerEntryRepository;
    private final ObjectMapper objectMapper;

    public LedgerEventConsumer(LedgerEntryRepository ledgerEntryRepository, ObjectMapper objectMapper) {
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = {
            "payment.initiated",
            "account.debited",
            "fraud.cleared",
            "fraud.flagged",
            "payment.completed",
            "payment.rollback",
            "notification.sent"
        },
        groupId = "ledger-service-group"
    )
    public void consume(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        try {
            JsonNode node = objectMapper.readTree(message);

            UUID transactionId = null;
            if (node.has("paymentId")) {
                transactionId = UUID.fromString(node.get("paymentId").asText());
            } else if (node.has("transactionId")) {
                transactionId = UUID.fromString(node.get("transactionId").asText());
            } else {
                transactionId = UUID.randomUUID();
            }

            UUID correlationId = null;
            if (node.has("correlationId")) {
                correlationId = UUID.fromString(node.get("correlationId").asText());
            } else {
                correlationId = UUID.randomUUID();
            }

            logger.info("[CorrelationID: {}] Consumed event from topic '{}' for transaction: {}", 
                    correlationId, topic, transactionId);

            LedgerEntry entry = LedgerEntry.builder()
                    .eventType(topic)
                    .transactionId(transactionId)
                    .payload(message)
                    .correlationId(correlationId)
                    .createdAt(LocalDateTime.now())
                    .build();

            ledgerEntryRepository.save(entry);

        } catch (Exception e) {
            logger.error("Failed to process event from topic '{}'. Raw message: {}", topic, message, e);
        }
    }
}
