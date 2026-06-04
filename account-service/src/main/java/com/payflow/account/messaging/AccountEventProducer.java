package com.payflow.account.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.account.event.AccountDebitedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AccountEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(AccountEventProducer.class);
    private static final String TOPIC = "account.debited";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public AccountEventProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishAccountDebited(AccountDebitedEvent event) {
        try {
            String message = objectMapper.writeValueAsString(event);
            logger.info("Publishing account.debited event: {}", message);
            kafkaTemplate.send(TOPIC, event.getPaymentId().toString(), message);
        } catch (Exception e) {
            logger.error("Failed to publish account.debited event for paymentId: {}", event.getPaymentId(), e);
        }
    }
}
