package com.payflow.account.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.account.event.AccountDebitedEvent;
import com.payflow.account.event.PaymentInitiatedEvent;
import com.payflow.account.service.AccountService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PaymentEventConsumer.class);
    private static final String TOPIC = "payment.initiated";

    private final AccountService accountService;
    private final AccountEventProducer eventProducer;
    private final ObjectMapper objectMapper;

    public PaymentEventConsumer(
            AccountService accountService,
            AccountEventProducer eventProducer,
            ObjectMapper objectMapper) {
        this.accountService = accountService;
        this.eventProducer = eventProducer;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = TOPIC, groupId = "account-service-group")
    public void consumePaymentInitiated(String message) {
        logger.info("Received raw payment.initiated event: {}", message);
        PaymentInitiatedEvent event = null;
        try {
            event = objectMapper.readValue(message, PaymentInitiatedEvent.class);
            logger.info("Parsed event: {}", event);

            // Trigger debit operation
            accountService.withdraw(event.getAccountId(), event.getAmount());
            logger.info("Successfully debited account {} for amount {}", event.getAccountId(), event.getAmount());

            // Publish account.debited event on success
            AccountDebitedEvent debitedEvent = AccountDebitedEvent.builder()
                    .paymentId(event.getPaymentId())
                    .accountId(event.getAccountId())
                    .amount(event.getAmount())
                    .status("SUCCESS")
                    .build();
            eventProducer.publishAccountDebited(debitedEvent);

        } catch (Exception e) {
            logger.error("Failed to process payment.initiated event. Message: {}", message, e);
            if (event != null) {
                // Publish failed account.debited event so the Saga transitions to FAILED
                AccountDebitedEvent failedEvent = AccountDebitedEvent.builder()
                        .paymentId(event.getPaymentId())
                        .accountId(event.getAccountId())
                        .amount(event.getAmount())
                        .status("FAILED")
                        .build();
                eventProducer.publishAccountDebited(failedEvent);
            }
        }
    }

    @KafkaListener(topics = "payment.completed", groupId = "account-service-group")
    public void consumePaymentCompleted(String message) {
        logger.info("Received raw payment.completed event: {}", message);
        try {
            com.payflow.account.event.PaymentCompletedEvent event = objectMapper.readValue(message, com.payflow.account.event.PaymentCompletedEvent.class);
            logger.info("Parsed event: {}", event);

            if (event.getReceiverAccountId() != null && event.getAmount() != null) {
                // Trigger credit operation for the receiver
                accountService.deposit(event.getReceiverAccountId(), event.getAmount());
                logger.info("Successfully credited receiver account {} for amount {}", 
                        event.getReceiverAccountId(), event.getAmount());
            } else {
                logger.warn("Received payment.completed event but receiverAccountId or amount was null. Event: {}", event);
            }
        } catch (Exception e) {
            logger.error("Failed to process payment.completed event. Message: {}", message, e);
        }
    }

    @KafkaListener(topics = "payment.rollback", groupId = "account-service-group")
    public void consumePaymentRollback(String message) {
        logger.info("Received raw payment.rollback event: {}", message);
        try {
            com.payflow.account.event.PaymentRollbackEvent event = objectMapper.readValue(message, com.payflow.account.event.PaymentRollbackEvent.class);
            logger.info("Parsed rollback event: {}", event);

            if (event.getAccountId() != null && event.getAmount() != null) {
                // Refund/deposit the money back to the sender
                accountService.deposit(event.getAccountId(), event.getAmount());
                logger.info("Successfully rolled back payment. Refunded account {} for amount {}", 
                        event.getAccountId(), event.getAmount());
            } else {
                logger.warn("Received payment.rollback event but accountId or amount was null. Event: {}", event);
            }
        } catch (Exception e) {
            logger.error("Failed to process payment.rollback event. Message: {}", message, e);
        }
    }
}
