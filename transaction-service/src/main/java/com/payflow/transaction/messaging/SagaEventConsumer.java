package com.payflow.transaction.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.transaction.dto.TransactionResponse;
import com.payflow.transaction.entity.TransactionStatus;
import com.payflow.transaction.event.AccountDebitedEvent;
import com.payflow.transaction.event.FraudClearedEvent;
import com.payflow.transaction.event.FraudFlaggedEvent;
import com.payflow.transaction.service.TransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class SagaEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SagaEventConsumer.class);

    private final TransactionService transactionService;
    private final PaymentEventProducer eventProducer;
    private final ObjectMapper objectMapper;

    public SagaEventConsumer(
            TransactionService transactionService,
            PaymentEventProducer eventProducer,
            ObjectMapper objectMapper) {
        this.transactionService = transactionService;
        this.eventProducer = eventProducer;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "account.debited", groupId = "transaction-service-group")
    public void consumeAccountDebited(String message) {
        logger.info("Saga Consumer received account.debited event: {}", message);
        try {
            AccountDebitedEvent event = objectMapper.readValue(message, AccountDebitedEvent.class);
            UUID paymentId = event.getPaymentId();
            
            // Retrieve correlationId from transaction context
            TransactionResponse tx = transactionService.getTransaction(paymentId);
            UUID correlationId = tx.getCorrelationId();

            if ("SUCCESS".equalsIgnoreCase(event.getStatus())) {
                logger.info("[CorrelationID: {}] Account debited successfully. Transitioning transaction {} to PROCESSING",
                        correlationId, paymentId);
                transactionService.updateStatus(paymentId, TransactionStatus.PROCESSING);
            } else {
                logger.warn("[CorrelationID: {}] Account debit failed. Status: {}. Transitioning to FAILED and triggering compensation",
                        correlationId, event.getStatus());
                transactionService.compensate(paymentId, "Account debit failed: " + event.getStatus());
            }
        } catch (Exception e) {
            logger.error("Failed to process account.debited event", e);
        }
    }

    @KafkaListener(topics = "fraud.cleared", groupId = "transaction-service-group")
    public void consumeFraudCleared(String message) {
        logger.info("Saga Consumer received fraud.cleared event: {}", message);
        try {
            FraudClearedEvent event = objectMapper.readValue(message, FraudClearedEvent.class);
            UUID paymentId = event.getPaymentId();

            TransactionResponse tx = transactionService.getTransaction(paymentId);
            UUID correlationId = tx.getCorrelationId();

            logger.info("[CorrelationID: {}] Fraud check cleared. Transitioning transaction {} to COMPLETED",
                    correlationId, paymentId);
            transactionService.updateStatus(paymentId, TransactionStatus.COMPLETED);

            // Publish final payment.completed event
            eventProducer.publishPaymentCompleted(correlationId, paymentId, tx.getSenderAccountId(), tx.getReceiverAccountId(), tx.getAmount());
        } catch (Exception e) {
            logger.error("Failed to process fraud.cleared event", e);
        }
    }

    @KafkaListener(topics = "fraud.flagged", groupId = "transaction-service-group")
    public void consumeFraudFlagged(String message) {
        logger.info("Saga Consumer received fraud.flagged event: {}", message);
        try {
            FraudFlaggedEvent event = objectMapper.readValue(message, FraudFlaggedEvent.class);
            UUID paymentId = event.getPaymentId();

            TransactionResponse tx = transactionService.getTransaction(paymentId);
            UUID correlationId = tx.getCorrelationId();

            logger.warn("[CorrelationID: {}] Fraud check flagged transaction {}. Transitioning to FAILED and triggering compensation",
                    correlationId, paymentId);
            transactionService.compensate(paymentId, "Fraud check flagged: " + event.getReason());
        } catch (Exception e) {
            logger.error("Failed to process fraud.flagged event", e);
        }
    }

    @KafkaListener(topics = "account.insufficient", groupId = "transaction-service-group")
    public void consumeAccountInsufficient(String message) {
        logger.info("Saga Consumer received account.insufficient event: {}", message);
        try {
            FraudFlaggedEvent event = objectMapper.readValue(message, FraudFlaggedEvent.class);
            UUID paymentId = event.getPaymentId();

            TransactionResponse tx = transactionService.getTransaction(paymentId);
            UUID correlationId = tx.getCorrelationId();

            logger.warn("[CorrelationID: {}] Insufficient account balance event consumed for transaction {}. Transitioning to FAILED and triggering compensation",
                    correlationId, paymentId);
            transactionService.compensate(paymentId, "Insufficient account balance: " + event.getReason());
        } catch (Exception e) {
            logger.error("Failed to process account.insufficient event", e);
        }
    }
}
