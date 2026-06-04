package com.payflow.transaction.service;

import com.payflow.transaction.client.AccountServiceClient;
import com.payflow.transaction.dto.AccountDto;
import com.payflow.transaction.dto.TransactionRequest;
import com.payflow.transaction.dto.TransactionResponse;
import com.payflow.transaction.entity.Transaction;
import com.payflow.transaction.entity.TransactionStatus;
import com.payflow.transaction.entity.OutboxEvent;
import com.payflow.transaction.entity.OutboxStatus;
import com.payflow.transaction.event.PaymentInitiatedEvent;
import com.payflow.transaction.event.PaymentRollbackEvent;
import com.payflow.transaction.repository.TransactionRepository;
import com.payflow.transaction.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class TransactionService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionService.class);

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public TransactionService(
            TransactionRepository transactionRepository,
            AccountServiceClient accountServiceClient,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {
        this.transactionRepository = transactionRepository;
        this.accountServiceClient = accountServiceClient;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public TransactionResponse createTransaction(TransactionRequest request, UUID correlationId) {
        logger.info("[CorrelationID: {}] Initiating transaction from account {} to {} for amount {}",
                correlationId, request.getSenderAccountId(), request.getReceiverAccountId(), request.getAmount());

        // 1. Synchronous validation of accounts via Feign client
        try {
            AccountDto sender = accountServiceClient.getAccount(request.getSenderAccountId());
            AccountDto receiver = accountServiceClient.getAccount(request.getReceiverAccountId());
            if (sender == null || receiver == null) {
                throw new IllegalArgumentException("One or both accounts do not exist");
            }
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Account validation failed", correlationId, e);
            throw new IllegalArgumentException("Account validation failed: " + e.getMessage());
        }

        // 2. Create database entry in INITIATED state
        Transaction transaction = Transaction.builder()
                .senderAccountId(request.getSenderAccountId())
                .receiverAccountId(request.getReceiverAccountId())
                .amount(request.getAmount())
                .status(TransactionStatus.INITIATED)
                .correlationId(correlationId)
                .build();
        Transaction saved = transactionRepository.save(transaction);

        // 3. Create OutboxEvent for asynchronous Saga flow (instead of direct Kafka publish)
        PaymentInitiatedEvent initiatedEvent = PaymentInitiatedEvent.builder()
                .paymentId(saved.getId())
                .accountId(saved.getSenderAccountId())
                .amount(saved.getAmount())
                .build();
        try {
            String payload = objectMapper.writeValueAsString(initiatedEvent);
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("Transaction")
                    .aggregateId(saved.getId())
                    .eventType("payment.initiated")
                    .payload(payload)
                    .status(OutboxStatus.PENDING)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxEventRepository.save(outboxEvent);
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Failed to write outbox event for payment.initiated", correlationId, e);
            throw new RuntimeException("Failed to write outbox event", e);
        }

        return mapToResponse(saved);
    }

    @Transactional
    public void updateStatus(UUID transactionId, TransactionStatus newStatus) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found with ID: " + transactionId));
        UUID correlationId = transaction.getCorrelationId();

        logger.info("[CorrelationID: {}] Transitioning transaction {} state from {} to {}",
                correlationId, transactionId, transaction.getStatus(), newStatus);

        transaction.setStatus(newStatus);
        transactionRepository.save(transaction);
    }

    @Transactional
    public void completeTransaction(UUID transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found with ID: " + transactionId));
        UUID correlationId = transaction.getCorrelationId();

        logger.info("[CorrelationID: {}] Transitioning transaction {} state from {} to COMPLETED",
                correlationId, transactionId, transaction.getStatus());

        transaction.setStatus(TransactionStatus.COMPLETED);
        transactionRepository.save(transaction);

        // Create OutboxEvent for payment.completed (instead of direct Kafka publish)
        try {
            String payload = objectMapper.writeValueAsString(java.util.Map.of(
                    "paymentId", transactionId,
                    "status", "COMPLETED",
                    "senderAccountId", transaction.getSenderAccountId(),
                    "receiverAccountId", transaction.getReceiverAccountId(),
                    "amount", transaction.getAmount(),
                    "correlationId", correlationId
            ));
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("Transaction")
                    .aggregateId(transactionId)
                    .eventType("payment.completed")
                    .payload(payload)
                    .status(OutboxStatus.PENDING)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxEventRepository.save(outboxEvent);
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Failed to write outbox event for payment.completed", correlationId, e);
            throw new RuntimeException("Failed to write outbox event", e);
        }
    }

    @Transactional
    public void compensate(UUID transactionId, String reason) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found with ID: " + transactionId));
        UUID correlationId = transaction.getCorrelationId();

        logger.warn("[CorrelationID: {}] Compelling saga compensation for transaction {}. Reason: {}",
                correlationId, transactionId, reason);

        // Transition state to FAILED
        transaction.setStatus(TransactionStatus.FAILED);
        transactionRepository.save(transaction);

        // Create OutboxEvent for payment.rollback (instead of direct Kafka publish)
        PaymentRollbackEvent rollbackEvent = PaymentRollbackEvent.builder()
                .paymentId(transactionId)
                .accountId(transaction.getSenderAccountId())
                .amount(transaction.getAmount())
                .reason(reason)
                .build();
        try {
            String payload = objectMapper.writeValueAsString(rollbackEvent);
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("Transaction")
                    .aggregateId(transactionId)
                    .eventType("payment.rollback")
                    .payload(payload)
                    .status(OutboxStatus.PENDING)
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxEventRepository.save(outboxEvent);
        } catch (Exception e) {
            logger.error("[CorrelationID: {}] Failed to write outbox event for payment.rollback", correlationId, e);
            throw new RuntimeException("Failed to write outbox event", e);
        }
    }

    @Transactional(readOnly = true)
    public TransactionResponse getTransaction(UUID id) {
        Transaction transaction = transactionRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found with ID: " + id));
        return mapToResponse(transaction);
    }

    @Transactional(readOnly = true)
    public List<TransactionResponse> getTransactionsByAccount(UUID accountId) {
        return transactionRepository.findBySenderAccountIdOrReceiverAccountId(accountId, accountId)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private TransactionResponse mapToResponse(Transaction tx) {
        return TransactionResponse.builder()
                .transactionId(tx.getId())
                .senderAccountId(tx.getSenderAccountId())
                .receiverAccountId(tx.getReceiverAccountId())
                .amount(tx.getAmount())
                .status(tx.getStatus().name())
                .correlationId(tx.getCorrelationId())
                .build();
    }
}
