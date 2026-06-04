package com.payflow.transaction.service;

import com.payflow.transaction.client.AccountServiceClient;
import com.payflow.transaction.dto.AccountDto;
import com.payflow.transaction.dto.TransactionRequest;
import com.payflow.transaction.dto.TransactionResponse;
import com.payflow.transaction.entity.Transaction;
import com.payflow.transaction.entity.TransactionStatus;
import com.payflow.transaction.event.PaymentInitiatedEvent;
import com.payflow.transaction.event.PaymentRollbackEvent;
import com.payflow.transaction.messaging.PaymentEventProducer;
import com.payflow.transaction.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class TransactionService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionService.class);

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final PaymentEventProducer eventProducer;

    public TransactionService(
            TransactionRepository transactionRepository,
            AccountServiceClient accountServiceClient,
            PaymentEventProducer eventProducer) {
        this.transactionRepository = transactionRepository;
        this.accountServiceClient = accountServiceClient;
        this.eventProducer = eventProducer;
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

        // 3. Trigger asynchronous Saga flow by publishing to payment.initiated
        PaymentInitiatedEvent initiatedEvent = PaymentInitiatedEvent.builder()
                .paymentId(saved.getId())
                .accountId(saved.getSenderAccountId())
                .amount(saved.getAmount())
                .build();
        eventProducer.publishPaymentInitiated(correlationId, initiatedEvent);

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
    public void compensate(UUID transactionId, String reason) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found with ID: " + transactionId));
        UUID correlationId = transaction.getCorrelationId();

        logger.warn("[CorrelationID: {}] Compelling saga compensation for transaction {}. Reason: {}",
                correlationId, transactionId, reason);

        // Transition state to FAILED
        transaction.setStatus(TransactionStatus.FAILED);
        transactionRepository.save(transaction);

        // Publish compensation rollback event
        PaymentRollbackEvent rollbackEvent = PaymentRollbackEvent.builder()
                .paymentId(transactionId)
                .accountId(transaction.getSenderAccountId())
                .amount(transaction.getAmount())
                .reason(reason)
                .build();
        eventProducer.publishPaymentRollback(correlationId, rollbackEvent);
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
