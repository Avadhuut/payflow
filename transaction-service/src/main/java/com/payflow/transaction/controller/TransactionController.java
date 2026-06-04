package com.payflow.transaction.controller;

import com.payflow.transaction.dto.TransactionRequest;
import com.payflow.transaction.dto.TransactionResponse;
import com.payflow.transaction.service.IdempotencyService;
import com.payflow.transaction.service.TransactionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final TransactionService transactionService;
    private final IdempotencyService idempotencyService;

    public TransactionController(TransactionService transactionService, IdempotencyService idempotencyService) {
        this.transactionService = transactionService;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping
    public ResponseEntity<?> createTransaction(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody TransactionRequest request) {

        if (idempotencyKey == null || idempotencyKey.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing mandatory 'Idempotency-Key' header"));
        }

        // 1. Check if response is already cached in Redis
        TransactionResponse cached = idempotencyService.getCachedResponse(idempotencyKey, TransactionResponse.class);
        if (cached != null) {
            return ResponseEntity.ok(cached);
        }

        // 2. Acquire execution lease
        boolean leaseAcquired = idempotencyService.acquireLease(idempotencyKey);
        if (!leaseAcquired) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "A concurrent request with the same Idempotency-Key is currently processing"));
        }

        try {
            // Generate correlation ID
            UUID correlationId = UUID.randomUUID();
            
            // Process the transaction
            TransactionResponse response = transactionService.createTransaction(request, correlationId);
            
            // Cache final response for 24 hours
            idempotencyService.cacheResponse(idempotencyKey, response);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            idempotencyService.releaseLease(idempotencyKey);
            throw e; // Handled by RestControllerAdvice
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getTransaction(@PathVariable UUID id) {
        TransactionResponse response = transactionService.getTransaction(id);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/account/{accountId}")
    public ResponseEntity<?> getTransactionsByAccount(@PathVariable UUID accountId) {
        return ResponseEntity.ok(transactionService.getTransactionsByAccount(accountId));
    }
}
