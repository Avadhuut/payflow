package com.payflow.transaction.controller;

import com.payflow.transaction.dto.TransactionRequest;
import com.payflow.transaction.dto.TransactionResponse;
import com.payflow.transaction.service.IdempotencyService;
import com.payflow.transaction.service.TransactionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class TransactionControllerTest {

    @Mock
    private TransactionService transactionService;

    @Mock
    private IdempotencyService idempotencyService;

    @InjectMocks
    private TransactionController transactionController;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testIdempotencyGuardCacheHit() {
        String idempotencyKey = "test-key-123";
        TransactionRequest request = new TransactionRequest();
        request.setSenderAccountId(UUID.randomUUID());
        request.setReceiverAccountId(UUID.randomUUID());
        request.setAmount(new BigDecimal("100.00"));

        TransactionResponse firstResponse = new TransactionResponse();
        firstResponse.setTransactionId(UUID.randomUUID());
        firstResponse.setSenderAccountId(request.getSenderAccountId());
        firstResponse.setReceiverAccountId(request.getReceiverAccountId());
        firstResponse.setAmount(request.getAmount());

        // First call: cache miss, lease acquired, executes business logic, caches response
        when(idempotencyService.getCachedResponse(idempotencyKey, TransactionResponse.class)).thenReturn(null);
        when(idempotencyService.acquireLease(idempotencyKey)).thenReturn(true);
        when(transactionService.createTransaction(eq(request), any(UUID.class))).thenReturn(firstResponse);

        ResponseEntity<?> response1 = transactionController.createTransaction(idempotencyKey, request);
        assertEquals(HttpStatus.OK, response1.getStatusCode());
        assertEquals(firstResponse, response1.getBody());

        // Verify that transactionService.createTransaction was called
        verify(transactionService, times(1)).createTransaction(eq(request), any(UUID.class));
        verify(idempotencyService, times(1)).cacheResponse(idempotencyKey, firstResponse);

        // Second call with same key: cache hit!
        when(idempotencyService.getCachedResponse(idempotencyKey, TransactionResponse.class)).thenReturn(firstResponse);

        ResponseEntity<?> response2 = transactionController.createTransaction(idempotencyKey, request);
        assertEquals(HttpStatus.OK, response2.getStatusCode());
        assertEquals(firstResponse, response2.getBody());

        // Verify transactionService was NOT called again (still total 1 time)
        verify(transactionService, times(1)).createTransaction(eq(request), any(UUID.class));
        // Verify idempotencyService.acquireLease was NOT called again (still total 1 time)
        verify(idempotencyService, times(1)).acquireLease(idempotencyKey);
    }
}
