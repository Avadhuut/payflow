package com.payflow.transaction.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Data
public class TransactionRequest {
    private UUID senderAccountId;
    private UUID receiverAccountId;
    private BigDecimal amount;
}
