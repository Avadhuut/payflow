package com.payflow.account.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class TransactionRequest {
    private BigDecimal amount;
}
