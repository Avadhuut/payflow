package com.payflow.fraud.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
public class AccountDto {
    private UUID id;
    private UUID userId;
    private BigDecimal balance;
    private LocalDateTime createdAt;
}
