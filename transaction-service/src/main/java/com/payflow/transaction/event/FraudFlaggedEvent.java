package com.payflow.transaction.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FraudFlaggedEvent {
    private UUID paymentId;
    private UUID accountId;
    private BigDecimal amount;
    private String reason;
}
