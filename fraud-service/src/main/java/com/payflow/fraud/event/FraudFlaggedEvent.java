package com.payflow.fraud.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudFlaggedEvent {
    private UUID paymentId;
    private UUID accountId;
    private BigDecimal amount;
    private String reason;
}
