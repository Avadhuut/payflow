package com.payflow.transaction.event;

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
public class PaymentInitiatedEvent {
    private UUID paymentId;
    private UUID accountId;
    private BigDecimal amount;
}
