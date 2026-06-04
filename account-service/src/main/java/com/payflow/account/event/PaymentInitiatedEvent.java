package com.payflow.account.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentInitiatedEvent {
    private UUID paymentId;
    private UUID accountId;
    private BigDecimal amount;
}
