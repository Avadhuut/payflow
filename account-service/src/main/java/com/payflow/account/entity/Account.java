package com.payflow.account.entity;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "accounts")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Account {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(nullable = false)
    private UUID userId;

    @Column(nullable = false, precision = 18, scale = 2)
    @Setter(AccessLevel.NONE) // Restrict direct balance modifications
    private BigDecimal balance;

    @Version
    private Long version;

    @Builder.Default
    @Column(nullable = false)
    private java.time.LocalDateTime createdAt = java.time.LocalDateTime.now();

    public void debit(BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Debit amount must be greater than zero");
        }
        if (this.balance == null || this.balance.compareTo(amount) < 0) {
            throw new IllegalStateException("Insufficient funds");
        }
        this.balance = this.balance.subtract(amount);
    }

    public void credit(BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Credit amount must be greater than zero");
        }
        if (this.balance == null) {
            this.balance = BigDecimal.ZERO;
        }
        this.balance = this.balance.add(amount);
    }
}
