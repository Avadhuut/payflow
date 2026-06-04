package com.payflow.fraud.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "fraud_checks")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudCheck {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(nullable = false, columnDefinition = "BINARY(16)")
    private UUID transactionId;

    @Column(nullable = false, columnDefinition = "BINARY(16)")
    private UUID senderAccountId;

    @Column(nullable = false)
    private int score;

    @Column(nullable = false)
    private String decision;

    @Column(nullable = false, columnDefinition = "BINARY(16)")
    private UUID correlationId;

    @Column(nullable = false)
    private LocalDateTime createdAt;
}
