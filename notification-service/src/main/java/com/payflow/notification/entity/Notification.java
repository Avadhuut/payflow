package com.payflow.notification.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "notifications")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(columnDefinition = "BINARY(16)")
    private UUID userId;

    @Column(nullable = false, columnDefinition = "BINARY(16)")
    private UUID transactionId;

    @Column(nullable = false, length = 500)
    private String message;

    @Column(nullable = false)
    private String status; // PENDING, SENT, FAILED

    @Column(nullable = false, columnDefinition = "BINARY(16)")
    private UUID correlationId;

    @Column(nullable = false)
    private LocalDateTime createdAt;
}
