package com.payflow.notification.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.notification.client.AccountServiceClient;
import com.payflow.notification.client.AuthServiceClient;
import com.payflow.notification.dto.AccountDto;
import com.payflow.notification.dto.UserDto;
import com.payflow.notification.entity.Notification;
import com.payflow.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class NotificationEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(NotificationEventConsumer.class);

    private final NotificationRepository notificationRepository;
    private final ObjectMapper objectMapper;
    private final AccountServiceClient accountServiceClient;
    private final AuthServiceClient authServiceClient;
    private final NotificationEventProducer notificationEventProducer;

    public NotificationEventConsumer(
            NotificationRepository notificationRepository,
            ObjectMapper objectMapper,
            AccountServiceClient accountServiceClient,
            AuthServiceClient authServiceClient,
            NotificationEventProducer notificationEventProducer) {
        this.notificationRepository = notificationRepository;
        this.objectMapper = objectMapper;
        this.accountServiceClient = accountServiceClient;
        this.authServiceClient = authServiceClient;
        this.notificationEventProducer = notificationEventProducer;
    }

    private String getDisplayName(String email) {
        if (email == null || !email.contains("@")) return "User";
        String prefix = email.split("@")[0];
        return prefix.substring(0, 1).toUpperCase() + prefix.substring(1);
    }

    @KafkaListener(topics = "payment.completed", groupId = "notification-service-group")
    public void consumePaymentCompleted(String message) {
        logger.info("Notification received payment.completed event: {}", message);
        try {
            JsonNode node = objectMapper.readTree(message);
            UUID paymentId = UUID.fromString(node.get("paymentId").asText());
            UUID correlationId = node.has("correlationId") ? UUID.fromString(node.get("correlationId").asText()) : UUID.randomUUID();
            UUID senderAccountId = UUID.fromString(node.get("senderAccountId").asText());
            UUID receiverAccountId = UUID.fromString(node.get("receiverAccountId").asText());
            BigDecimal amount = new BigDecimal(node.get("amount").asText());

            // 1. Fetch sender details
            AccountDto senderAccount = accountServiceClient.getAccount(senderAccountId);
            UUID senderUserId = senderAccount.getUserId();
            UserDto senderUser = authServiceClient.getUser(senderUserId);
            String senderName = getDisplayName(senderUser.getEmail());

            // 2. Fetch receiver details
            AccountDto receiverAccount = accountServiceClient.getAccount(receiverAccountId);
            UUID receiverUserId = receiverAccount.getUserId();
            UserDto receiverUser = authServiceClient.getUser(receiverUserId);
            String receiverName = getDisplayName(receiverUser.getEmail());

            // 3. Create Rahul (sender) notification
            String senderMsg = String.format("You sent ₹%s to %s", amount.toString(), receiverName);
            logger.info("[CorrelationID: {}] SENT Notification for sender: {}", correlationId, senderMsg);
            Notification senderNotification = Notification.builder()
                    .userId(senderUserId)
                    .transactionId(paymentId)
                    .message(senderMsg)
                    .status("SENT")
                    .correlationId(correlationId)
                    .createdAt(LocalDateTime.now())
                    .build();
            notificationRepository.save(senderNotification);
            notificationEventProducer.publishNotificationSent(correlationId, paymentId, senderUserId, senderMsg, "SENT");

            // 4. Create Priya (receiver) notification
            String receiverMsg = String.format("You received ₹%s from %s", amount.toString(), senderName);
            logger.info("[CorrelationID: {}] SENT Notification for receiver: {}", correlationId, receiverMsg);
            Notification receiverNotification = Notification.builder()
                    .userId(receiverUserId)
                    .transactionId(paymentId)
                    .message(receiverMsg)
                    .status("SENT")
                    .correlationId(correlationId)
                    .createdAt(LocalDateTime.now())
                    .build();
            notificationRepository.save(receiverNotification);
            notificationEventProducer.publishNotificationSent(correlationId, paymentId, receiverUserId, receiverMsg, "SENT");

        } catch (Exception e) {
            logger.error("Failed to process payment.completed notification", e);
        }
    }

    @KafkaListener(topics = "fraud.flagged", groupId = "notification-service-group")
    public void consumeFraudFlagged(String message) {
        logger.info("Notification received fraud.flagged event: {}", message);
        try {
            JsonNode node = objectMapper.readTree(message);
            UUID paymentId = UUID.fromString(node.get("paymentId").asText());
            String reason = node.get("reason").asText();
            UUID correlationId = node.has("correlationId") ? UUID.fromString(node.get("correlationId").asText()) : UUID.randomUUID();
            UUID accountId = UUID.fromString(node.get("accountId").asText());

            // Fetch sender details
            AccountDto senderAccount = accountServiceClient.getAccount(accountId);
            UUID senderUserId = senderAccount.getUserId();

            String notificationMsg = String.format("Payment %s failed due to fraud checks. Reason: %s", paymentId, reason);
            logger.warn("[CorrelationID: {}] SENT Notification for fraud flagged: {}", correlationId, notificationMsg);

            Notification notification = Notification.builder()
                    .userId(senderUserId)
                    .transactionId(paymentId)
                    .message(notificationMsg)
                    .status("SENT")
                    .correlationId(correlationId)
                    .createdAt(LocalDateTime.now())
                    .build();
            notificationRepository.save(notification);
            notificationEventProducer.publishNotificationSent(correlationId, paymentId, senderUserId, notificationMsg, "SENT");

        } catch (Exception e) {
            logger.error("Failed to process fraud.flagged notification", e);
        }
    }

    @KafkaListener(topics = "payment.rollback", groupId = "notification-service-group")
    public void consumePaymentRollback(String message) {
        logger.info("Notification received payment.rollback event: {}", message);
        try {
            JsonNode node = objectMapper.readTree(message);
            UUID paymentId = UUID.fromString(node.get("paymentId").asText());
            String reason = node.get("reason").asText();
            UUID correlationId = node.has("correlationId") ? UUID.fromString(node.get("correlationId").asText()) : UUID.randomUUID();
            UUID accountId = UUID.fromString(node.get("accountId").asText());
            BigDecimal amount = new BigDecimal(node.get("amount").asText());

            // Fetch sender details
            AccountDto senderAccount = accountServiceClient.getAccount(accountId);
            UUID senderUserId = senderAccount.getUserId();

            String notificationMsg = String.format("Your payment of ₹%s was blocked. Reason: %s. Money returned to your account.", amount.toString(), reason);
            logger.warn("[CorrelationID: {}] SENT Notification for payment rollback: {}", correlationId, notificationMsg);

            Notification notification = Notification.builder()
                    .userId(senderUserId)
                    .transactionId(paymentId)
                    .message(notificationMsg)
                    .status("SENT")
                    .correlationId(correlationId)
                    .createdAt(LocalDateTime.now())
                    .build();
            notificationRepository.save(notification);
            notificationEventProducer.publishNotificationSent(correlationId, paymentId, senderUserId, notificationMsg, "SENT");

        } catch (Exception e) {
            logger.error("Failed to process payment.rollback notification", e);
        }
    }
}
