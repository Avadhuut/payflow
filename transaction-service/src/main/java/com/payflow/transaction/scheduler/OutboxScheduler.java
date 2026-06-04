package com.payflow.transaction.scheduler;

import com.payflow.transaction.entity.OutboxEvent;
import com.payflow.transaction.entity.OutboxStatus;
import com.payflow.transaction.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxScheduler {

    private static final Logger logger = LoggerFactory.getLogger(OutboxScheduler.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxScheduler(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 2000) // Polls every 2 seconds
    public void processOutboxEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);
        if (pendingEvents.isEmpty()) {
            return;
        }

        logger.info("Found {} pending outbox events to process", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                logger.info("Outbox processing event {} for topic {}", event.getId(), event.getEventType());
                
                // Publish to Kafka using the eventType as the topic name and aggregateId as the key
                CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(
                        event.getEventType(),
                        event.getAggregateId().toString(),
                        event.getPayload()
                );

                // Wait for the broker acknowledgment (guarantees at-least-once delivery)
                future.get(5, TimeUnit.SECONDS);

                // Mark as processed in the DB
                event.setStatus(OutboxStatus.PROCESSED);
                outboxEventRepository.save(event);
                logger.info("Outbox event {} successfully published and marked as PROCESSED", event.getId());
            } catch (Exception e) {
                logger.error("Failed to publish outbox event {}. Will retry in the next scheduling cycle.", event.getId(), e);
            }
        }
    }
}
