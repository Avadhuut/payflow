package com.payflow.fraud.service;

import com.payflow.fraud.client.AccountServiceClient;
import com.payflow.fraud.dto.AccountDto;
import com.payflow.fraud.entity.FraudCheck;
import com.payflow.fraud.repository.FraudCheckRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class FraudScoringService {

    private static final Logger logger = LoggerFactory.getLogger(FraudScoringService.class);

    private final FraudCheckRepository checkRepository;
    private final AccountServiceClient accountServiceClient;
    private final StringRedisTemplate redisTemplate;

    public FraudScoringService(
            FraudCheckRepository checkRepository,
            AccountServiceClient accountServiceClient,
            StringRedisTemplate redisTemplate) {
        this.checkRepository = checkRepository;
        this.accountServiceClient = accountServiceClient;
        this.redisTemplate = redisTemplate;
    }

    @Transactional
    public FraudCheck executeScoring(UUID correlationId, UUID transactionId, UUID senderAccountId, BigDecimal amount) {
        logger.info("[CorrelationID: {}] Starting fraud scoring evaluation for transaction {} (senderAccount: {})",
                correlationId, transactionId, senderAccountId);

        int score = 0;

        // Rule 1: Amount > 10,000 (+40)
        if (amount != null && amount.compareTo(new BigDecimal("10000")) > 0) {
            score += 40;
            logger.info("[CorrelationID: {}] Rule matched: Amount > 10,000 (+40)", correlationId);
        }

        // Rule 2: 3 or more transactions within 60s (+30)
        String redisKey = "fraud:counter:" + senderAccountId;
        Long txCount = redisTemplate.opsForValue().increment(redisKey);
        if (txCount != null && txCount == 1) {
            redisTemplate.expire(redisKey, 60, TimeUnit.SECONDS);
        }
        if (txCount != null && txCount >= 3) {
            score += 30;
            logger.info("[CorrelationID: {}] Rule matched: High frequency transaction counter = {} (+30)",
                    correlationId, txCount);
        }

        // Rule 3: Unusual hours between 12 AM and 5 AM (+20)
        int hour = LocalDateTime.now().getHour();
        if (hour >= 0 && hour < 5) {
            score += 20;
            logger.info("[CorrelationID: {}] Rule matched: Unusual hours = {} AM (+20)", correlationId, hour);
        }

        // Rule 4: Account age < 30 days (+10)
        try {
            AccountDto accountDto = accountServiceClient.getAccount(senderAccountId);
            if (accountDto != null && accountDto.getCreatedAt() != null) {
                long daysOld = ChronoUnit.DAYS.between(accountDto.getCreatedAt(), LocalDateTime.now());
                if (daysOld < 30) {
                    score += 10;
                    logger.info("[CorrelationID: {}] Rule matched: Account age = {} days old (<30d) (+10)",
                            correlationId, daysOld);
                }
            }
        } catch (Exception e) {
            logger.warn("[CorrelationID: {}] Feign check failed to retrieve account details for {}",
                    correlationId, senderAccountId, e);
        }

        // Evaluate decision
        String decision = (score >= 60) ? "FLAGGED" : "CLEARED";
        logger.info("[CorrelationID: {}] Fraud evaluation finished. Score: {}, Decision: {}",
                correlationId, score, decision);

        // Save check entry to database
        FraudCheck check = FraudCheck.builder()
                .transactionId(transactionId)
                .senderAccountId(senderAccountId)
                .score(score)
                .decision(decision)
                .correlationId(correlationId)
                .createdAt(LocalDateTime.now())
                .build();
        return checkRepository.save(check);
    }

    @Transactional(readOnly = true)
    public FraudCheck getCheckByTransaction(UUID transactionId) {
        return checkRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new IllegalArgumentException("Fraud check not found for transaction: " + transactionId));
    }

    @Transactional(readOnly = true)
    public List<FraudCheck> getChecksByAccount(UUID accountId) {
        return checkRepository.findBySenderAccountId(accountId);
    }
}
