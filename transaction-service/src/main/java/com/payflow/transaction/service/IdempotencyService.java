package com.payflow.transaction.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class IdempotencyService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public IdempotencyService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    private String getResponseKey(String key) {
        return "idem:" + key;
    }

    private String getLockKey(String key) {
        return "idem:lock:" + key;
    }

    public <T> T getCachedResponse(String key, Class<T> responseType) {
        String cachedValue = redisTemplate.opsForValue().get(getResponseKey(key));
        if (cachedValue == null) {
            return null;
        }
        try {
            return objectMapper.readValue(cachedValue, responseType);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean acquireLease(String key) {
        // Set execution lease with 30-second TTL to avoid permanent locks on failures
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(
                getLockKey(key),
                "LOCKED",
                30,
                TimeUnit.SECONDS
        );
        return acquired != null && acquired;
    }

    public void cacheResponse(String key, Object response) {
        try {
            String value = objectMapper.writeValueAsString(response);
            redisTemplate.opsForValue().set(
                    getResponseKey(key),
                    value,
                    24,
                    TimeUnit.HOURS
            );
        } catch (Exception e) {
            // Fail silently or log
        } finally {
            releaseLease(key);
        }
    }

    public void releaseLease(String key) {
        redisTemplate.delete(getLockKey(key));
    }
}
