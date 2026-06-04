package com.payflow.transaction.client;

import com.payflow.transaction.dto.AccountDto;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.UUID;

@FeignClient(name = "account-service", fallback = AccountServiceClientFallback.class)
@CircuitBreaker(name = "accountService")
@Retry(name = "accountService")
public interface AccountServiceClient {

    @GetMapping("/api/accounts/{id}")
    AccountDto getAccount(@PathVariable("id") UUID id);
}

