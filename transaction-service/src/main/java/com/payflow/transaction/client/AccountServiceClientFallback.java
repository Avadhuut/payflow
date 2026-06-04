package com.payflow.transaction.client;

import com.payflow.transaction.dto.AccountDto;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class AccountServiceClientFallback implements AccountServiceClient {

    @Override
    public AccountDto getAccount(UUID id) {
        // Managed safe degradation response
        AccountDto dto = new AccountDto();
        dto.setId(id);
        dto.setUserId(null);
        dto.setBalance(BigDecimal.ZERO);
        return dto;
    }
}
