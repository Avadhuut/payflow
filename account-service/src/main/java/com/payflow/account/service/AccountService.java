package com.payflow.account.service;

import com.payflow.account.entity.Account;
import com.payflow.account.repository.AccountRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class AccountService {

    private final AccountRepository accountRepository;

    public AccountService(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    @Transactional
    public Account createAccount(UUID userId, BigDecimal initialBalance) {
        if (initialBalance == null || initialBalance.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Initial balance cannot be negative");
        }
        Account account = Account.builder()
                .userId(userId)
                .balance(initialBalance)
                .build();
        return accountRepository.save(account);
    }

    @Transactional(readOnly = true)
    public Account getAccount(UUID accountId) {
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account not found with ID: " + accountId));
    }

    @Transactional
    public Account deposit(UUID accountId, BigDecimal amount) {
        Account account = getAccount(accountId);
        account.credit(amount);
        return accountRepository.save(account);
    }

    @Transactional
    public Account withdraw(UUID accountId, BigDecimal amount) {
        Account account = getAccount(accountId);
        account.debit(amount);
        return accountRepository.save(account);
    }
}
