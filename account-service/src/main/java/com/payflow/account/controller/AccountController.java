package com.payflow.account.controller;

import com.payflow.account.dto.CreateAccountRequest;
import com.payflow.account.dto.TransactionRequest;
import com.payflow.account.entity.Account;
import com.payflow.account.service.AccountService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/accounts")
public class AccountController {

    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    @PostMapping
    public ResponseEntity<?> createAccount(@RequestBody CreateAccountRequest request) {
        Account account = accountService.createAccount(request.getUserId(), request.getInitialBalance());
        return ResponseEntity.ok(account);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getAccount(@PathVariable UUID id) {
        Account account = accountService.getAccount(id);
        return ResponseEntity.ok(account);
    }

    @GetMapping("/{id}/balance")
    public ResponseEntity<?> getBalance(@PathVariable UUID id) {
        Account account = accountService.getAccount(id);
        return ResponseEntity.ok(Map.of(
                "accountId", account.getId().toString(),
                "balance", account.getBalance()
        ));
    }

    @PostMapping("/{id}/deposit")
    public ResponseEntity<?> deposit(@PathVariable UUID id, @RequestBody TransactionRequest request) {
        Account account = accountService.deposit(id, request.getAmount());
        return ResponseEntity.ok(account);
    }

    @PostMapping("/{id}/withdraw")
    public ResponseEntity<?> withdraw(@PathVariable UUID id, @RequestBody TransactionRequest request) {
        Account account = accountService.withdraw(id, request.getAmount());
        return ResponseEntity.ok(account);
    }
}
