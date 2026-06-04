package com.payflow.fraud.controller;

import com.payflow.fraud.entity.FraudCheck;
import com.payflow.fraud.service.FraudScoringService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/fraud/checks")
public class FraudController {

    private final FraudScoringService scoringService;

    public FraudController(FraudScoringService scoringService) {
        this.scoringService = scoringService;
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<?> getCheckByTransaction(@PathVariable UUID transactionId) {
        FraudCheck check = scoringService.getCheckByTransaction(transactionId);
        return ResponseEntity.ok(check);
    }

    @GetMapping("/account/{accountId}")
    public ResponseEntity<?> getChecksByAccount(@PathVariable UUID accountId) {
        List<FraudCheck> checks = scoringService.getChecksByAccount(accountId);
        return ResponseEntity.ok(checks);
    }
}
