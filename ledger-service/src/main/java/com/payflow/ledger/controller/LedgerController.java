package com.payflow.ledger.controller;

import com.payflow.ledger.entity.LedgerEntry;
import com.payflow.ledger.repository.LedgerEntryRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/ledger")
public class LedgerController {

    private final LedgerEntryRepository ledgerEntryRepository;

    public LedgerController(LedgerEntryRepository ledgerEntryRepository) {
        this.ledgerEntryRepository = ledgerEntryRepository;
    }

    @GetMapping("/transaction/{transactionId}")
    public ResponseEntity<?> getLedgerEntriesByTransaction(@PathVariable UUID transactionId) {
        List<LedgerEntry> entries = ledgerEntryRepository.findByTransactionId(transactionId);
        return ResponseEntity.ok(entries);
    }

    @GetMapping("/recent")
    public ResponseEntity<?> getRecentLedgerEntries() {
        List<LedgerEntry> entries = ledgerEntryRepository.findTop100ByOrderByCreatedAtDesc();
        return ResponseEntity.ok(entries);
    }
}
