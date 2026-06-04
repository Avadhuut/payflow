package com.payflow.transaction.repository;

import com.payflow.transaction.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, UUID> {
    List<Transaction> findBySenderAccountIdOrReceiverAccountId(UUID senderAccountId, UUID receiverAccountId);
}
