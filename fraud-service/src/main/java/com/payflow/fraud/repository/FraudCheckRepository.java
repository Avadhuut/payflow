package com.payflow.fraud.repository;

import com.payflow.fraud.entity.FraudCheck;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface FraudCheckRepository extends JpaRepository<FraudCheck, UUID> {
    Optional<FraudCheck> findByTransactionId(UUID transactionId);
    List<FraudCheck> findBySenderAccountId(UUID senderAccountId);
}
