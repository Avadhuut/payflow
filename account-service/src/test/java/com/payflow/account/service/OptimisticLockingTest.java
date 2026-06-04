package com.payflow.account.service;

import com.payflow.account.entity.Account;
import com.payflow.account.repository.AccountRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
    "eureka.client.enabled=false",
    "spring.kafka.listener.auto-startup=false"
})
public class OptimisticLockingTest {

    @Autowired
    private AccountService accountService;

    @Test
    public void testOptimisticLockingConcurrency() throws InterruptedException {
        // Create an account
        UUID userId = UUID.randomUUID();
        Account account = accountService.createAccount(userId, new BigDecimal("1000.00"));
        UUID accountId = account.getId();

        int threadCount = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    latch.await(); // wait for start signal
                    // Call withdraw which is @Transactional
                    accountService.withdraw(accountId, new BigDecimal("10.00"));
                } catch (ObjectOptimisticLockingFailureException e) {
                    exceptionThrown.set(true);
                } catch (Exception e) {
                    // Ignored or logged
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        latch.countDown(); // start all threads simultaneously
        doneLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertTrue(exceptionThrown.get(), "Expected ObjectOptimisticLockingFailureException to be thrown due to version conflict");
    }
}
