# 📘 PayFlow: Technical Architecture & Deep Dive Reference Manual

---

## 1. Executive System Abstract

### Business and Operational Context
PayFlow is a production-grade, highly resilient microservices payment orchestration engine designed to support high-throughput, low-latency financial transactions. At its core, the system facilitates transfer requests from senders to receivers while maintaining strict regulatory compliance, fraud mitigation, and data consistency. 

In modern digital transaction systems, a payment is rarely a single database transaction. It is a multi-phased pipeline involving user authentication, account balance validation, risk checks, wallet debits, wallet credits, notification dispatches, and immutable record logging. In a distributed infrastructure, achieving operational stability across these disparate components is a complex engineering challenge.

```
[Client Request] ──> [API Gateway] ──> [Saga Broker (Kafka)] ──> [Distributed Services]
                                                                        │
                                       ┌────────────────────────────────┼────────────────────────────────┐
                                       ▼                                ▼                                ▼
                            [Account Service]                    [Fraud Service]                 [Ledger Service]
                         (Debit/Credit/Rollback)                (Risk Evaluation)            (Immutable Audit Trails)
```

### Core Problems Solved by PayFlow
1. **Distributed Data Consistency**: Ensures that if a payment is initiated, it is either fully completed across all database boundaries (sender debited, receiver credited, and ledger logged) or completely rolled back to its baseline state, preventing "lost money" scenarios.
2. **Double-Charging & Replay Attacks**: Mitigates duplicate transaction processing caused by unstable internet connections, client retries, or network timeouts by executing strict idempotency checks at the gateway and transaction boundaries.
3. **Real-Time Risk Classification**: Evaluates every payment against a dynamic multi-dimensional risk matrix (transaction sizes, user behaviors, velocity, and temporal patterns) to block fraudulent transactions before any funds settle.
4. **Immutable Compliance Auditing**: Satisfies strict financial auditing standards (e.g., PCI-DSS, SOC2) by keeping an append-only, tamper-proof record of every transaction state transition, mapping it to a unique trace identifier.

---

## 2. Monolith vs. Microservices: Why Database-per-Service?

### Structural Isolation vs. Single Monoliths
While a monolithic architecture is simpler to design, build, and deploy in the early phases of an application, it presents severe limitations when applied to core payment infrastructures:
* **Blast Radius**: A bug or memory leak in a secondary monolithic component (such as the notification engine) can crash the entire application process, rendering the core transaction processing system completely offline.
* **Scaling Mismatch**: The ledger service is highly write-intensive, whereas the auth service is read-heavy. In a monolith, scaling the ledger service requires scaling the entire application instance, leading to inefficient resource utilization.
* **Deployment Bottlenecks**: A single monolith forces all development teams to deploy within a shared release lifecycle, introducing merge conflicts, regression risks, and slower feature delivery.

### The Database-per-Service Pattern
To solve these problems, PayFlow implements the **Database-per-Service** pattern. Each microservice manages and encapsulates its own persistent state. No service can directly query or modify another service's database. All communication occurs strictly via REST APIs (orchestrated via Eureka and Feign clients) or asynchronously through event streams (orchestrated via Kafka).

```
                      ┌──────────────┐
                      │ API Gateway  │
                      └──────┬───────┘
                             │
       ┌─────────────────────┼─────────────────────┐
       ▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  Auth Service│      │ Account Svc  │      │ Transaction  │
└──────┬───────┘      └──────┬───────┘      └──────┬───────┘
       ▼                     ▼                     ▼
 ┌───────────┐         ┌───────────┐         ┌───────────┐
 │MySQL (Auth)│         │MySQL (Acc)│         │MySQL (Tx) │
 └───────────┘         └───────────┘         └───────────┘
```

The physical database schemas are isolated as follows:
* `payflow_auth`: Stores identity data, credentials, and roles.
* `payflow_account`: Manages account balances, owners, and versions.
* `payflow_transaction`: Manages transaction states and idempotency mappings.
* `payflow_fraud`: Tracks risk assessments, scores, and blacklist rules.
* `payflow_notification`: Persists historical notification payloads mapped to users.
* `payflow_ledger`: Serves as the write-only event audit store.

### Architectural Trade-offs
Implementing Database-per-Service introduces distinct challenges:
1. **Lack of ACID Joint SQL Queries**: Because schemas are split across different databases, running a single SQL statement containing `JOIN` clauses across `Account` and `User` tables is physically impossible. Data aggregation must be executed at the application layer using Feign HTTP calls or by stitching materialized views asynchronously.
2. **Eventual Consistency Complexities**: If Service A updates its state, Service B may not reflect this change instantly due to network or processing lag. The system must accept a window of eventual consistency and be designed to handle intermediate states gracefully.
3. **Increased Latency**: Restricting database sharing forces microservices to communicate over the network, introducing TCP handshake overhead and network serialization costs compared to simple local memory access in a monolith.

---

## 3. Distributed Data Consistency: Saga Choreography vs. Orchestration

### The Downside of Two-Phase Commit (2PC)
In classical databases, distributed transactions are managed via Two-Phase Commit (2PC). However, 2PC is poorly suited for high-volume cloud-native microservices:
* **Blocking Locks**: During the voting and commit phases, all involved database resources are locked. If a single participant lags or experiences a network partition, all other databases remain locked, causing database connection pool exhaustion.
* **Single Point of Failure**: If the central coordinator node crashes during the commit phase, participants remain in an uncertain state indefinitely.
* **Scalability Bottleneck**: 2PC is synchronous and chatty, reducing the throughput of the transaction engine to the speed of the slowest participant.

### Saga Orchestration vs. Saga Choreography
Instead of 2PC, PayFlow uses the **Saga Pattern**, which breaks a distributed transaction into a sequence of local transactions. Each local transaction updates the database inside a single service and publishes an event. Subsequent services consume the event and execute their local actions.

While **Saga Orchestration** relies on a central controller class to tell each service what to do, **Saga Choreography** uses an event-driven model where services react to events autonomously. PayFlow adopts Choreography via Kafka for the following reasons:
* **Decoupling**: Services do not need to know about the existence or interfaces of other services. They only produce and consume events on designated Kafka topics.
* **Resilience**: There is no single central orchestrator process whose failure halts the entire lifecycle. If a service goes offline, events queue up in Kafka and are processed when the service recovers.
* **Sub-second Reactivity**: Kafka allows message consumption at ultra-low latency, allowing steps to run in parallel without waiting for synchronous network call returns.

```
TxSvc ──[initiated]──> Kafka ──> AccSvc (Debit) ──[debited]──> Kafka ──> FraudSvc (Score)
                                                                            │
                                                                            ▼
TxSvc <──[completed/rollback]── Kafka <──[cleared/flagged]── Kafka <────────┘
```

### Happy Path Execution Trace
When a valid request to transfer ₹5,000 from Rahul to Priya occurs:
1. `transaction-service` persists a transaction in state `INITIATED` and publishes a `PaymentInitiatedEvent` to the `payment.initiated` topic.
2. `account-service` consumes this event, locks Rahul's account, deducts ₹5,000, updates the version, and publishes an `AccountDebitedEvent` to the `account.debited` topic.
3. `fraud-service` consumes the `AccountDebitedEvent`, runs risk calculations, and publishes a `FraudClearedEvent` to the `fraud.cleared` topic.
4. `transaction-service` consumes both events, updates the transaction status to `COMPLETED`, and publishes a `PaymentCompletedEvent` to the `payment.completed` topic.
5. `account-service` consumes the `PaymentCompletedEvent` and credits Priya's account with ₹5,000.
6. `notification-service` and `ledger-service` consume the completion event to send alerts and log the final state.

### Failure Path Execution Trace (Compensating Transaction)
If a transaction fails the risk check (e.g., fraud score is calculated as 75):
1. Steps 1 and 2 proceed normally: Rahul is debited ₹5,000.
2. `fraud-service` processes the transaction and calculates a high fraud score. It publishes a `FraudFlaggedEvent` to the `fraud.flagged` topic.
3. `transaction-service` consumes the `FraudFlaggedEvent`, marks the transaction state as `FAILED`, and publishes a `PaymentRollbackEvent` to the `payment.rollback` topic.
4. `account-service` consumes the `PaymentRollbackEvent` and executes a **compensating transaction**: it deposits ₹5,000 back into Rahul's account.
5. `notification-service` alerts Rahul that the payment was blocked and refunded.

---

## 4. Transaction Idempotency: The Redis Lease Lock Pattern

### Limitations of Database Constraints
At high scales, physical database unique keys (e.g., a SQL `UNIQUE` constraint on a column) are not enough to prevent double-charging:
* Database inserts are relatively slow and disk-bound. High-frequency duplicate requests hitting the server within milliseconds can bypass simple check-then-insert blocks due to read isolation levels.
* Database-level constraints do not handle request leasing. If a user double-taps a payment button, both requests hit the application layer simultaneously. We want to reject the second request immediately rather than letting it sit in a thread pool queue waiting for a database transaction slot.

### Step-by-Step Lifecycle of the Idempotency Key
PayFlow mitigates this using a Redis-backed Distributed Lease Lock pattern:

```
[Client Request] 
       │
       ▼
[Check Redis Key]
       ├──> (Exists) ──────> [Return Cached Response]
       │
       └──> (Not Exists) ──> [Acquire Lease Lock (10s)] ──> [Process Saga]
                                                                  │
                             [Cache Final Response (24h)] <───────┘
```

1. **Header Extraction**: The Gateway or `transaction-service` extracts the `Idempotency-Key` header from the incoming request.
2. **Lease Checking**: It attempts to write a key `lock:idempotency:<key>` in Redis with a short time-to-live (TTL) of 10 seconds using the atomic `SETNX` (Set if Not Exists) command.
   * If `SETNX` returns `0` (false), it indicates that a previous request with the same key is currently running. The system immediately rejects the request with an HTTP `409 Conflict`.
   * If `SETNX` returns `1` (true), the lease is acquired, and the transaction is allowed to proceed.
3. **Execution & Caching**: The transaction executes through the Saga pipeline. Once the final response payload is determined (either success or failure), the service:
   * Saves the response JSON string in Redis under the key `response:idempotency:<key>` with a TTL of 24 hours.
   * Deletes the lease lock key `lock:idempotency:<key>`.
4. **Subsequent Hits**: If another request with the same `Idempotency-Key` arrives within 24 hours, the service detects `response:idempotency:<key>`, bypasses the application logic entirely, and directly returns the cached JSON response.

### Why Redis is Chosen Over Relational Databases
* **Throughput and Latency**: Redis stores data in memory, resolving lookups in sub-millisecond times ($O(1)$ complexity) without disk I/O.
* **Atomic Primitives**: Commands like `SETNX` and key expiration are evaluated in a single thread inside Redis, preventing race conditions during lock acquisition.
* **Auto-Expiring State**: Redis handles TTLs natively. Attempting to manage expiring tokens in MySQL requires running a cron-like cleanup job, which causes table locks and resource contention.

---

## 5. Asynchronous Risk Mitigation: Fraud Evaluation Mechanical Engine

### The Rule-Based Scoring Matrix
The `fraud-service` implements an isolated rule evaluation engine to compute a risk score for each payment. If a transaction gathers a score $\ge 60$, it is blocked.

| Rule ID | Rule Trigger Condition | Assigned Risk Points | Data Fetching Mechanism |
| :--- | :--- | :--- | :--- |
| `RL_01` | Transaction Amount > ₹10,000 | `+40` Points | Inspects incoming event payload |
| `RL_02` | High Velocity (3+ tx / 60 seconds) | `+30` Points | Queries rolling window counter in Redis |
| `RL_03` | Off-hours transaction (12 AM - 5 AM) | `+20` Points | Inspects current system timestamp |
| `RL_04` | New Account creation age < 30 days | `+10` Points | Synchronous Feign client call to `auth-service` |

### Velocity Checking via Sliding-Window Redis Counters
To determine if a user has initiated 3 or more payments within 60 seconds, query patterns like `SELECT COUNT(*) FROM transactions WHERE account_id = X AND created_at > NOW() - INTERVAL 1 MINUTE` are highly inefficient:
* Under heavy load, these aggregate query scans lock database rows and consume substantial CPU cycles.
* Disk-bound transactional tables become bottlenecked by simple read queries.

PayFlow solves this using Redis **Sorted Sets (ZSET)** to maintain a memory-efficient sliding-window counter:
1. Each time a payment is initiated, the system executes an atomic Redis pipeline for key `velocity:account:<id>`:
   * Adds the current transaction ID with a score equal to the current epoch timestamp (`ZADD`).
   * Removes all entries with a timestamp older than `(currentTime - 60 seconds)` (`ZREMRANGEBYSCORE`).
   * Counts the remaining entries in the set (`ZCARD`).
   * Sets an idle timeout TTL of 60 seconds on the entire set key (`EXPIRE`).
2. If `ZCARD` returns $\ge 3$, the velocity rule is triggered, and `+30` points are added to the fraud score.

This approach resolves velocity evaluation in memory without hitting the relational database.

---

## 6. Enterprise Hardening & Fault Isolation: Resilience4j

### The Cascading Failure Problem
In a microservices mesh, downstream failures can propagate upstream. If the `transaction-service` calls the `account-service` synchronously and `account-service` slows down due to database locking, the request threads in `transaction-service` block. As more requests arrive, the thread pool of `transaction-service` quickly saturates, causing it to freeze. This cascading failure can eventually consume all available threads in the API Gateway, taking the entire platform down.

```
[Client] ──> [API Gateway] ──> [Transaction Svc (Threads Blocked)] ──> [Slow Account Svc]
```

### Resilience4j Circuit Breakers
PayFlow places a Resilience4j Circuit Breaker at the API Gateway level to isolate the system from slow dependencies. The circuit breaker operates as a state machine:

```
      ┌─────────────────────────┐
      │         CLOSED          │ <──────────────────┐
      │  (Normal Operation)     │                    │
      └──────────┬──────────────┘                    │
                 │                                   │
        (Failure Rate > 50%)                         │
                 │                                   │
                 ▼                                   │
      ┌─────────────────────────┐                    │
      │          OPEN           │                    │
      │  (Short-circuit Calls)  │                    │
      └──────────┬──────────────┘                    │
                 │                                   │
         (Wait 10 Seconds)                           │
                 │                                   │
                 ▼                                   │
      ┌─────────────────────────┐                    │
      │        HALF-OPEN        │                    │
      │  (Test with Few Calls)  ├────────────────────┘
      └─────────────────────────┘
```

1. **CLOSED**: All requests are routed to downstream services normally. The gateway monitors failure rates and response times.
2. **OPEN**: If more than 50% of the last 10 requests fail or take longer than configured thresholds, the circuit breaker trips to `OPEN`. All incoming calls to `/api/v1/payments/**` are immediately blocked at the gateway level. The gateway bypasses the target service and returns a fallback JSON payload:
   ```json
   {
     "status": "SERVICE_UNAVAILABLE",
     "message": "Payment service temporarily unavailable. Please try again in a moment."
  }
  ```
   This prevents thread exhaustion by failing fast.
3. **HALF-OPEN**: The circuit breaker remains `OPEN` for 10 seconds. Once this wait window expires, it transitions to `HALF-OPEN`, allowing a limited number of requests through to verify if the downstream service has recovered. If those requests succeed, it returns to `CLOSED`; if they fail, it trips back to `OPEN` for another 10 seconds.

### Dead-Letter Queues (DLQ)
When handling asynchronous messages via Kafka, consumer logic might fail due to "poison pills" (malformed messages, database transient errors, or logic bugs).
* Letting the consumer retry indefinitely blocks the partition, stopping all other payments.
* Dropping the message causes silent data loss.

PayFlow resolves this by configuring a **Dead-Letter Queue (DLQ)**. If a message fails processing after 3 retry attempts, the consumer catches the exception, publishes the failed payload along with its stack trace headers to a `payment.DLQ` topic, and commits the offset. This keeps the primary queue moving while preserving the failed message for administrative inspection and manual replay.

---

## 7. Data Security: System Auditing & Concurrency

### Append-Only Immutable Auditing
In financial accounting, records must never be modified. Modifying a ledger entry violates basic regulatory standards (e.g., SOX compliance).
* The `ledger-service` does not expose any SQL `UPDATE` or `DELETE` mappings.
* The database user granted to the ledger service has only `SELECT` and `INSERT` privileges.
* If a mistake is made (e.g., incorrect funds credited), it is resolved by inserting a new compensating record (a credit or debit correction entry) rather than editing the original row. This preserves a complete historical timeline.

### MDC Correlation ID Logging
Because transactions flow asynchronously across HTTP and Kafka boundaries, debugging a single request across multiple log files is difficult. PayFlow implements a custom logging filter using Spring's **MDC (Mapped Diagnostic Context)**:
1. When a request enters the API Gateway, a unique `Correlation-ID` UUID is generated (if not already present in the headers) and placed in the MDC.
2. The `Correlation-ID` is appended to all HTTP request/response headers and injected as a header in every Kafka record produced.
3. Downstream services read the header and place the UUID into their local MDC.
4. The logging framework (`logback.xml`) is configured to print the correlation ID in every log statement:
   `[2026-06-04 17:30:00] [INFO] [tx-svc] [corr-id: 9a2f1c8b-...] Initiating payment...`
   
This allows an engineer to search a centralized log manager (e.g., Elasticsearch, Grafana Loki) for a specific `Correlation-ID` and view the entire system trace across all 8 microservices.

### Concurrency Controls: Optimistic vs. Pessimistic Locking
When multiple threads attempt to update the same account balance simultaneously, a race condition occurs. If two threads read a balance of ₹10,000, and both deduct ₹3,000, they might both write the balance as ₹7,000 back to the database, resulting in a ₹3,000 loss (lost update problem).

#### Pessimistic Locking
* Uses database-level locks (e.g., `SELECT ... FOR UPDATE` in SQL).
* It blocks all concurrent reads and writes on the target row until the transaction commits.
* **Trade-off**: This reduces database throughput and increases the risk of deadlocks when concurrent calls interact.

#### Optimistic Locking
* Assumes collisions are rare and does not lock the database row.
* Leverages an incremental `@Version` integer column on the `Account` table.
* When updating a balance, Hibernate runs:
  `UPDATE account SET balance = :newBalance, version = version + 1 WHERE id = :id AND version = :oldVersion`
* If another thread updated the row first, the `version` column will have changed. The update query returns `0` rows modified. Hibernate detects this and throws an `ObjectOptimisticLockingFailureException`.
* The application catches this exception and handles it safely by retrying the transaction or returning a clean concurrency error to the user, preventing balance corruption.

---
*End of Reference Manual.*
