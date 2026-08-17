# Hyperswitch Revenue Recovery — Codebase Guide

> A developer-facing explainer of the **Revenue Recovery** (a.k.a. *Passive Churn Recovery*, internal codename **PCR**) module: what it does, where it lives, and how the retry engine works end to end.

---

## 1. What Revenue Recovery Is

Revenue Recovery combats **passive churn** for subscription/recurring businesses. When a customer's invoice payment fails at a billing platform (Chargebee, Stripe Billing, Recurly), Hyperswitch picks up the failure via webhook and **automatically retries the payment** itself — as a Merchant-Initiated Transaction (MIT) — using stored processor tokens, on an intelligent schedule.

Key ideas:

- **Billing connector** (Chargebee / Stripe Billing / Recurly) owns the *invoice/subscription* and reports failures.
- **Payment processor** (Stripe, Adyen, etc.) is what Hyperswitch actually charges through during a retry.
- Hyperswitch keeps a per-customer **token vault in Redis** (multiple cards per customer) and cycles a **3-task state machine** (`CALCULATE → EXECUTE → PSYNC`) until the invoice is paid or every token is exhausted.
- Retry timing is pluggable: fixed **Cascading** schedule, ML-driven **Smart** retries via an external gRPC "decider" service, or passive **Monitoring** mode.
- On success, the result is **recorded back to the billing connector** (`InvoiceRecordBack`) so the invoice closes in the merchant's billing system.

Gated behind the Cargo feature flag `revenue_recovery` (requires `v2`).

---

## 2. Architecture at a Glance

```
                        ┌──────────────────────────┐
                        │  Billing Connector        │
                        │  (Chargebee / Stripe      │
                        │   Billing / Recurly)      │
                        └──────────┬───────────────┘
                                   │ webhook: payment failed
                                   ▼
            POST /v2/webhooks/recovery/{mid}/{pid}/{connector_id}
                 crates/router/src/core/webhooks/recovery_incoming.rs
                                   │
                create/update intent + attempt, store tokens in Redis,
                publish Kafka, decide RecoveryAction
                                   │  (retry_count > MCA threshold)
                                   ▼
                    insert CALCULATE_WORKFLOW process-tracker task
                                   │  (scheduler picks up when due)
                                   ▼
   ┌─────────────────── PCR workflow loop ───────────────────────┐
   │                                                             │
   │  CALCULATE_WORKFLOW ──token+time found──► EXECUTE_WORKFLOW  │
   │        ▲  ▲                                   │             │
   │        │  │                         ┌─────────┼──────────┐  │
   │        │  │                       success   failed    processing
   │        │  │                         │          │          │  │
   │        │  └─────── reopen ──────────┘          ▼          ▼  │
   │        │            (fail)              PSYNC_WORKFLOW       │
   │        └──────────────────────────────────┘  │  loop        │
   │                                              ▼              │
   │                                    succeeded/failed/processing
   │                                                             │
   └─────────────────────────────────────────────────────────────┘
                                   │
                    success ───────┼─────── terminal failure (hard decline)
                    ▼              │              ▼
        InvoiceRecordBack to      │      PaymentFailed outgoing webhook,
        billing connector,        │      customer lock released
        outgoing webhook,         │
        Kafka event               ▼
```

---

## 3. Entry Points

| # | Trigger | Route / Mechanism | Handler |
|---|---------|-------------------|---------|
| 1 | **Billing connector webhook** (primary) | `POST /v2/webhooks/recovery/{merchant_id}/{profile_id}/{connector_id}` | `recovery_receive_incoming_webhook` → `core/webhooks/recovery_incoming.rs::recovery_incoming_webhook_flow` |
| 2 | **Explicit recovery payment API** | `POST /v2/payments/recovery` | `payments::recovery_payments_create` (`routes/payments.rs:148`) |
| 3 | **Scheduler** (background loop) | process-tracker tasks, runner `PassiveRecoveryWorkflow` | `workflows/revenue_recovery.rs::ExecutePcrWorkflow` (registered in `bin/scheduler.rs` ~L375) |
| 4 | **Retrieve / resume (ops)** | `GET /v2/process_tracker/revenue_recovery_workflow/{id}`, `POST …/resume` (also hyphenated `/v2/process-trackers/revenue-recovery-workflow`) | `routes/process_tracker/revenue_recovery.rs` |
| 5 | **Backfill / Redis inspection (ops)** | `POST /v2/recovery/data-backfill`, `GET …/redis-data/{id}`, `PUT …/update-token`, `POST …/status/...` | `routes/revenue_recovery_data_backfill.rs`, `routes/revenue_recovery_redis.rs` |
| 6 | **Merchant-facing status** | `GET /v2/payments/{id}/get-revenue-recovery-intent`, `GET /v2/payments/recovery-list` | `payments::revenue_recovery_get_intent`, `payments::revenue_recovery_invoices_list` |

### 3.1 The incoming webhook pipeline (`recovery_incoming.rs`)

When a billing connector reports an event (`RecoveryPaymentFailure` / `Success` / `Pending` / `InvoiceCancelled`):

1. **Verify source authenticity** (mandatory — no payment objects exist yet).
2. **Optional enrichment**: if configured (`billing_connectors_payment_sync` / `billing_connectors_invoice_sync` settings), call the billing connector's Payments Sync and/or Invoice Sync APIs for authoritative data.
3. **Fetch-or-create** the `PaymentIntent` by `merchant_reference_id`; fetch-or-record the `PaymentAttempt` (marking `revenue_recovery.attempt_triggered_by = External`).
4. **Upsert processor tokens into Redis** (`RedisTokenManager::upsert_payment_processor_token`) keyed by `connector_customer_id`, with card details + error code.
5. **Publish Kafka event** (`services/kafka/revenue_recovery.rs`).
6. Compute a **`RecoveryAction`**. For a failed payment, if `intent_retry_count > billing_MCA.retry_threshold`, it inserts the first `CALCULATE_WORKFLOW` process-tracker task (`upsert_calculate_pcr_task`). Below the threshold the billing connector is left to attempt its own retries first.

---

## 4. The PCR Workflow State Machine

Three process-tracker tasks under runner `PassiveRecoveryWorkflow`, dispatched by `ExecutePcrWorkflow::execute_workflow` in `crates/router/src/workflows/revenue_recovery.rs`. Task IDs look like `PassiveRecoveryWorkflow_CALCULATE_WORKFLOW_{payment_id}`.

### 4.1 `CALCULATE_WORKFLOW` — when & with what to retry
`core/revenue_recovery.rs::perform_calculate_workflow` (~L552)

1. Reads `connector_customer_id` from the intent's feature metadata and the retry algorithm from the business profile (`Monitoring` / `Smart` / `Cascading`).
2. If an active attempt exists, first does a `psync` to learn the latest payment state.
3. Calls `get_token_with_schedule_time_based_on_retry_algorithm_type` (workflows file) → one of:
   - **`ScheduledTime`** → reset `payment_connector_transmission` + clear active attempt on the intent, insert `EXECUTE_WORKFLOW` at that time, finish CALCULATE as `CALCULATE_WORKFLOW_SCHEDULED`.
   - **`NextAvailableTime`** → reschedule CALCULATE at `next_available_time + buffer`.
   - **`None`** → reschedule CALCULATE at `now + buffer`.
   - **`HardDecline`** → finish CALCULATE (`FAILED_DUE_TO_HARD_DECLINE_ERROR`) and send a `PaymentFailed` outgoing webhook.

### 4.2 `EXECUTE_WORKFLOW` — charge the card
`core/revenue_recovery.rs::perform_execute_payment` (~L244)

1. `Decision::get_decision_based_on_params(intent_status, connector_transmission, active_attempt_id)` (`core/revenue_recovery/types.rs:380`):
   - `(Failed, Unsuccessful, None)` → **Execute** (fresh retry)
   - `(PartiallyCaptured, Succeeded, Some)` → **Execute** (top-up retry)
   - `(Processing, Succeeded, Some)` → **Psync** first
   - `(Failed, Unsuccessful, Some)` → **ReviewForFailedPayment** (internal → requeue; external → complete-for-review)
   - `(Succeeded, _, _)` → **ReviewForSuccessfulPayment**
2. On **Execute**: lock the customer in Redis (`SETNX customer:{id}:status`), fetch the scheduled token (`RedisTokenManager::get_token_based_on_retry_type`):
   - **No token** → fail the task, `reopen_calculate_workflow_on_payment_failure`, unlock the customer if the intent is Failed.
   - **Token found** → `record_internal_attempt_and_execute_payment`:
     a. `record_internal_attempt_api` writes a new internal `PaymentAttempt`,
     b. `call_proxy_api` runs a MIT payment with `ProxyPaymentsRequest { processor_token }` against the **payment processor**,
     c. `execute_payment_task_response_handler` maps the outcome (`Action`):

     | `Action` | Effects |
     |---|---|
     | `SuccessfulPayment` | Kafka event → unlock token → outgoing webhook → **record back to billing connector** → `COMPLETED_EXECUTE_TASK` |
     | `Failed` | Kafka → `check_hard_decline` → update token error code + retry counts → unlock → reopen CALCULATE → `FAILED_EXECUTE_TASK` |
     | `Processing` | insert `PSYNC_WORKFLOW` → `COMPLETED_EXECUTE_TASK_TO_TRIGGER_PSYNC` |
     | `PartialCharged` | outgoing webhook → `COMPLETED_EXECUTE_TASK` |
     | `ReviewPayment` (call error) | `TRIGGER_REQUEUE_FOR_EXECUTE_WORKFLOW` |

### 4.3 `PSYNC_WORKFLOW` — confirm ambiguous payments
`core/revenue_recovery.rs::perform_payments_sync` (~L502)

- `call_psync_api` → map `IntentStatus` → `RevenueRecoveryPaymentIntentStatus`, then `update_pt_status_based_on_attempt_status_for_payments_sync`:
  - **Succeeded** → finish; clear token error code; unlock; webhook; record back.
  - **PartialCharged** → finish; publish event; reopen CALCULATE.
  - **Failed** → finish; hard-decline check; update token error + hourly retry count; unlock; reopen CALCULATE.
  - **Processing** → requeue via `payment_sync::recovery_retry_sync_task`.

### 4.4 Business statuses (state labels in `process_tracker.business_status`)

Defined in `crates/diesel_models/src/process_tracker.rs` (`business_status` module, ~L221):

- **CALCULATE**: `Pending` → `CALCULATE_WORKFLOW_SCHEDULED` | `FAILED_DUE_TO_HARD_DECLINE_ERROR` (finish); reopened back to `Pending` after failures.
- **EXECUTE**: `Pending` → `COMPLETED_EXECUTE_TASK` | `FAILED_EXECUTE_TASK` | `COMPLETED_EXECUTE_TASK_TO_TRIGGER_PSYNC` | `COMPLETED_EXECUTE_TASK_TO_TRIGGER_REVIEW` | `TRIGGER_REQUEUE_FOR_EXECUTE_WORKFLOW`.
- **PSYNC**: `Pending` → `COMPLETED_PSYNC_TASK` | `TRIGGER_REQUEUE_FOR_PSYNC_WORKFLOW`.

### 4.5 Status types to know

| Type | Variants | Where |
|---|---|---|
| `RevenueRecoveryAlgorithmType` | `Monitoring`, `Smart`, `Cascading` | `crates/common_enums/src/enums.rs:357` |
| `RecoveryStatus` (merchant-facing aggregate) | `Monitoring`, `Queued`, `Scheduled`, `Processing`, `Pending`, `Recovered`, `PartiallyRecovered`, `PartiallyCapturedAndProcessing`, `Terminated`, `NoPicked` | `common_enums/src/enums.rs` (~L2181) |
| `RevenueRecoveryPaymentIntentStatus` (internal) | `Succeeded`, `Failed`, `Processing`, `PartialCharged`, `InvalidStatus` | `core/revenue_recovery/types.rs:65` |
| `Decision` | `Execute`, `Psync`, `ReviewForSuccessfulPayment`, `ReviewForFailedPayment`, `InvalidDecision` | `core/revenue_recovery/types.rs:380` |
| `Action` | `SyncPayment`, `RetryPayment`, `TerminalFailure`, `SuccessfulPayment`, `PartialCharged`, `ReviewPayment`, `ManualReviewAction` | `core/revenue_recovery/types.rs:484` |
| `TriggeredBy` | `Internal` (Hyperswitch retried) vs `External` (billing connector attempt) | `common_enums/src/enums.rs:10587` |

---

## 5. Retry Decision Logic

### 5.1 Three algorithms (`RevenueRecoveryAlgorithmType`)

| Algorithm | How times are chosen | Key code |
|---|---|---|
| **Monitoring** | No active retries; just tracks failures. If failures exceed `monitoring_threshold_in_seconds`-era budget, profile is upgraded to an active algorithm. | `handle_monitoring_threshold` in `recovery_incoming.rs` |
| **Cascading** | Fixed retry offsets pulled from the **Superposition** config service, keyed by merchant + connector; tokens checked in Redis for hard-decline / wait hours. | `get_schedule_time_to_retry_mit_payments` in `workflows/revenue_recovery.rs` |
| **Smart** | External **gRPC "Recovery Decider"** ML service scores each token and returns the optimal retry time; the token with the *earliest* retry time wins. | `get_schedule_time_for_smart_retry`, `call_decider_for_payment_processor_tokens_select_closest_time` |

### 5.2 The Smart decider contract (`proto/recovery_decider.proto`)

```proto
service Decider { rpc Decide (DeciderRequest) returns (DeciderResponse); }

message DeciderRequest {
  string first_error_message = 1;
  optional string billing_state = 2;  optional string card_funding = 3;
  optional string card_network = 4;   optional string card_issuer  = 5;
  google.protobuf.Timestamp invoice_start_time = 6;
  optional int64 retry_count = 7;     optional string merchant_id = 8;
  optional int64 invoice_amount = 9;  optional string invoice_currency = 10;
  optional google.protobuf.Timestamp invoice_due_date = 11;
  optional string billing_country = 12; optional string billing_city = 13;
  optional string attempt_currency = 14; optional string attempt_status = 15;
  optional int64 attempt_amount = 16;
  optional string pg_error_code = 17;
  optional string network_advice_code = 18;
  optional string network_error_code = 19;
  optional string first_pg_error_code = 20;      // ← first-ever failure, sticky
  optional string first_network_advice_code = 21;
  optional string first_network_error_code = 22;
  optional google.protobuf.Timestamp attempt_response_time = 23;
  optional string payment_method_type = 24; optional string payment_gateway = 25;
  optional int64 retry_count_left = 26;
  optional int64 total_retry_count_within_network = 27;
  optional google.protobuf.Timestamp first_error_msg_time = 28;
  optional google.protobuf.Timestamp wait_time = 29;
  optional string payment_id = 30;
  map<string, int32> hourly_retry_history = 31;   // penalties per hour bucket
  optional double previous_threshold = 32;
}
message DeciderResponse {
  bool retry_flag = 1;
  google.protobuf.Timestamp retry_time = 2;
  optional double decision_threshold = 3;
}
```

Client: `crates/external_services/src/grpc_client/revenue_recovery/recovery_decider_client.rs` (headers via `GrpcRecoveryHeaders`).

### 5.3 Guard rails around the decider

- **Retry limits per card network** — `RetryLimitsConfig` (`types/storage/revenue_recovery.rs`): `max_retries_per_day` and `max_retry_count_for_thirty_day` (default 20/20; the 30-day window is 720 hourly buckets). Retry history lives in Redis.
- **Hard decline** — `check_hard_decline` (`workflows/revenue_recovery.rs:1115`) classifies the error through GSM (Generic Status Mapping); `ErrorCategory::HardDecline` permanently marks the token `is_hard_decline = true` in Redis → that token is never retried again.
- **Missed-slot backstop** — `should_force_schedule_due_to_missed_slots` (~L430): if a token hasn't been tried within `720h / max_30_day_retries`, force a retry without calling the decider.
- **Jitter / buffers** — `RecoveryTimestamp` config: `job_schedule_buffer_time_in_seconds` (15), `reopen_workflow_buffer_time_in_seconds` (60), `max_random_schedule_delay_in_seconds` (300), `redis_ttl_buffer_in_seconds` (300), `unretried_invoice_schedule_time_offset_seconds` (300).
- **MCA gate** — recovery only begins once `intent_retry_count > billing_connector_account.get_retry_threshold()`.

---

## 6. Data Model & Storage

**No dedicated SQL tables** — state is spread across existing tables + Redis + `process_tracker`:

| Store | What lives there | Where |
|---|---|---|
| `business_profile` (SQL) | `revenue_recovery_retry_algorithm_type`, `revenue_recovery_retry_algorithm_data` (JSONB) | `diesel_models/src/schema_v2.rs:336-337` |
| `payment_intent.feature_metadata` (SQL) | `PaymentRevenueRecoveryMetadata` — `payment_connector_transmission`, `billing_connector_payment_details`, `active_attempt_payment_connector_id`, `first_pg_error_*` | `api_models/src/payments.rs:13506`, `diesel_models/src/types.rs:471` |
| `payment_attempt.revenue_recovery` (SQL, JSONB) | `{ attempt_triggered_by, charge_id }` | `diesel_models/src/payment_attempt.rs` |
| `merchant_connector_account.revenue_recovery` (SQL) | billing-processor MCA metadata (which billing connector, retry threshold) | `api_models/src/admin.rs:1239` |
| `process_tracker` (SQL) | The 3 workflow tasks + tracking payload `RevenueRecoveryWorkflowTrackingData` | `types/storage/revenue_recovery.rs:16` |
| **Redis** | Two keys per customer: `customer:{connector_customer_id}:status` (SETNX lock) and `customer:{connector_customer_id}:tokens` (hash of `PaymentProcessorTokenStatus` — card info, error code, hard-decline flag, hourly retry history, scheduled time) | `types/storage/revenue_recovery_redis_operation.rs` (`RedisTokenManager`) |
| **Kafka** | One `RevenueRecovery` event per attempt/webhook (amounts, error codes, card network/issuer, retry count, gateway) | `services/kafka/revenue_recovery.rs` |

### Write-backs

- **To the billing connector** — `record_back_to_billing_connector` (`core/revenue_recovery/types.rs:1447`) sends `InvoiceRecordBackRequest { merchant_reference_id, amount, currency, payment_method_type, attempt_status, connector_transaction_id }` on terminal success.
- **To the merchant** — `RevenueRecoveryOutgoingWebhook::send_outgoing_webhook_based_on_revenue_recovery_status` emits PaymentSucceeded / PaymentFailed / PaymentProcessing / PaymentCaptured.
- **"Stop tracking"** = `unlock_connector_customer_status` — deletes the Redis lock key (only by the owning `payment_id`), on success, on failure (so CALCULATE can re-lock), or when all tokens are hard-declined. Manual unlock: `unlock_connector_customer_status_handler` in `core/revenue_recovery_data_backfill.rs`.

---

## 7. Connector Integration Surface

Traits in `crates/hyperswitch_interfaces/src/api/revenue_recovery.rs` (v1 impls) and `revenue_recovery_v2.rs`:

| Flow trait | Purpose | When |
|---|---|---|
| `BillingConnectorPaymentsSyncIntegration` | Fetch transaction details from the billing platform | during webhook ingest (if configured) |
| `BillingConnectorInvoiceSyncIntegration` | Fetch invoice details from the billing platform | during webhook ingest (if configured) |
| `InvoiceRecordBackIntegration` | Report the recovered payment back to the billing platform | on terminal success in EXECUTE/PSYNC |

Implementations: `hyperswitch_connectors/src/connectors/chargebee.rs`, `stripebilling.rs`, `recurly.rs`.

Actual retry charges go through the **normal payment connectors** via `call_proxy_api` (MIT with a stored processor token) — not through the billing connector.

---

## 8. Configuration

- **Cargo feature**: `revenue_recovery` in `crates/router/Cargo.toml:140` (routes gated `#[cfg(all(feature = "revenue_recovery", feature = "v2"))]`).
- **TOML**: `[revenue_recovery]` section (`config/development.toml`, `config/config.example.toml`) → `Settings.revenue_recovery` (`configs/settings.rs:187`) → `RevenueRecoverySettings` (`types/storage/revenue_recovery.rs:87`): `monitoring_threshold_in_seconds`, `retry_algorithm_type`, `recovery_timestamp`, `card_config` (retry limits), `redis_ttl_in_seconds`.
- **gRPC decider**: `[grpc_client.recovery_decider_client]` (validated at `configs/settings.rs:1542`).
- **Profile-level**: `revenue_recovery_retry_algorithm_type` + `_data` on the business profile (per-profile algorithm override).

---

## 9. Reading Path (source files in order)

1. `crates/router/src/core/webhooks/recovery_incoming.rs` — entry point, ingest + `RecoveryAction`
2. `crates/router/src/workflows/revenue_recovery.rs` — scheduler workflow, decider gRPC call, hard-decline, cascading schedule
3. `crates/router/src/core/revenue_recovery.rs` — the three `perform_*_workflow` functions
4. `crates/router/src/core/revenue_recovery/types.rs` — `Decision` / `Action` engine, reopen + record-back + outgoing webhooks
5. `crates/router/src/core/revenue_recovery/api.rs` — internal calls: psync, proxy payment, record attempt, update intent
6. `crates/router/src/types/storage/revenue_recovery_redis_operation.rs` — token vault, locking, retry history
7. `crates/hyperswitch_domain_models/src/revenue_recovery.rs` — domain structs
8. `proto/recovery_decider.proto` — Smart-retry contract
9. `crates/diesel_models/src/process_tracker.rs` (`business_status`) — state labels
10. `crates/router/src/routes/app.rs` (grep `recovery`) — full route wiring

---

## 10. Key Design Takeaways

- **Cyclic, self-healing loop** — failures reopen CALCULATE with incremented retry count until success or exhaustion; random jitter avoids thundering herds.
- **Token-per-customer vault in Redis** — multiple cards per customer; the engine rotates across tokens; each token carries its own penalty box (error code, hard-decline flag, hourly retry history).
- **Distributed locking** — Redis `SETNX` on `customer:{id}:status`, releasable only by the owning payment_id, prevents concurrent retries for one customer.
- **Internal vs External attempts** (`TriggeredBy`) — billing-platform failures enter recovery; Hyperswitch's own failures are handled inside the workflow.
- **Dual brain** — deterministic Cascading schedules for predictability, ML-driven Smart retries for optimization, Monitoring for observe-only onboarding.
- **Penalty-aware** — retry budgets are enforced per card network (daily + rolling 30-day), because card networks penalize excessive failed retries.
