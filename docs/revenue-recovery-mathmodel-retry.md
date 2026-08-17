# Solution Doc — MathModel Retry Algorithm (Cascading + Opportunistic Retries)

> **Status**: Proposed
> **Scope**: Hyperswitch Revenue Recovery (`router` crate + external recovery-decider gRPC service)
> **Related**: `docs/revenue-recovery.md` (module explainer)

---

## 1. Goal

Introduce a fourth revenue-recovery retry algorithm, **MathModel**, that preserves the deterministic Superposition ("Cascading") schedule as a guaranteed baseline while allowing an ML prediction model to insert **additional, earlier retries** between static offsets.

**Core rule (per retry cycle):**

```
next_retry_time = min(next_static_offset_time, math_model_predicted_time)
```

- MathModel can only ever make a retry happen **sooner** — never later, never skip a static slot.
- If payments keep failing, **every static retry still fires**. MathModel retries are strictly additive.
- If the prediction service fails or returns garbage, behavior degrades to pure Cascading.

### Worked example — static offsets Aug 10, Aug 13, Aug 17

| Cycle runs | Static next | Model predicts | Scheduled | Tag |
|---|---|---|---|---|
| Aug 5 | Aug 10 | Aug 7 | **Aug 7** | math_model (extra) |
| Aug 7 fails | Aug 10 | Aug 9 | **Aug 9** | math_model (extra) |
| Aug 9 fails | Aug 10 | Aug 14 | **Aug 10** | static (slot consumed) |
| Aug 10 fails | Aug 13 | Aug 11 | **Aug 11** | math_model (extra) |
| Aug 11 fails | Aug 13 | Aug 13 | **Aug 13** | static (epsilon collapse) |

---

## 2. Key Design Decision — Model Lives in the Existing gRPC Decider

The MathModel prediction logic is implemented **inside the existing external recovery-decider gRPC service** (the same service that powers the Smart algorithm). Hyperswitch does not host the model.

To distinguish requests, one field is added to the decider contract:

```proto
// proto/recovery_decider.proto
message DeciderRequest {
  // … all existing 32 fields …
  // 33: which algorithm the decider should score this request with.
  // Absent/empty = "smart" (backwards compatible with the current behavior).
  optional string retry_algorithm = 33;  // "smart" | "math_model"
}
```

- The decider service branches internally on `retry_algorithm`. Existing Smart callers are untouched (absent ⇒ smart).
- The gRPC client in `external_services` (`recovery_decider_client.rs`) gains only the plumbing to populate field 33 from the calling algorithm.
- Hyperswitch-side orchestration (static schedule lookup, `min()`, Redis bookkeeping, guard rails) lives in the router — the decider only returns a predicted time, exactly as it does today.

This keeps the external contract to a single additive proto field and reuses the entire Smart request/response pipeline (headers, client, token-scoring loop).

---

## 3. Where It Plugs In

Unchanged: webhook ingest, EXECUTE_WORKFLOW, PSYNC_WORKFLOW, hard-decline check, budget accounting, Redis lock lifecycle, record-back to billing connector, outgoing webhooks. MathModel only affects **when** retries are scheduled, inside `CALCULATE_WORKFLOW`.

| Component | Change |
|---|---|
| `common_enums` — `RevenueRecoveryAlgorithmType` | New variant `MathModel` (profile-level opt-in via `revenue_recovery_retry_algorithm_type`) |
| `proto/recovery_decider.proto` + `external_services` gRPC client | Add `retry_algorithm` field (§2) |
| `router/src/workflows/revenue_recovery.rs` — token/time selection | New `MathModel` branch: reuse existing **Cascading** static-schedule logic for the static time; call the decider (with `retry_algorithm="math_model"`) per candidate token; compute per-token `min(static, model)`; select the token with the earliest resulting time |
| `router/src/types/storage/revenue_recovery_redis_operation.rs` | Three additive fields on `PaymentProcessorTokenStatus` (§4) |
| `router/src/core/revenue_recovery/types.rs` (failure handler) | Advance static pointer based on stored provenance (§4) |
| Config (`[revenue_recovery]` TOML / profile `revenue_recovery_retry_algorithm_data`) | MathModel guard-rail knobs (§5) |
| Kafka `RevenueRecovery` event | New `retry_source: static \| math_model` field for attribution |

### CALCULATE flow with MathModel enabled

```
perform_calculate_workflow (MathModel branch)
  ├─ hard-decline check (unchanged, dominates everything)
  ├─ static_time = static_schedule[redis_token.static_retry_index]     ← pointer, NOT retry count
  ├─ for each eligible token:
  │     model_time = decider.Decide(retry_algorithm="math_model", …)
  │     candidate  = apply_guard_rails( min(static_time, model_time) )
  └─ pick token with earliest candidate →
        write Redis: scheduled_at = candidate,
                     scheduled_by = static | math_model
        → insert EXECUTE_WORKFLOW at candidate
```

Fallback order inside the branch: decider error ⇒ static time ⇒ if static schedule exhausted ⇒ terminate as today (`HardDecline`/`Finish` path unchanged).

---

## 4. State Management — the Static Pointer

The one piece of genuinely new state machinery. Today the Cascading schedule is effectively count-derived; interleaved model retries would corrupt that derivation, so progression becomes explicit.

### New fields on the Redis token hash (`PaymentProcessorTokenStatus`)

| Field | Type | Meaning |
|---|---|---|
| `static_retry_index` | `u32` (default 0) | Which static offset fires next. **Advanced only when a static-tagged retry fires.** `#[serde(default)]` for backward compatibility with existing tokens. |
| `mathmodel_retries_in_current_window` | `u32` (default 0) | Inserted retries since the last static slot fired. Enforces `max_mathmodel_retries_per_window`; reset to 0 when `static_retry_index` advances. |
| `scheduled_by` | `"static" \| "math_model"` (default `"static"`) | Provenance of the pending `scheduled_at`. |

### Pointer advance rule (runs once per failed retry, idempotently)

When EXECUTE (or PSYNC) finalizes a **failed** attempt and reopens CALCULATE:

```
if token.scheduled_by == "static": static_retry_index += 1; mathmodel_retries_in_current_window = 0
else:                              mathmodel_retries_in_current_window += 1
```

Idempotency under requeues/crashes is achieved by making advancement **derived from evidence**, not fire-and-forget: the handler only advances when the failed attempt corresponds to the token's current `scheduled_at` slot (attempt already recorded ⇒ skip). This tolerates the `TRIGGER_REQUEUE_FOR_EXECUTE_WORKFLOW` duplicate-fire path.

Token selection and the customer-level SETNX lock are unchanged; the lock still guards all mutations.

---

## 5. Guard Rails

Prevents a bad model from exhausting retry budgets or clustering retries. All enforced Hyperswitch-side (the model is advisory).

| # | Guard rail | Default proposal | Purpose |
|---|---|---|---|
| 1 | `max_mathmodel_retries_per_window` | 2 | Cap inserted retries between two static slots |
| 2 | `math_model_min_gap_seconds` | 6h | Clamp predictions that are too close to "now" |
| 3 | **Budget reservation** | enabled | Before honoring a model time: `remaining_budget > remaining_static_slots`. Model retries share the existing daily + rolling-30-day network budgets — they must never starve a guaranteed static retry. |
| 4 | **Static-imminence window** (`static_imminence_seconds`) | 12h | No model insert once the static slot is imminent — prevents ping-pong clustering just before the offset |
| 5 | `math_model_enabled_after_static_exhaustion` | false | Once the static schedule is exhausted, terminate exactly as Cascading does today (model cannot extend a plan) |
| 6 | Epsilon collapse (`schedule_epsilon_seconds`) | 1h | `|model − static| < ε` ⇒ one retry, tagged static |
| 7 | Past-time clamp | `now + job_schedule_buffer` | Model returning past/stale times ⇒ clamp + metric |
| 8 | Jitter | existing `max_random_schedule_delay_in_seconds` | Applied to model times too (thundering-herd protection on e.g. salary-day predictions) |
| 9 | Hard decline | unchanged | Dominates everything; checked before any model call |
| 10 | `max_total_retries_per_invoice` | config | Absolute outer fuse per payment intent |

Termination paths (`HardDecline` finish, success, cancelled invoice, manual unlock) are identical to Cascading — MathModel can never create work beyond the reopen cycle.

---

## 6. Edge Cases Handled

- **Crash between EXECUTE and pointer advance** → idempotent, evidence-derived advancement (§4)
- **External success while a model retry is in flight** → caught by existing Decision logic (`ReviewForSuccessfulPayment`) + CALCULATE's psync of active attempts; model retries make this race *more frequent*, so a regression test is required
- **Static config change mid-plan** (Superposition offsets edited) → clamp `static_retry_index` to schedule length; new invoice/intent resets pointer to 0
- **Profile algorithm switch mid-cycle** (Cascading → MathModel) → pointer state is additive with serde defaults; next CALCULATE computes from current profile cleanly
- **Account Updater replaces card mid-cycle** → decider is called fresh per CALCULATE with current card metadata; no cached predictions
- **Model outage** → silent fallback to static + `mathmodel_fallback_total` metric + alert on fallback rate
- **Degenerate constant predictions** (same time for all customers) → distribution-drift metric on predicted times

---

## 7. Observability

- **Kafka attribution**: `retry_source` (static | math_model) on every recovery event → recovered-revenue lift measurement for inserted retries vs pure-Cascading cohort.
- **Metrics**: `mathmodel_retries_scheduled_total`, `mathmodel_fallback_total`, histogram `mathmodel_time_minus_static_time`, guard-rail rejection counters per rule, constant-output drift.
- **Shadow mode** (rollout step): compute and log the `min()` decision *without* scheduling it — validates delta distribution and fallback rate before enabling.

---

## 8. Rollout Plan

1. **Proto + client**: add field 33; deploy decider service with `math_model` branch (can return Smart-like heuristics initially).
2. **Router**: enum variant + MathModel branch + Redis fields (default off).
3. **Shadow mode** on a subset of Cascading profiles for ~1–2 weeks: log decisions, no schedule changes. Validate: fallback rate < 1%, delta distribution sane, no budget violations.
4. **Enable** per-profile (`revenue_recovery_retry_algorithm_type = math_model`), starting with low-volume merchants.
5. **Compare** recovered revenue vs pure-Cascading cohorts via `retry_source` attribution; tune guard-rail defaults.

---

## 9. Open Questions

1. **Decider enum vs string** for field 33 — string keeps the proto stable for future algorithms; an enum is stricter. Current proposal: string with documented values.
2. Should a **successful** MathModel retry skip remaining static slots? (Assumed yes — success ends the cycle as today.)
3. Do model retries in a **partially-captured** (top-up) scenario follow the same window counter, or a separate budget?
4. Exact guard-rail defaults (§5) — to be tuned after shadow-mode data.

---

## 10. Implementation Checklist (router-side)

- [ ] `RevenueRecoveryAlgorithmType::MathModel` variant (common_enums + profile plumbing)
- [ ] Proto field 33 + client plumbing (`external_services`)
- [ ] MathModel branch in token/time selection: static lookup by pointer → per-token decider → guard rails → min → earliest token
- [ ] Redis: `static_retry_index`, `mathmodel_retries_in_current_window`, `scheduled_by` (serde defaults)
- [ ] Idempotent pointer advance in failure handler (+ window counter)
- [ ] Guard-rail enforcement + config knobs (TOML + profile data)
- [ ] Kafka `retry_source` field
- [ ] Metrics + shadow-mode flag
- [ ] Backfill repair support for the three new fields (ops endpoint)
- [ ] Tests: external-success race, pointer idempotency, budget reservation, imminence window, epsilon collapse, static-guarantee invariant (property test: for any model output sequence, static slots all fire on repeated failure)

---

## 11. PoC Implementation Notes (lands first)

These are the resolved design bits for the first working slice, landed as `crates/router/src/core/revenue_recovery/cluster_stats/` plus migration `2026-08-13-000001_create_cluster_stats`:

### 11.1 Where outcomes are recorded — no webhook needed

| Path | When | Location | Statuses covered |
|---|---|---|---|
| `Action::execute_payment_task_response_handler` | EXECUTE_WORKFLOW returns terminal (Succeeded / Failed) synchronously | `crates/router/src/core/revenue_recovery/types.rs` — the `Self::SuccessfulPayment(payment_attempt)` and `Self::TerminalFailure(payment_attempt)` arms | Sync-succeeded, sync-failed |
| `Action::psync_response_handler` | PSYNC_WORKFLOW settles an initially-Processing attempt into terminal | same file — `Self::SuccessfulPayment(payment_attempt)` / `Self::TerminalFailure(payment_attempt)` arms | Async-succeeded, async-failed |

The two handlers cover **100% of internal retry outcomes** — recovery webhooks deliberately return `NoAction` for `TriggeredBy::Internal`, and the standard payment webhook path does not re-notify recovery. The known hole is **`PartialCharged`**: neither handler passes the attempt to that arm, so the PoC doesn't record it. That surface needs a dedicated follow-up; treat partial-charge recovery as currently-uncounted in the stats model.

### 11.2 Cluster key codec (back/forward compatible)

```
v{protocol}|{unified_error_code}/{card_network}/{issuer}
```

(`card_network` sources the second segment in the PoC — replacing the legacy `{card_type}` naming from the data-model doc. The PoC does not distinguish network vs. funding-type here; if the stats need finer granularity, fold in `card_type` from Redis as an additional segment in v2.)

- **`unified_error_code`** uses `payment_attempt.error.unified_code` (GSM-normalized) when populated, falling back to connector-specific `error.code` only when the unified field is absent. GSM normalizes e.g. Stripe `"card_declined"`, Adyen `"Refused"`, and a bare `"05"` into one spelling, so cluster statistics pool correctly across connectors. The raw `payment_attempt.error.code` is *not* used for keying.

> **Open follow-up (beyond PoC):** `check_hard_decline` in `workflows/revenue_recovery.rs` already resolves `ErrorCategory::{Hard,Soft}Decline` via the GSM table; if PoC data shows `unified_code` is sparsely populated for retry-attempt webhooks, the error_code dim can be upgraded to also fold in the `ErrorCategory` as a coarser sibling bucket so `{category}/{network}/{issuer}` is a fallback chain for sparse leaves.

- `v1|card_declined/visa/HDFC` is a leaf; ancestors use `*` in trailing positions.
- `Dim` enum: `Val`, `Unknown` (bad/missing), `Any` (wildcard — unconstructible from event data, type-enforced).
- Missing/atypical values map to `UNK`. Restricted characters `% | / *` inside a dimension value are **percent-encoded** per segment, so v2 can add more segments without re-keying existing rows, and SQL `LIKE 'v1|ec/%'` prefix scans keep working for per-error-code refresher passes.
- The chain `[root, mid, leaf]` is derivable from any node via `ClusterKey::chain()` for future refresher/ancestor writes.

### 11.3 Stub for the MathModel decision function (other dev's)

```rust
fn compute_mathmodel_retry_time(
    _key: &ClusterKey,
    _doc: &StatsDocument,
    _window: &RetryWindow,
) -> Option<OffsetDateTime> { None }
```

The CALCULATE branch in the workflow will call it later and `min()` the result with the dynamic static-schedule time. `None` currently means "no opinion", which the router-side min-logic treats as "model didn't propose — use static only."

### 11.4 PoC write-path is log-only

`record_outcome` in `cluster_stats/record.rs` currently `tracing::info!`s the cluster key and slot values when `revenue_recovery.enable_retry_stats_logging = true` (new optional field under `[revenue_recovery]`). The diesel model + query layer under `crates/diesel_models/src/{cluster_stats,query/cluster_stats}.rs` and migration `2026-08-13-000001_create_cluster_stats/` are wired and ready; flipping the PoC to persist requires only swapping the record stub to call `ClusterStatsNew::insert_or_replace` and adding the per-key Redis lock before handoff.

### 11.5 `PartialCharged` is **not** recorded in the PoC

Both response handlers (`execute_payment_task_response_handler`, `psync_response_handler`) match `Self::PartialCharged` with no attempt in scope, so there's a blind spot in training data: partially-captured retries neither count as successes nor failures in this iteration. If partial captures are common for a merchant, their cluster will look weaker than reality. Options: (a) accept the bias and document it, (b) lift `PartialCharged` to also bind `payment_attempt` in a follow-up, or (c) count partial as success with reduced weight. Recommendation: (b) — smallest, keeps data honest.
