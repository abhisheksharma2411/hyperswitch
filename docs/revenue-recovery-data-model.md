# Revenue Recovery — Retry Statistics Data Model

**Status:** Finalized design · **Store:** PostgreSQL (CockroachDB kept portable) + Redis (locking) · **ORM:** Diesel (Rust)

---

## 1. Problem & Goals

The retry engine must decide, per failed payment per day, whether to retry now — maximizing recovery probability while minimizing retry count. The decision is driven by historical per-cluster success statistics over three slot families:

| Slot family | Slots | Meaning |
|---|---|---|
| `dow` | 7 | Day of week (0 = Monday) of the retry |
| `dom` | 31 | Day of month (0-based) of the retry |
| `hod` | 24 | Hour of day (0–23) of the retry |

Each slot holds `n` (retries attempted) and `k` (successful retries). The engine reads a payment's cluster chain, computes per-slot success estimates with confidence, and falls back to less granular clusters when data is insufficient (cold start).

**Design goals**

1. **Zero-DDL evolution (hard constraint, on-prem driven):** the product ships for on-prem hosting. Adding a cluster dimension, a slot family, or time bucketing must require only deploying a new application version — never `ALTER TABLE` or any schema change at customer sites. The schema is final on day one; all structure lives in *data*.
2. Bounded, small read/write fan-out per event (no power-set explosion); event-path writes touch only the leaf.
3. All decision, merge, and derivation logic in the application; the database stores documents, nothing else. No DB-side functions, aggregates, triggers, or views.
4. Plain, portable SQL — no dialect-specific operators in hot statements.
5. Concurrency control via the existing Hyperswitch Redis-based application locking infrastructure (already a required deployment component, including on-prem).

---

## 2. Cluster Model

### 2.1 Fixed generalization chain, not a power set

Clusters use **3 dimensions**: `error_code`, `card_type`, `issuer`. They form a **fixed hierarchy (chain)**, not a subset lattice:

```
{error_code}  →  {error_code, card_type}  →  {error_code, card_type, issuer}
    root                 mid                          leaf
```

- `error_code` is the mandatory root: it is the strongest signal and is present in every cluster.
- Each payment maps to exactly **one path of ≤ 3 nodes**. Fallback = walk up the path. Ancestor keys are computed client-side as prefix truncations — no lookups needed.
- Rationale vs. the power set: most dimension subsets are statistically meaningless (correlated dimensions), the Bayesian shrinkage design requires a unique parent per node, and fan-out drops from 2^N−1 to N per payment.
- Adding a dimension later = one more chain level, encoded purely in key strings (§3.2) — zero DDL.

### 2.2 Sentinels: `*` and `UNK` are distinct

Two special segment values encode two **different** kinds of absence:

| Value | Meaning | Where it appears |
|---|---|---|
| `*` | **Aggregated over** this dimension — a structural ancestor node pooling all children | Ancestor keys only; produced solely by chain-walking code; written only by the refresher (§6) |
| `UNK` | **Unknown** value for a real payment (e.g., BIN lookup failed) | Leaf keys; a real population with its own counters; written by the event path |

Rules:

- Every event lands in **exactly one leaf**. Missing payment data maps to `UNK`, never to `*`.
- Ancestor counters are **pure sums of leaves**: `{IF, D, *} = Σ over issuers (incl. UNK) of {IF, D, issuer}` — maintained by the refresher's recompute (§6), so the invariant is self-healing: any drift washes out on the next cycle.
- `*` and `UNK` must never be legitimate dimension values; ingestion validates/rejects collisions. In Rust: `Dim::Any | Dim::Unknown | Dim::Val(s)`, with `Dim::Any` constructible only by chain-walking code, never from payment data.
- The two-sentinel distinction is what keeps the sum invariant well-defined: unknown-issuer events live in a real `UNK` leaf and are *contained in* the `*` aggregate, rather than being conflated with it.

### 2.3 Time: lifetime counters (no bucketing)

Counters are **lifetime totals** — there is no time-bucket dimension (decision: `month_bucket` removed). Consequences, recorded for the future:

- Old observations never age out: the model adapts slowly to issuer-behavior drift, and confidence terms grow monotonically. Acceptable for now.
- If drift ever bites, time bucketing can be reintroduced with **zero DDL** — as a key-encoding version (e.g., `v2|2026-08|IF/D/HDFC`), a nesting level inside the document, or both — by deploying a new app version. Retention would be `DELETE WHERE cluster_key LIKE 'v2|2026-01|%'`. Nothing about the schema constrains this.

---

## 3. Schema — two columns, final forever

```sql
CREATE TABLE cluster_stats (
    cluster_key TEXT  NOT NULL PRIMARY KEY,   -- app-generated, versioned encoding
    statistics  JSONB NOT NULL                -- nested slot document, §3.3
);
```

That is the entire schema. No other columns, no other DB objects. Every future evolution — new dimensions, new slot families, bucketing, key-format changes — is new *data* written by a new *application version*.

### 3.1 No database-side logic

No user-defined functions, aggregates, triggers, or views; hot-path SQL is a point `SELECT`, a plain upsert, and a `= ANY(...)` fetch — the least dialect-dependent statements possible, portable to CockroachDB unchanged. All merge/aggregation arithmetic lives in Rust (§4, §6).

### 3.2 Cluster key encoding — the load-bearing contract

```
v1|IF/D/HDFC          leaf
v1|IF/D/*             card-type ancestor
v1|IF/*/*             root
v1|IF/D/UNK           unknown-issuer leaf
```

- **Fixed dimension order** (`error_code / card_type / issuer`), one codec implementation (`ClusterKey::as_db()` / `ClusterKey::chain()`), sentinels embedded as segments.
- **Versioned** (`v1|` prefix): a future encoding change (added dimension, bucketing) writes `v2|…` keys that coexist in the same table during lazy migration — the zero-DDL evolution mechanism.
- **Ingestion boundary validates values**: delimiter characters (`|`, `/`) and sentinel spellings (`*`, `UNK`) are rejected or escaped in raw dimension values. A delimiter collision is silent key corruption; this check is not optional.
- The codec is a forever-contract: every consumer (engine, refresher, analytics, support tooling) reads keys through it. Since the key is opaque to SQL, ad-hoc structural queries use `LIKE 'v1|IF/%'` prefix patterns; the chain-shape invariant (no `('*', <issuer>)` keys — §7) is enforced entirely by the codec.

### 3.3 The statistics document — nested by slot family

```json
{
  "dow": {
    "0": {"n": 1843, "k": 512},
    "4": {"n": 1998, "k": 671},
    "6": {"n": 987,  "k": 240}
  },
  "dom": {
    "0":  {"n": 2210, "k": 1104},
    "14": {"n": 733,  "k": 156},
    "30": {"n": 1631, "k": 799}
  },
  "hod": {
    "9":  {"n": 1120, "k": 342},
    "10": {"n": 1345, "k": 460},
    "22": {"n": 512,  "k": 108}
  }
}
```

Two levels: **family → slot → {n, k}**, with slot indices as JSON string keys.

- **Nesting is safe here** because all merging is application-side full-document replace (§4) — no SQL merge operator ever touches the document, so the shallow-merge pitfalls that would make nesting hazardous with DB-side JSONB operators don't apply. If a DB-side merge is ever reintroduced, revisit this.
- **Still schema-generic**: `merge_stats` iterates families and slots generically, so a new family (e.g., `"how"` hour-of-week) is a new top-level key emitted by `slot_keys()` — zero DDL, zero SQL changes.
- **Absent family or slot = zero counts.** Documents start with the first event's slots and grow; no zero-filling. Absence means "never attempted", not "attempted and failed" (a tried-and-poor slot is present with its real n/k).
- **Consistency invariant:** each family is an independent marginal of the same events, so Σn over `dow` = Σn over `dom` = Σn over `hod` within any document. Worth a test and a monitor.
- **Data dictionary lives in code:** slot conventions (dow 0 = Monday, dom 0-based, hod 0–23) are documented on the single `slot_keys()` function — with a schemaless document there is no other place to consult.
- Sizing: mature leaf ≈ 1 KB, fully-populated ancestor ≈ 2 KB compact JSONB — far below TOAST thresholds.

---

## 4. Write Path — leaf-only, Redis-locked read-merge-write

The event path writes **only the payment's leaf row**. Ancestor rows are derived by the refresher (§6) and are never touched by the event path — this keeps the hottest write traffic spread across many leaf keys instead of concentrating on shared ancestor keys, and makes the ancestor/leaf writer separation structural.

Writes use the **existing Hyperswitch Redis application-locking infrastructure**; reads never take locks (§5). Per event:

```
1. acquire  : SET lock:cluster_stats:{leaf_key} {token} NX PX {ttl_ms}
              (retry with jittered backoff on failure, bounded attempts)
2. read     : SELECT statistics FROM cluster_stats WHERE cluster_key = $1
3. merge    : merged = merge_stats(current /* or empty */, delta)   -- pure Rust
4. write    : INSERT INTO cluster_stats (cluster_key, statistics)
              VALUES ($1, $2)
              ON CONFLICT (cluster_key) DO UPDATE SET statistics = EXCLUDED.statistics
5. release  : via the locking infra's guarded delete (checks {token}; never a bare DEL)
```

- **The write is a full-document replace** — no delta arithmetic in SQL, no version column, no rows-affected checks. Correctness rests on the lock: the holder is the only writer of that row for the lock's duration, so read-merge-write is race-free *among lock-honoring writers*.
- **Merge in Rust** (`merge_stats`): fold the event's delta slots into the current document, creating absent families/slots at zero. Pure, unit-testable; merge is associative/commutative over deltas — property-test it. This function is the single place document-shape assumptions live; prefer failing loudly on malformed entries over absorbing them as zeros.
- **First write**: no row found in step 2 ⇒ merge against empty ⇒ the upsert's INSERT arm creates the row. One statement covers both cases.
- **Delta document**: just the event's touched slots, in the same nested shape (`{"dow":{"4":{"n":1,"k":1}}, "dom":{"0":…}, "hod":{"10":…}}`). One delta value drives the whole event (slots computed once — a midnight boundary mid-processing must not split families across slots).
- **Coalescing-ready**: buffering ~200 ms of increments per leaf app-side and merging before a single locked write divides lock acquisitions and hold time on busy leaves by the batch factor. Wire the trigger metric (lock wait time / acquisition failures per key) from day one.
- **Bounded lock acquisition**: after N failed attempts, do not block the payment path — emit a metric and queue the delta for a later coalesced write (a silently dropped delta is counter drift; prefer queueing).

**Locking protocol requirements** (load-bearing):

- **TTL sizing**: PX TTL must comfortably exceed worst-case read+merge+write latency (rule of thumb: ≥ 10× p99). Monitor lock hold times against the TTL.
- **Token-guarded release only** — a bare `DEL` can release another writer's lock.
- **Residual risk, accepted**: a TTL-based lock without fencing cannot fully exclude a writer that stalls past its TTL and then completes its stale write — that overlapped increment is silently lost. Accepted because: increments are small relative to counters, exposure is minimized by short hold times + coalescing, and for ancestors the refresher's recompute (§6) continuously restores exact sums. Keep hold-time monitoring in place so TTL breaches are visible, not silent.
- **Every writer, forever, takes the lock** — engine (leaves) and refresher (ancestors) today; backfills and support fixups tomorrow. The database does not enforce this convention; the single `record_node()` chokepoint and code review must (§7 #2).
- **Redis unavailability** fails writes closed: the payment retry itself proceeds, the statistics write is queued/skipped with a metric. Statistics are advisory; payment processing never depends on the lock service.

---

## 5. Read Path — lock-free

### 5.1 Chain fetch (prediction)

Reads take no lock: MVCC gives a consistent document snapshot per row, and the estimator is insensitive to reading a document one event (leaf) or one refresh cycle (ancestors) stale.

```sql
SELECT cluster_key, statistics FROM cluster_stats WHERE cluster_key = ANY($1);
```

`$1` = the chain keys from `ClusterKey::chain()` (root, mid, leaf) — client-computed, one round trip, ≤3 point rows, a few KB. The application extracts today's three slots per node in Rust (`slot_keys(now)`), treats absent rows/families/slots as zero, identifies each row's depth from its key (`*` segment count), and runs the shrinkage/confidence/fallback walk. Keeping slot extraction in Rust keeps the read SQL slot-vocabulary-agnostic.

Ancestor staleness (≤ one refresh interval, §6) is directionally conservative: understated ancestor n makes fallback marginally more cautious, never falsely confident.

### 5.2 Exploration counters

The `n < 50 ⇒ explore` rule and the retry-budget bound operate on the payment's most granular cluster — read the **leaf key only** (same query, one-element array). Leaves are written synchronously by the event path, so exploration always sees near-real-time counts and never depends on refresher staleness.

---

## 6. Refresher — ancestors derived from leaves (current phase)

Ancestor documents are a **materialized aggregation over leaves, recomputed by a Rust refresher** on a ~2-minute cycle. This ships in the current phase alongside the event path; it is the only writer of `*` keys.

Per error code (or per dirty subtree, if marking is added later):

1. **Fetch leaf documents**: `SELECT cluster_key, statistics FROM cluster_stats WHERE cluster_key LIKE 'v1|IF/%'`, then classify rows through the codec and **discard `*` keys** — the scan must aggregate leaves only. Optional on CRDB: `AS OF SYSTEM TIME follower_read_timestamp()` to avoid contention with live leaf writes.
2. **Sum in Rust**: slot-wise n/k addition (the same `merge_stats` core) into two ancestor documents per subtree — the root `{ec, *, *}` and one `{ec, ct, *}` per card type. `UNK` leaves flow through like any value, so the root correctly contains the unknown population and `{ec, UNK, *}` gets its own node.
3. **Write each ancestor under the standard lock protocol (§4 steps 1–5)** — but as a full-document **replace with the recomputed value**, never a merge of deltas. Replace-not-merge is what makes each cycle a recompute from ground truth: crashes, missed runs, TTL-breach drift, and past bugs all wash out on the next pass (self-healing).

Operational notes:

- **Single logical runner**: run one refresher instance (or partition by error code) — the lock protocol makes overlapping runs safe (last replace wins with a consistent recomputed value), but a single runner keeps cycles cheap and metrics clean.
- **Two hard rules** (invariants §7 #3): the leaf scan excludes `*` keys — folding prior ancestors back in causes compounding double-counts, inflating n exponentially cycle over cycle; and the refresher REPLACEs while the event path MERGEs — blurring these corrupts ancestors quietly.
- **Staleness contract**: ancestors lag ≤ the cycle interval; statistically invisible to the estimator (§5.1), and exploration never reads ancestors (§5.2). Start at 2 minutes; tune on data.
- **Doubles as the repair tool**: a one-shot run for a subtree restores exact ancestor = Σ(leaves) after any suspected corruption (bug, manual edit, lock-TTL breach).
- **Retention & housekeeping** (when bucketing or key-version migrations arrive) fold naturally into this cycle.

---

## 7. Invariants & Guardrails

All invariants are enforced in **application code** — the two-column schema constrains nothing beyond key uniqueness. These are load-bearing:

| # | Invariant | Enforcement (application) |
|---|---|---|
| 1 | Writer separation: event path writes only leaf keys; refresher writes only `*` keys | `Dim::Any` unconstructible from payment data; ancestor keys come only from `ClusterKey::chain()`/refresher code. No code path yields a `('*', <issuer>)` key |
| 2 | Every writer of `cluster_stats` holds the Redis lock for that key | Convention enforced by a single `record_node()` chokepoint that all writers (engine, refresher, backfills, fixups) use + code review. The DB does not enforce this |
| 3 | Refresher scan aggregates leaves only (discards `*` keys); refresher REPLACEs, event path MERGEs | Codec-side classification in the scan; two distinct write functions with documented semantics — never blur them. Violation causes compounding double-counts or quiet clobbering |
| 4 | Lock release is token-guarded; TTL ≥ 10× p99 write latency; hold times monitored | Hyperswitch locking infra + metrics; TTL breaches must be visible |
| 5 | One delta value drives all of an event's slot updates | Slots computed once per event; prevents family-splitting at day/hour boundaries |
| 6 | Key codec is versioned and validated | `v1|` prefix; ingestion rejects/escapes `|`, `/`, `*`, `UNK` in raw values; codec logic exists in exactly one module |
| 7 | Analytics must not double-count sentinels | Ad-hoc/tuning SQL filters ancestor keys (prefix patterns or codec-side classification); reviewed convention |
| 8 | Per-document marginal consistency | Σn over dow = Σn over dom = Σn over hod; checked in tests and sampled by a monitor |
| 9 | Ancestor = Σ(leaves) monitoring | Sampled subtree check after refresh; persistent or growing drift ⇒ bug or lock-protocol breach (transient sub-cycle drift is expected) |

Integration tests: `*`-bearing key from payment data must be unconstructible; `merge_stats` property tests (associative, commutative, absent-family/slot creation); refresher round-trip (write leaves → run cycle → ancestors equal Rust-side sums; run twice → identical result, proving no double-counting); chain-fetch results are always key-prefixes of the payment key; concurrent `record_node` calls on one key under the lock never lose increments.

---

## 8. Diesel Integration

Every statement is expressible in Diesel's **typed DSL** — the raw-SQL surface is zero:

- Read: `cluster_stats.select((cluster_key, statistics)).filter(cluster_key.eq_any(keys))`
- Refresher scan: `.filter(cluster_key.like(prefix))`
- Write: `insert_into(cluster_stats).values(...).on_conflict(cluster_key).do_update().set(statistics.eq(excluded(statistics)))`
- `JSONB ↔ serde_json::Value` binds natively (`serde_json` feature).

The Redis lock acquire/release goes through the existing Hyperswitch locking module. `record_node()` (lock → read → merge/replace → upsert → release) is the single write chokepoint shared by the event path and refresher; `merge_stats()` and `slot_keys()` are the two pure functions carrying all document semantics.

---

## 9. Concurrency Contract Summary

| Path | Mechanism | Guarantee |
|---|---|---|
| Event write (leaf only) | Redis lock per leaf key → read → Rust merge → full-doc upsert → guarded release | Race-free among lock-honoring writers; residual TTL-breach window accepted & monitored (§4) |
| Refresher write (`*` keys only) | Same lock protocol; full-doc replace of recomputed value | Self-healing recompute; overlap-safe |
| Prediction read | Lock-free `= ANY` point reads | MVCC snapshot; leaf ≤ one event stale, ancestors ≤ one refresh cycle stale — both statistically invisible |
| Exploration read | Lock-free leaf point read | Near-real-time leaf counts |

Hot-key exposure is limited to **busy leaves** (e.g., `v1|IF/D/HDFC`) — root keys left the event path entirely with the refresher. Relief valve: app-side coalescing (§4).

---

## 10. Portability Ledger

| Component | Postgres | CockroachDB |
|---|---|---|
| Point SELECT / `= ANY` fetch / prefix LIKE / plain `ON CONFLICT` upsert | ✔ | ✔ |
| JSONB storage type (opaque read/write; no operators in hot path) | ✔ | ✔ |
| Refresher leaf scan (`AS OF SYSTEM TIME` optional) | ✔ (omit AOST) | ✔ |
| Redis locking | independent of the SQL engine | independent of the SQL engine |
| Avoided entirely | — | UDFs, aggregates, `ROLLUP`, matviews, subscript writes, JSONB merge operators, arrays, version-CAS |

This is the most portable incarnation of the design: the database is used as a locked key-value document store, and all semantics live in Rust.

---

## 11. Sizing & Evolution

**Sizing.** One row per cluster node: worst case ~(50 error codes × 3 card types × ~300 issuers + interiors) ≈ 180K rows, ≈ 1–2 KB each ⇒ the whole table is a few hundred MB ceiling, in practice far less (most leaves are sparse young documents). Bounded by **catalog cardinality, never by traffic**. Refresher full-cycle scans stay sub-second per error code at this scale.

**Zero-DDL evolution stories** (the point of this design):

- *New dimension* (e.g., `network`): new `v2|` key encoding with one more segment; app maps old `v1|` keys as coexisting aggregates or lazily migrates; refresher gains one accumulation level. No DDL.
- *New slot family* (e.g., hour-of-week): `slot_keys()` starts emitting a `"how"` family; documents absorb the new top-level key via `merge_stats`; refresher sums it like any family. No DDL, no SQL changes at all.
- *Time bucketing* (if drift bites): `v2|{bucket}|…` key segment or a document nesting level; retention = key-prefix DELETE in the refresher cycle. No DDL.
- *Codec change of any kind*: version prefix lets old and new keys coexist during migration; the refresher is the natural place to run lazy rewrites.

**Future Redis-data path** (if read latency ever demands): rows map 1:1 to Redis keys (`cluster_key` → document); with data and locks in the same Redis, per-slot `HINCRBY` on flattened fields becomes available and the lock protocol can retire. The migration surface is a table scan.

---

## 12. Decision Log (summary)

| Decision | Chosen | Over | Key reason |
|---|---|---|---|
| Cluster space | Fixed chain, error-code root | Power set (2^N−1) | Fan-out N vs 1023; unique parent required by shrinkage math; correlated dims make most subsets meaningless |
| Schema stability | Two-column key+JSONB document table, final on day one | Per-dimension columns; array columns; row-per-slot | Hard on-prem constraint: evolution must never require DDL — dimensions, slot families, and bucketing all live in data |
| Key encoding | App-generated versioned path string (`v1|IF/D/HDFC`), single codec module | Per-dimension columns with sentinels (previously chosen) | Overturned by the zero-DDL constraint; versioned prefix preserves migration ability; introspection cost accepted, mitigated by prefix LIKE + codec-aware tooling |
| Absence encoding | `*` (aggregate) + `UNK` (unknown) as key segments, distinct | Single sentinel / NULL | Two different absences; preserves ancestor = Σ(leaves); UNK is a real population with signal |
| Statistics layout | Nested JSONB document: family → slot → {n,k}, absent = zero | Flat "family:slot" map; arrays; 90 columns; row-per-slot | Readability of the document; nesting is safe because all merging is app-side full-doc replace (no SQL merge operators); still slot-vocabulary-generic via `merge_stats`/`slot_keys` |
| Concurrency | Redis SETNX app locking (existing Hyperswitch infra); read-merge-write full documents; reads lock-free | In-DB JSONB merge; `SELECT FOR UPDATE`; version-column CAS | Redis already required on-prem; reuses proven infra; merge logic in Rust; plainest possible SQL; FOR UPDATE rejected for unbounded lock on hung connections, CAS for unbounded retries under contention; TTL-breach residual risk accepted & monitored, repairable via §6 recompute |
| Ancestors | Refresher-derived, current phase: event path writes leaves only; Rust refresher recomputes and REPLACEs `*` keys on a ~2-min cycle | Synchronous per-event write-through of ancestors | Removes root-key lock contention from the event path; structural writer separation; self-healing sums; staleness ≤ cycle, statistically invisible and conservative |
| Time | Lifetime counters, no bucketing | `month_bucket` / weekly buckets | Simplicity per review; drift-adaptation caveat recorded; bucketing reintroducible with zero DDL via key versioning |
| Slot families | dow (7), dom (31), hod (24) | 24h-delta-since-last-retry | Hour-of-day timing signal preferred; note hod does not encode inter-retry spacing |
