# Engineering Decisions Log

## 2026-06-01 - Pin local Python and Java versions

**Decision:**  
Use Python 3.11.9 through `pyenv` and Java 17 through Temurin for local PySpark development.

**Why:**  
PySpark/Spark compatibility is sensitive to Python and Java versions. Pinning known-working versions prevents local setup issues and makes the project easier to reproduce. Python 3.11 and Java 17 are conservative choices for Spark 3.5 local development.

**Tradeoffs:**  
This local setup does not exactly match Databricks Runtime 14.3 LTS, which uses Python 3.10.12 and Java 8. The local environment is mainly for development and testing smaller pieces of the project; Databricks remains the primary runtime for the full pipeline.


## 2026-06-01 - Use pyenv for Python version management

**Decision:**  
Use `pyenv` to manage the local Python version instead of relying on the system Python or Homebrew Python.

**Why:**  
The project needs a stable Python version for PySpark. `pyenv` makes the Python version explicit through `.python-version`, which helps avoid differences between machines and future Homebrew upgrades.

**Context:**  
During setup, a Homebrew Python 3.11 environment hit a local `pyexpat` symbol mismatch related to outdated Command Line Tools. Using `pyenv` avoided that issue and made the environment easier to control.

## 2026-06-11 - Data Modeling Descions
 - Drafted v1 star schema for fact_claims (PNG + prose)
  - Locked grain: accumulating snapshot, one row per claim
  - Locked SCD types: SCD2 on policy/customer, SCD1 on agent/property/geography/cat_event
  - Documented role-playing date dimension and degenerate dimension patterns

  ## 2026-06-14 — Storage layer: Delta Lake

**Decision:** Use Delta Lake as the storage format for all three medallion layers (bronze, silver, gold).

**Why:**
- ACID transactions mean concurrent reads see consistent snapshots even mid-write — important once Airflow is orchestrating multiple jobs touching the same tables
- MERGE operations enable clean SCD2 implementation in a single SQL statement; doing SCD2 on plain parquet requires read-modify-write of entire partitions and is error-prone
- Schema enforcement catches upstream schema breaks at ingest rather than silently corrupting downstream data
- Time travel supports debugging ("what did fact_claims look like before this morning's load?") and reproducibility (re-run yesterday's analytics against yesterday's data)
- Native to Databricks Runtime — no extra setup required

**Trade-offs:**
- Lock-in to Delta as the format (mitigated by Delta being open source and supported outside Databricks via delta-rs and OSS Delta)
- Slight write-time overhead vs raw parquet (negligible at this project's scale)

## 2026-06-14 — Architecture: medallion (bronze/silver/gold)

**Decision:** Three-layer medallion architecture, with Delta tables at every layer.

**Why three layers, not one direct raw-to-star pipeline:**
- Bronze isolates ingestion from transformation. If silver logic has a bug, replay from bronze rather than re-pulling source data (which may not be repeatable — FEMA datasets get updated/revised over time).
- Silver isolates cleaning from modeling. Cleaning (dedup, type cast, null handling, validation) is general-purpose. Modeling (dim/fact design, SCD, aggregations) changes as business questions evolve. Separating them avoids re-doing one when changing the other.
- Gold isolates the analytical contract from implementation. Dashboards and analysts query gold; gold's schema is the stable interface. Bronze and silver can be refactored freely behind it.

**Layer contracts:**
- **Bronze:** schema-on-read, append-only, minimal transformation, audit columns (`_ingested_at`, `_source_file`, `_pipeline_run_id`). Source of truth.
- **Silver:** typed, deduped, validated, conformed. One row per business entity at its natural grain. Light business rules (reject clearly invalid rows) but no analytical measures yet.
- **Gold:** star schema, SCD2 where required, computed measures, denormalized where it helps query patterns. What stakeholders and dashboards see.

**Rebuild guarantee:** Every layer is reproducible from the layer below it. Bronze is the only layer that depends on external systems.


## 2026-06-20 — Synthetic data: 1:1 with FEMA claims for v1

**Context:** FEMA NFIP redacts customer, agent, policy details. We synthesize them.

**Decision (v1 simplification):** Generate one synthetic customer and one synthetic policy per FEMA claim, joined via the FEMA `id` UUID.

**Trade-off:** This simplification produces a 1:1:1 model that is easier to reason about for v1 but doesn't model multi-claim policyholders or repeat-loss properties.

**v2 plan:** Group claims by (state, county, dateOfLoss-window, coverage similarity) to consolidate to one policy with multiple claims where plausible.

## 2026-06-23 — Silver passes for clean-by-construction synthetic data

**Decision:** Synthetic data still flows through silver (`silver.{agents,customers,policies}_clean`) even though generators enforce cleanliness upstream.

**Why:** Silver is the typed contract layer for all downstream consumers (gold, dashboards). Routing synthetic data through silver gives uniform interface: any silver consumer can assume types are correct, audit columns are present, and table location is predictable. The transformation pass is cheap (type tighten + audit columns) but the architectural consistency is valuable.

## 2026-06-23 — Selective projection + broadcast join in enrichment

`silver.claims_enriched` join strategy:
1. Inner join claims to policies on `id = fema_claim_id` (1:1 by validator)
2. Inner join to customers on `customer_id`
3. Inner join to agents with `F.broadcast()` hint — agents table is 75 rows

Pre/post row counts asserted to catch silent data loss. Result: 2,721,780 rows = source claim count, confirming 100% FK integrity from validator.

Columns selectively projected before joining to (a) prevent name collisions, (b) reduce shuffle data volume. Customer / agent / policy columns prefixed/aliased for analytical clarity downstream (e.g., `policy_effective_date` not just `effective_date`).


## 2026-06-24 — Gold layer landed; KPIs validate against industry numbers

`gold.fact_claims` v0 + `gold.dim_date` built. First star schema query against published NFIP industry payouts matched within single-digit percentage:

| Event | Project total | Published NFIP total | Δ |
|---|---|---|---|
| Hurricane Katrina | $16.26B | ~$16.3B | <1% |
| Hurricane Sandy | $8.96B | ~$8.5B | ~5% |
| Hurricane Harvey | $9.06B | ~$9B | ~1% |

This validation matters: the pipeline doesn't just run; it produces correct numbers. Sanity-check queries against external published figures should remain a permanent fixture of the gold layer.

### Hurricane-season analytical query (one-line via dim_date.hurricane_season_flag)
- In-season: 1.91M claims, avg severity $40,769, $77.78B total
- Off-season: 814k claims, avg severity $14,641, $11.92B total
- Hurricane-season claims are 2.8x more severe — directionally correct for the domain
- This query exists because `hurricane_season_flag` is a domain attribute on dim_date. Without it, the same insight would require month-comparison logic in every query.




## 2026-06-28 — Synthesized claim lifecycle dates (silver layer)

FEMA does not provide `date_filed`, `date_first_payment`, or `date_closed`. Synthesized in `silver.claims_clean` using deterministic per-claim seeded offsets from `dateOfLoss`:

- `date_filed = dateOfLoss + uniform(1, 30) days`
- `date_first_payment = date_filed + uniform(7, 90) days` (NULL when no payment recorded — affects 566,351 of 2.72M claims = 21%)
- `date_closed = date_filed + uniform(30, 365) days` (constrained ≥ date_first_payment when both exist)

**Determinism via MD5(id || salt) hash → modulo offset.** Each claim's three offsets are derived from salted MD5 hashes of the FEMA `id` UUID. Same UUID → same dates. Critical for reproducibility: non-deterministic lifecycle dates would break point-in-time joins in fact_claims v1 and would produce different cycle-time measures across pipeline runs.

**Validated distributions:**
- days_loss_to_filed: min 1, median 16, max 30 (uniform across configured range)
- days_filed_to_closed: min 30, median 198, max 365 (uniform across configured range)
- date_first_payment NULL rate: 21% (matches Day 5 profiling exactly)

**Disclosure:** Lifecycle dates are synthesized, not from FEMA. Cycle-time analytics in this project demonstrate the modeling pattern but are not statements about real NFIP claim-processing performance.


## 2026-06-30 — dim_agent: SCD Type 1 + Delta time travel separation

**Decision:** `gold.dim_agent` built as SCD Type 1 (75 rows, overwrite-on-change).

**Why SCD1 over SCD2 for agents:**
- Commission rate and agency assignment changes aren't analytically meaningful for claims analysis (no KPI depends on agent-attribute-at-time-of-claim)
- SCD2 would add operational complexity (effective_date / expiration_date / is_current / MERGE) without analytical payoff
- Upgradable to SCD2 in future without breaking changes if agent productivity tracking becomes a requirement

**Demonstrated and documented:** modeling history (SCD type) and storage history (Delta versions) are decoupled. Simulated a commission rate change → overwrote dim_agent → row count stayed at 75 (no analytical history row added) → used `VERSION AS OF 1` to recover the pre-change state from the Delta transaction log. **The SCD type controls what analysts see; Delta time travel controls what engineers can recover for audit/debugging.**

This distinction is a Delta Lake capability that traditional dimensional modeling doesn't have. Builds the conceptual ground for tomorrow's SCD2 implementation where the row-count semantics will be the opposite (changes ADD rows rather than overwriting).


## 2026-06-30 — Surrogate key strategy: switched from monotonically_increasing_id to xxhash64

**Bug found in initial dim_policy build.** The two-step MERGE INSERT used `MONOTONICALLY_INCREASING_ID()` as the surrogate key. This function is only unique *within a single query execution*, not across multiple executions. When the simulated policy change ran, the INSERT started ID assignment at the same low integers as the initial load — causing v1 and v2 of the same policy to share a `policy_key`. This would have broken point-in-time joins in fact_claims.

**Fix:** switched surrogate key to `xxhash64(policy_number, policy_version)`. Advantages:
- Deterministic (same natural key + version → same surrogate every time)
- Cross-run stable (rebuilds produce identical keys)
- Unique per version by construction
- Distinct from initial-load range

**Trade-off:** hashed surrogates are less human-friendly than sequential IDs. In production, Delta Lake IDENTITY columns are the better solution but require table redefinition. For portfolio scope, hash-based surrogate is defensible and demonstrates awareness of the pitfall.


## 2026-07-04 — Surrogate key discipline: always assert uniqueness

Two distinct surrogate key bugs caught in Week 2 dim builds:

** dim_policy:** `monotonically_increasing_id()` reused ID ranges across separate MERGE executions. Fixed with `xxhash64(natural_key, version)`.

** dim_geography:** `xxhash64` didn't include all columns that varied in the DataFrame. Rows differing on non-hashed attributes collided. Fixed by expanding hash to include every identifying column.

Both bugs would have caused silent data corruption in fact table joins. Both were caught by the same assertion pattern:

```python
assert n_total == n_distinct_keys, "Surrogate key collision"
```

**Rule adopted:** every gold dim build ends with a uniqueness assertion. Cheap to write, catches the bug class both mechanisms produce. Now a permanent template.


## 2026-07-06 — fact_claims v1: point-in-time joins on SCD2 dims

**Decision:** For SCD2 dims (dim_policy, dim_customer), fact_claims FKs resolve to the version of the entity in effect on `dateOfLoss` — not the current version.

**Point-in-time filter:** dim.effective_date <= claim.dateOfLoss < COALESCE(dim.expiration_date, '9999-12-31')

**Why:** loss ratio analytics require premium-earned at time of loss. If FKs always pointed to the current version, historical loss ratios would float every time a policy renewed. Point-in-time joins tie each historical claim to the specific policy state that was priced against it.


## 2026-07-07 — Dashboard v2 and data model v2 

### Dashboard refresh
- 4 new charts enabled by v1 star schema:
  - Cycle time distribution histogram (with honest caption about synthesized dates)
  - Coastal vs Inland claim severity
  - CAT event impact by region (with log Y-axis to show all magnitude ranges)
  - Cause of damage rollup (using bronze.ref_cause_of_damage reference table)
- 4 existing Week 1 charts re-generated from v1 gold tables — validated match with prior numbers
- 8 aggregated CSVs committed under `dashboard/sample_data/` for reproducibility
- Streamlit KPI strip expanded to show "Dimensions: 6" — visible recruiter signal

### Cycle time chart  disclosure
Initial state-by-state chart showed near-uniform means (~199 days) across all states — an artifact of uniform random offset synthesis. Replaced with a distribution histogram plus caption disclosing synthesis. "The query works, but the chart tells the wrong story" was the lesson; distribution histogram + caption is the right framing.

### Data model v2
- ERD created in dbdiagram.io export at `docs/data_model_v2.png`
- Six dimensions + fact_claims with 10 FKs (6 dim, 4 role-playing date)
- SCD2 dims annotated in diagram title bar
- Role-playing dim_date shown with "x4 role-playing" annotation
- v1 ERD (`data_model_v1.png`) retained for history
- data_model.md updated with both versions and rationale for deferring dim_property to future work

## 2026-07-09 — dbt migration approach: sources over refactor

**Context:** Gold-layer transformations from PySpark to dbt. Decision on how to handle bronze and silver.

**Decision:** Bronze and silver stay in PySpark. dbt reads them as `sources`, not migrated as models.

**Why:**
- Bronze ingestion is inherently procedural (CSV parsing, schema declaration, audit columns) — awkward as SQL
- Silver cleaning is 11+ specific rules with null-handling logic — cleaner as PySpark
- dbt's strength is dimensional modeling and transformation, which is gold-layer work
- Attempting to do everything in dbt would inflate scope without much analytical payoff

### dbt profile: separate schema (gold_dbt) alongside existing gold

**Decision:** dbt materializes to `workspace.gold_dbt.*`, not `workspace.gold.*`.

**Why:**
- Prevents accidental overwrites of PySpark-managed gold tables during dbt development
- Allows side-by-side comparison of dbt output vs PySpark output for validation
- Once dbt output is byte-identical to PySpark, dashboard and other consumers can migrate to gold_dbt.*
- Clean rollback: if dbt work regresses, gold tables remain untouched


## 2026-07-11 — dbt staging tests: relationships checks introduced

Added `relationships` tests to `stg_policies` for `customer_id → stg_customers` and `agent_id → stg_agents`. These are dbt's declarative FK integrity checks — run as SQL under the hood ("SELECT * FROM stg_policies WHERE customer_id NOT IN (SELECT customer_id FROM stg_customers)"), fails if any row returned.

This is the same class of check I did informally with row count assertions in Week 1's silver enrichment (Day 11). Now it's formalized as a test that runs on every `dbt test` invocation. If the Day 9 generator drift bug ever recurred, this would catch it immediately.

Pattern to be extended in mart layer: every FK on fact_claims will have a `relationships` test to its target dim.


## 2026-07-11 — Staging layer pattern: view materialization + snake_case renaming

All 4 staging models materialize as views (not tables) — staging is a thin renaming/aliasing layer, not a heavy transformation. Views cost nothing to rebuild and stay current with source data.

**Not tracked:** Full column descriptions in `schema.yml` for every column of every model. Documented only interesting columns — natural keys, FKs, domain-specific attributes. Over-documenting adds maintenance burden without value.

