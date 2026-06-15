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