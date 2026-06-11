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