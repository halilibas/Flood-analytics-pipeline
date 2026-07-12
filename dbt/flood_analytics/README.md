# dbt project — flood_analytics

This project migrates gold-layer transformations from PySpark notebooks to dbt models.

## Setup

Prerequisites:
- Python 3.11+ with `dbt-databricks` installed (see `../requirements.txt`)
- A Databricks Personal Access Token (PAT) for the target workspace

Configure your `~/.dbt/profiles.yml`:

```yaml
flood_analytics:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-hostname>
      http_path: <cluster-http-path>
      token: <PAT>
      schema: gold_dbt
      catalog: workspace
      threads: 4
```

## Commands

```bash
cd dbt/insurance_claims

dbt debug           # verify connection
dbt run             # run all models
dbt run --select stg_claims
dbt test            # run all tests
dbt test --select stg_claims
dbt docs generate   # generate documentation
dbt docs serve      # browse documentation locally
```

## Project structure

- `models/staging/` — thin renaming views over silver
- `models/intermediate/` — reusable business logic
- `models/marts/` — final gold-layer tables (dims and facts)
- `models/sources.yml` — bronze + silver source declarations
- `models/staging/staging_schema.yml` — column definitions and tests