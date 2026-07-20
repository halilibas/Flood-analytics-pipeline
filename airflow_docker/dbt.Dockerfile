FROM python:3.11-slim

# Install git for dbt package handling
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-databricks at your pinned version
RUN pip install --no-cache-dir dbt-databricks==1.12.2

WORKDIR /usr/app/dbt

CMD ["dbt", "--version"]