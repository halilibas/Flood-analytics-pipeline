import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

# Local sample path. Full file lives outside the repo at ~/data/raw/
SAMPLE_PATH = "~/data/raw/FimaNfipClaims.csv"
SAMPLE_ROWS = 50_000

df = pd.read_csv(SAMPLE_PATH, nrows=SAMPLE_ROWS, low_memory=False)
print(f"Sample shape: {df.shape}")
print(f"Total columns: {len(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema overview

# COMMAND ----------

# All column names
for c in df.columns:
    print(c)

# COMMAND ----------

# Dtype distribution
df.dtypes.value_counts()

# COMMAND ----------

# First 5 rows
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Per-column inspection helper

# COMMAND ----------

def inspect(col):
    """Quick profile of a single column."""
    print(f"=== {col} ===")
    print(f"dtype: {df[col].dtype}")
    print(f"nulls: {df[col].isnull().sum():,} / {len(df):,}")
    print(f"sample values: {df[col].dropna().head(5).tolist()}")
    if df[col].dtype == 'object':
        print(f"unique: {df[col].nunique():,}")
    else:
        print(f"min: {df[col].min()}, max: {df[col].max()}, mean: {df[col].mean():.2f}")
    print()


# COMMAND ----------

sentinel_dates = df[df['originalConstructionDate'].str.startswith('1492', na=False)]
print(f"1492-10-12 sentinel rows: {len(sentinel_dates):,} / {len(df):,} "
      f"({len(sentinel_dates) / len(df):.1%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment column sparsity


# COMMAND ----------

for c in ['amountPaidOnBuildingClaim',
          'amountPaidOnContentsClaim',
          'amountPaidOnIncreasedCostOfComplianceClaim']:
    n_null = df[c].isna().sum()
    n_zero = (df[c] == 0).sum()
    n_pos  = (df[c] > 0).sum()
    print(f"{c}: {n_null:,} null, {n_zero:,} zero, {n_pos:,} positive")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top flood events


# COMMAND ----------

df['floodEvent'].value_counts().head(20)

# COMMAND ----------

print(f"Unique floodEvent values: {df['floodEvent'].nunique()}")
print(f"floodEvent null/blank rows: {df['floodEvent'].isna().sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean indicator encoding
# MAGIC Confirm 0/1 integer encoding before deciding to cast to BOOLEAN in silver.

# COMMAND ----------

bool_cols = [
    'elevatedBuildingIndicator',
    'primaryResidenceIndicator',
    'floodproofedIndicator',
    'nonProfitIndicator',
    'agricultureStructureIndicator',
    'rentalPropertyIndicator',
    'smallBusinessIndicatorBuilding',
    'stateOwnedIndicator',
]
for c in bool_cols:
    print(f"{c}: {df[c].value_counts().to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Degenerate dimension check
# MAGIC Verify `id` is unique and fully populated.

# COMMAND ----------

print(f"id unique: {df['id'].is_unique}")
print(f"id populated: {df['id'].notna().sum() / len(df):.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Geographic concentration

# COMMAND ----------

df['state'].value_counts().head(15)

# COMMAND ----------

top5 = df['state'].value_counts().head(5)
print(f"Top 5 states = {top5.sum() / len(df):.1%} of sample")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date range

# COMMAND ----------

print(f"dateOfLoss range: {df['dateOfLoss'].min()} to {df['dateOfLoss'].max()}")
print(f"originalNBDate range: {df['originalNBDate'].min()} to {df['originalNBDate'].max()}")

