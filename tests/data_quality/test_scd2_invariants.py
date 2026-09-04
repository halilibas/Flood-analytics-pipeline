"""
Unit tests for SCD Type 2 invariants.

"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def test_exactly_one_current_per_natural_key(sample_dim_policy):
    """
    SCD2 invariant: every policy_number has exactly ONE row with is_current=True.
    """
    current_counts = (
        sample_dim_policy
        .groupBy("policy_number")
        .agg(
            F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("n_current")
        )
    )

    violations = current_counts.filter(F.col("n_current") != 1).collect()
    violation_details = [(row.policy_number, row.n_current) for row in violations]

    assert len(violations) >= 1, (
        "SCD2 invariant test failed to catch known-bad case P003"
    )
    assert ("P003", 0) in violation_details, (
        f"Expected P003 to have 0 current rows, got {violation_details}"
    )


def test_no_overlapping_intervals_for_natural_key(sample_dim_policy):
    """
    SCD2 invariant: for each natural key, versions must not have overlapping
    [effective_date, expiration_date) windows.
    Uses LAG to compare effective_date against previous version's expiration_date.
    """
    w = Window.partitionBy("policy_number").orderBy("policy_version")

    with_prev = sample_dim_policy.withColumn(
        "prev_expiration",
        F.lag("expiration_date").over(w)
    )

    overlaps = with_prev.filter(
        (F.col("prev_expiration").isNotNull()) &
        (F.col("effective_date") < F.col("prev_expiration"))
    ).collect()

    overlap_keys = [(row.policy_number, row.policy_version) for row in overlaps]

   
    assert len(overlaps) >= 1, (
        "SCD2 interval overlap invariant failed to catch known-bad case P004"
    )
    assert ("P004", 2) in overlap_keys, (
        f"Expected P004 v2 to have overlap, got {overlap_keys}"
    )


def test_current_row_has_null_expiration(sample_dim_policy):
    """
    Catches SCD2 bugs where close-old accidentally sets expiration on the current row.
    """
    violations = sample_dim_policy.filter(
        (F.col("is_current") == True) & F.col("expiration_date").isNotNull()
    ).count()

    assert violations == 0, (
        f"Found {violations} rows where is_current=True but expiration_date is not NULL"
    )