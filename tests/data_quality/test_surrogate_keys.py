"""
Unit tests for surrogate key uniqueness invariants.

"""

from pyspark.sql import functions as F


def test_policy_key_is_unique(sample_dim_policy):
    """Surrogate key policy_key must be globally unique across all versions."""
    total = sample_dim_policy.count()
    distinct = sample_dim_policy.select("policy_key").distinct().count()
    assert total == distinct, (
        f"Surrogate key collision: {total} rows but {distinct} distinct keys"
    )


def test_natural_key_version_combination_is_unique(sample_dim_policy):
    """
    Natural key + version combination must be unique.
    Prevents duplicate SCD2 versions for the same policy.
    """
    total = sample_dim_policy.count()
    distinct = (
        sample_dim_policy
        .select("policy_number", "policy_version")
        .distinct()
        .count()
    )
    assert total == distinct, (
        f"Duplicate (policy_number, policy_version) pair: "
        f"{total} rows but {distinct} distinct combinations"
    )


def test_no_null_surrogate_keys(sample_dim_policy):
    """Surrogate keys must never be NULL and required for FK joins."""
    null_count = (
        sample_dim_policy
        .filter(F.col("policy_key").isNull())
        .count()
    )
    assert null_count == 0, f"Found {null_count} NULL policy_key values"