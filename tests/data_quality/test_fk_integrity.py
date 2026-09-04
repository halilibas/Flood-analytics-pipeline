"""
Unit tests for foreign key integrity between fact_claims and dimensions.


"""

from pyspark.sql import functions as F


def test_all_policy_keys_resolve_or_are_null(sample_fact_claims, sample_dim_policy):
    """
    Every non-NULL policy_key in fact_claims must exist in dim_policy.
    NULL policy_keys are allowed (documented as edge case) but must be tracked.
    """
    fact_keys = (
        sample_fact_claims
        .filter(F.col("policy_key").isNotNull())
        .select("policy_key")
        .distinct()
    )
    dim_keys = sample_dim_policy.select("policy_key").distinct()

    # LEFT JOIN — any fact_key not in dim_keys is an orphan
    orphans = (
        fact_keys.join(dim_keys, "policy_key", "left_anti")
        .count()
    )
    assert orphans == 0, f"Found {orphans} fact policy_key values with no matching dim row"


def test_null_fk_rate_below_threshold(sample_fact_claims):
    total = sample_fact_claims.count()
    null_policy = sample_fact_claims.filter(F.col("policy_key").isNull()).count()
    null_customer = sample_fact_claims.filter(F.col("customer_key").isNull()).count()

    null_policy_rate = null_policy / total
    null_customer_rate = null_customer / total

    threshold = 0.25 
    assert null_policy_rate <= threshold, (
        f"NULL policy_key rate {null_policy_rate:.2%} exceeds {threshold:.0%}"
    )
    assert null_customer_rate <= threshold, (
        f"NULL customer_key rate {null_customer_rate:.2%} exceeds {threshold:.0%}"
    )