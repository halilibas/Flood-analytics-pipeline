"""
Shared pytest fixtures for data-quality unit tests.

Uses small in-memory PySpark DataFrames as fixtures — tests exercise
transformation invariants without needing a live Databricks cluster.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, BooleanType, LongType
)
from datetime import date


@pytest.fixture(scope="session")
def spark():
    """Session-scoped Spark for all tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("data_quality_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_dim_policy(spark):
    """
    Small SCD Type 2 dim_policy fixture with intentional test scenarios:
    - policy P001: 2 versions (v1 closed, v2 current) — valid SCD2
    - policy P002: 1 version (v1 current) — valid SCD2
    - policy P003: 2 versions but no current row — INVALID (violation case)
    - policy P004: 2 versions with overlapping intervals — INVALID (violation case)
    """
    schema = StructType([
        StructField("policy_key", LongType(), False),
        StructField("policy_number", StringType(), False),
        StructField("policy_version", IntegerType(), False),
        StructField("effective_date", DateType(), False),
        StructField("expiration_date", DateType(), True),
        StructField("is_current", BooleanType(), False),
    ])
    data = [
        # P001: valid SCD2 (v1 closed, v2 current)
        (1001, "P001", 1, date(2020, 1, 1), date(2022, 6, 30), False),
        (1002, "P001", 2, date(2022, 7, 1), None, True),
        # P002: single current version
        (1003, "P002", 1, date(2021, 1, 1), None, True),
        # P003: no current row (violation)
        (1004, "P003", 1, date(2020, 1, 1), date(2022, 12, 31), False),
        (1005, "P003", 2, date(2023, 1, 1), date(2023, 6, 30), False),
        # P004: overlapping intervals (violation)
        (1006, "P004", 1, date(2020, 1, 1), date(2022, 12, 31), False),
        (1007, "P004", 2, date(2022, 6, 1), None, True),  # starts before v1 ends
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_fact_claims(spark):
    """
    Small fact_claims fixture for FK integrity tests.
    """
    schema = StructType([
        StructField("claim_key", LongType(), False),
        StructField("fema_claim_id", StringType(), False),
        StructField("policy_key", LongType(), True),
        StructField("customer_key", LongType(), True),
        StructField("total_claim_amount", LongType(), False),
    ])
    data = [
        (5001, "fema-001", 1001, 2001, 10000),
        (5002, "fema-002", 1003, 2002, 25000),
        (5003, "fema-003", None, 2003, 5000),   # NULL policy_key (violation case)
        (5004, "fema-004", 1002, None, 15000),  # NULL customer_key (violation case)
    ]
    return spark.createDataFrame(data, schema)