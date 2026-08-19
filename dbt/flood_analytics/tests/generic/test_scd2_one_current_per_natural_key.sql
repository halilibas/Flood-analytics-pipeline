{# 
  Custom generic test: verifies SCD2 invariant.
  
  For a given SCD2 dimension, asserts that each natural key
  has exactly one row where is_current = true
  
  Args:
    model: the dbt model or source (auto passed by dbt)
    natural_key_column: the natural key column name
  
  Usage in schema.yml:
    - name: dim_policy
      tests:
        - scd2_one_current_per_natural_key:
            natural_key_column: policy_number
  
  Passes when: every natural_key has exactly 1 current row
  Fails when: any natural_key has 0 current rows OR 2+ current rows
#}

{% test scd2_one_current_per_natural_key(model, natural_key_column) %}

with current_row_counts as (
    select
        {{ natural_key_column }} as natural_key,
        count(*) as n_current_rows
    from {{ model }}
    where is_current = true
    group by {{ natural_key_column }}
),

violations as (
    select *
    from current_row_counts
    where n_current_rows != 1
)

select * from violations

{% endtest %}