{% test valid_transaction_date(model, column_name) %}
  select 1
  from {{ model }}
  where 
    try_cast({{ column_name }} as date) is null  -- Fails if date cannot be parsed 
    -- I'm running on duckdb, if it would be Snowflake we would be using try_to_date({{ column_name }}) instead
    and {{ column_name }} is not null       -- Ignore NULLs (covered by `not_null` test)
{% endtest %}
