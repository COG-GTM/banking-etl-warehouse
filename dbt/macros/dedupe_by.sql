{#
    Deterministic deduplication helper.

    Keeps exactly one row per `partition_by` key, choosing the winner with
    `row_number() over (partition by <partition_by> order by <order_by>)`.

    Args:
      relation_or_cte: name of a CTE (or a relation) to read from
      partition_by:    string or list of columns forming the dedup key
      order_by:        string or list of ordering expressions deciding the winner
#}
{% macro dedupe_by(relation_or_cte, partition_by, order_by) %}
    {%- set partition_cols = [partition_by] if partition_by is string else partition_by -%}
    {%- set order_exprs = [order_by] if order_by is string else order_by -%}
    select *
    from (
        select
            *,
            row_number() over (
                partition by {{ partition_cols | join(', ') }}
                order by {{ order_exprs | join(', ') }}
            ) as _dedupe_row_number
        from {{ relation_or_cte }}
    )
    where _dedupe_row_number = 1
{% endmacro %}
