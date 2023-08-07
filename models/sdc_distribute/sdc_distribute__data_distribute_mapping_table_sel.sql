{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema(),
        materialized = 'table'
    )
}}

select
    '{{ invocation_id }}' invocation_id,
    mapp.*
from
    prd_distribute.snowflake_ops.data_distribute_mapping_table mapp
where
    distribute_database_target_view like '%."{{ sdc_distribute.get_target_project()|upper }}".%' and
    false
