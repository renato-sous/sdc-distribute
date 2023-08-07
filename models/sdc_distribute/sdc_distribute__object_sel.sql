{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema(),
        materialized = 'view'
    )
}}

select
    *
from
    {{ ref('sdc_distribute__object') }}
where
    false
