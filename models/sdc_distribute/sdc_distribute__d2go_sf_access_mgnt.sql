{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema()
    )
}}

{%- if var('sdc_distribute__external_sf_access_mgnt',none) is not none -%}
select
    account,
    view_name,
    active
from
    {{ var('sdc_distribute__external_sf_access_mgnt') }}
{%- else -%}
select
    axx.grantee account,
    '"'||'{{ sdc_distribute.get_target_instance()|upper ~ '_' }}'||axx.dist_database_name||'"."'||'{{ sdc_distribute.get_target_project()|upper }}'||'"."{{ sdc_distribute.get_target_schema()|upper }}'||axx.dist_object_name||'"' view_name,
    'X' active
from
    {{ ref('sdc_distribute__object_access') }} axx
    left outer join
    {{ ref('sdc_distribute__object') }} obj on
        obj.dist_database_name = axx.dist_database_name and
        obj.dist_object_name = axx.dist_object_name
where
    axx.dist_database_name = 'DISTRIBUTE_SF' and
    axx.enabled = 'Y' and
    obj.enabled = 'Y'
{%- endif -%}
