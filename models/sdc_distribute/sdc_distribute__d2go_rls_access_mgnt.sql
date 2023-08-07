{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema()
    )
}}

{%- if var('sdc_distribute__external_rls_access_mgnt',none) is not none -%}
select
    user_or_role,
    view_name
    {{- sdc_distribute.get_rls_columns_expression(',
    #COLUMN_NAME#') }}
from
    {{ var('sdc_distribute__external_rls_access_mgnt') }}
{%- else -%}
select
    rls.grantee user_or_role,
    '"'||'{{ sdc_distribute.get_target_instance()|upper ~ '_' }}'||rls.dist_database_name||'"."'||'{{ sdc_distribute.get_target_project()|upper }}'||'"."{{ sdc_distribute.get_target_schema()|upper }}'||rls.dist_object_name||'"' view_name
    {{- sdc_distribute.get_rls_columns_expression(',
    rls.#COLUMN_NAME#') }}
from
    {{ ref('sdc_distribute__object_access_rls') }} rls
    left outer join
    {{ ref('sdc_distribute__object') }} obj on
        obj.dist_database_name = rls.dist_database_name and
        obj.dist_object_name = rls.dist_object_name    
where
    rls.dist_database_name = 'DISTRIBUTE' and
    rls.enabled = 'Y' and
    obj.enabled = 'Y'
{%- endif -%}
