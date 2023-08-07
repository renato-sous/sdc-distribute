{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema()
    )
}}

with
    rbac_access as (
        select object_name, grantee_name, active
        from prd_distribute.snowflake_ops.v_rbac_on_project_objects
        where
            privilege = 'SELECT'
            and granted_on = 'VIEW'
            and object_name like '"{{ sdc_distribute.get_target_instance() | upper }}%'
            and object_name like '"{{ sdc_distribute.get_target_instance() | upper }}\\_DISTRIBUTE"."{{ sdc_distribute.get_target_project() | upper }}"."{{ sdc_distribute.get_target_schema() | upper }}%"' escape '\\' and
            ('{{ sdc_distribute.get_target_schema() | upper }}' like 'DBT\\_Z%' escape '\\'
             or object_name not like '"{{ sdc_distribute.get_target_instance() | upper }}\\_DISTRIBUTE"."{{ sdc_distribute.get_target_project() | upper }}"."DBT\\_Z%"' escape '\\')
    ),
    sdc_dist as (
        select
            view_name object_name,
            regexp_replace(upper(user_or_role), '_DISTRIBUTE$') grantee_name,
            decode(active, 'X', true, false) active
        from
            {{ ref('sdc_distribute__d2go_access_mgnt') }}
    ),
    projects as (
        select upper(project_name) project_name from prd_distribute.snowflake_ops.project_info_public
    ),
    dist_objects as (
        select
            '"'||table_catalog||'"."'||table_schema||'"."'||table_name||'"' object_name
        from
            {{ sdc_distribute.get_target_instance() | lower }}_distribute.information_schema.tables
        where table_schema = '{{ sdc_distribute.get_target_project() | upper }}' and
        ('{{ sdc_distribute.get_target_schema() | upper }}' like 'DBT\\_Z%' escape '\\'
         or table_name not like 'DBT\\_Z%' escape '\\')
    )
select
    coalesce(rbac_access.object_name, sdc_dist.object_name) object_name,
    coalesce(rbac_access.grantee_name, sdc_dist.grantee_name) grantee_name,
    case
        when sdc_dist.object_name is not null and obj_sdc.object_name is null
        then 'INVALID_SDC_DIST_OBJECT'
        when rbac_access.object_name is not null and obj_rbac.object_name is null
        then 'INVALID_RBAC_OBJECT'
        when sdc_dist.object_name is not null and prj_sdc.project_name is null
        then 'INVALID_SDC_DIST_GRANTEE'
        when rbac_access.object_name is not null and prj_rbac.project_name is null
        then 'INVALID_RBAC_GRANTEE'
        when rbac_access.object_name is null
        then 'MISSING_RBAC'
        when sdc_dist.object_name is null
        then 'MISSING_SDC_DIST'
        when sdc_dist.active != rbac_access.active
        then 'DIFF_ACTIVE_FLAG'
        else 'OK'
    end status,
    rbac_access.active rbac_active,
    sdc_dist.active sdc_dist_active
from rbac_access
full join
    sdc_dist
    on rbac_access.object_name = sdc_dist.object_name
    and rbac_access.grantee_name = sdc_dist.grantee_name
left join projects prj_sdc on sdc_dist.grantee_name = prj_sdc.project_name
left join projects prj_rbac on rbac_access.grantee_name = prj_rbac.project_name
left join dist_objects obj_sdc on sdc_dist.object_name = obj_sdc.object_name
left join dist_objects obj_rbac on rbac_access.object_name = obj_rbac.object_name
order by 
    object_name,
    grantee_name    
