{% macro fix_rbac_grants() -%}

    {%- call statement('exec', fetch_result=False) -%}

create or replace procedure {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute$fix_rbac_grants()
returns varchar
language sql
execute as caller
as
$$
begin

    let cur_object cursor for
    select	distinct object_name
    from	prd_distribute.snowflake_ops.v_rbac_on_project_objects
    where	object_name like '"{{ sdc_distribute.get_target_instance() | upper }}%'
    {{ 
        "and     object_name not like '\"%\".\"DBT%'"
        if sdc_distribute.get_target_schema() == '' else
        "and     object_name like '\"%\".\"" ~ sdc_distribute.get_target_schema() | upper ~ "%'"
    }}
    intersect
    select  '"'||table_catalog||'"."'||table_schema||'"."'||table_name||'"'
    from    {{ sdc_distribute.get_target_instance() | lower }}_distribute.information_schema.views
    where   table_schema = '{{ sdc_distribute.get_target_project() }}'
    order by 1;
    
    open cur_object;

    for rec_object in cur_object do

        let l_object_name varchar := rec_object.object_name;

        execute immediate 'show grants on '||l_object_name;

        let rs_grantee resultset := (
        select
            grantee_name
        from
            prd_distribute.snowflake_ops.v_rbac_on_project_objects
        where
            active = 'TRUE' and
            object_name = :l_object_name
        intersect
        select
            project_name
        from
            prd_distribute.snowflake_ops.project_info_public
        minus    
        select
            regexp_replace("grantee_name",'([^_]+)_(.+)_DISTRIBUTE_O_R(_RESTRICTED)?','\\2')
        from table ( result_scan ( last_query_id() ) )
        where "granted_to" = 'ROLE'
        order by 1);

        let cur_grantee cursor for rs_grantee;
    
        for rec_grantee in cur_grantee do
            
            let l_project_name varchar := rec_grantee.grantee_name;
        
            begin

                call common.distribute.prc_access_on_distribute_object_{{ sdc_distribute.get_target_database() | lower }}
                (
                    object_name => :l_object_name,
                    object_type => 'VIEW',
                    action => 'GRANT',
                    project_name => :l_project_name,
                    active => 'FALSE'
                );                    
                
            exception
                when other then null;
            end;
        
        end for;
            
        close cur_grantee;
            
        open cur_grantee;
    
        for rec_grantee in cur_grantee do
            
            let l_project_name varchar := rec_grantee.grantee_name;
        
            begin

                call common.distribute.prc_access_on_distribute_object_{{ sdc_distribute.get_target_database() | lower }}
                (
                    object_name => :l_object_name,
                    object_type => 'VIEW',
                    action => 'GRANT',
                    project_name => :l_project_name,
                    active => 'TRUE'
                );                    
                
            exception
                when other then null;
            end;
        
        end for;

        close cur_grantee;
            
    end for;

    close cur_object;
    
end;
$$
    {%- endcall -%}

    {%- call statement('exec', fetch_result=False) -%}
    call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute$fix_rbac_grants()
    {%- endcall -%}

    {%- call statement('exec', fetch_result=False) -%}
    drop procedure {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute$fix_rbac_grants()
    {%- endcall -%}

{%- endmacro %}

