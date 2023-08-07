{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema(),
        materialized = 'incremental',        
        full_refresh = false,
        transient = false,
        on_schema_change = 'append_new_columns',
        post_hook = ["
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access$set
(   
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_grantee variant,
    p_enabled varchar,
    p_defer_set_framework_objects varchar,
    p_force_update varchar
)
returns varchar
language sql
as
$$
declare
    l_row_count number;
    l_return varchar;

    l_invalid_grantee variant;

    l_current_ts timestamp_ntz;

    {{ sdc_distribute.tags_declare() }}
begin        
    if (:p_dist_object_name is null) then
        return 'The Distribution Object Name is mandatory';
    end if;
    
    if (:p_grantee is null) then
        return 'The Grantee is mandatory';
    end if;

    if (:p_dist_database_name = 'DISTRIBUTE') then

        select array_agg(value) into l_invalid_grantee
        from (select to_array(:p_grantee) arr), lateral flatten(arr)
        where
            regexp_replace(value,'_DISTRIBUTE$')
            not in (select upper(project_name) from prd_distribute.snowflake_ops.project_info_public)
        ;

        if (:l_invalid_grantee != []) then
            return 'Invalid Grantee list: '||:l_invalid_grantee;
        end if;
        
    end if;

    {{ sdc_distribute.tags_body() }}

    {{ sdc_distribute.set_sdc_distribute__object_sel() }}

    select convert_timezone('Europe/Berlin',current_timestamp)::timestamp_ntz into l_current_ts;

    merge into " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access trg
    using
    (
        select  *
        from (
        select
            dist_database_name,
            dist_object_name,
            P_GRANTEE grantee,
            decode(upper(:p_enabled),'Y','Y',null,null,'N') enabled,
            parse_json('{'||
                '\"user\":\"'||current_user||'\",'||
                '\"cet_timestamp\":\"'||:l_current_ts||'\"'||
            '}') audit_info
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel
            cross join (select distinct value P_GRANTEE from (select to_array(:p_grantee) arr), lateral flatten(arr))
        )
        qualify
            count(*) over (partition by dist_database_name,dist_object_name,grantee) = 1
    ) src
    on
    ( 
        src.dist_database_name = trg.dist_database_name and
        src.dist_object_name = trg.dist_object_name and
        src.grantee = trg.grantee 
    )
    when matched and (decode(src.enabled,trg.enabled,1,0) = 0 or :p_force_update = 'Y') then update
    set
        aud_updated_info = src.audit_info,
        enabled = nvl(src.enabled,trg.enabled)
    when not matched then
    insert
    (
        dist_database_name,
        dist_object_name,
        grantee,
        enabled,
        aud_created_info,
        aud_updated_info
    )
    values
    (
        src.dist_database_name,
        src.dist_object_name,
        src.grantee,
        nvl(src.enabled,'Y'),
        src.audit_info,
        src.audit_info
    );
    
    l_row_count := sqlrowcount;

    if (:p_dist_database_name = 'DISTRIBUTE' and l_row_count > 0) then
        if (:p_defer_set_framework_objects = 'Y') then
            create or replace view " ~ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower ~ ".call_prc_distribute_grant_access as select true value;
        else
            call sdc_distribute__object_access$set_framework_objects
            (   
                p_updated_since => :l_current_ts
            );
        end if;
    end if;

    if (l_row_count = 0) then
        l_return := 'No rows were processed';
    elseif (l_row_count = 1) then
        l_return := 'Processed 1 row';
    else
        l_return := 'Processed '||l_row_count||' rows';
    end if;
    
    return l_return;
end;
$$        
        ", "
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access$set
(   
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_grantee variant,
    p_enabled varchar
)
returns varchar
language sql
as
$$
declare 
    l_return varchar;
begin        
    call sdc_distribute__object_access$set
    (
        p_dist_database_name => :p_dist_database_name,
        p_dist_object_name => :p_dist_object_name,
        p_src_schema => :p_src_schema,
        p_src_object_name => :p_src_object_name,
        p_grantee => :p_grantee,
        p_enabled => :p_enabled,
        p_defer_set_framework_objects => 'N',
        p_force_update => 'N'
    );

    select * into l_return
    from table ( result_scan ( last_query_id() ) );

    return l_return;
end;
$$        
        ", "
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access$remove
( 
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_grantee variant
)
returns varchar
language sql
as
$$
declare
    l_row_count number;
    l_return varchar;

    l_object_name varchar;
    l_project_name varchar;

    l_count_distribute number default 0;

    {{ sdc_distribute.tags_declare() }}    
begin    
    if (:p_dist_object_name is null) then
        return 'The Distribution Object Name is mandatory';
    end if;
    
    if (:p_grantee is null) then
        return 'The Grantee is mandatory';
    end if;

    {{ sdc_distribute.tags_body() }}

    {{ sdc_distribute.set_sdc_distribute__object_sel() }}

    let cur_object_access cursor for
    select
        dist_database_name,
        dist_object_name,
        regexp_replace(grantee,'_DISTRIBUTE$') project_name
    from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access
    where
        grantee like any (select value from (select to_array(parse_json(?)) arr), lateral flatten(arr)) escape '\\"~"\\"~"' and
        (dist_database_name, dist_object_name) in
        (
            select
                dist_database_name,
                dist_object_name
            from
                " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel
        );

    open cur_object_access using (
        p_grantee::varchar
    );

    for rec in cur_object_access do
        
        l_object_name := '\"'||substr(current_database(),1,4)||rec.dist_database_name||'\".\"'||substr(current_database(),5)||'\".\"" ~ 
                         sdc_distribute.get_target_schema() | upper ~ "'||rec.dist_object_name ||'\"';
        l_project_name := rec.project_name;
        
        if (rec.dist_database_name = 'DISTRIBUTE') then
            l_count_distribute := l_count_distribute + 1;
            begin
                call common.distribute.prc_access_on_distribute_object_" ~ sdc_distribute.get_target_database() | lower ~ "                
                (
                    object_name => :l_object_name,
                    object_type => 'VIEW',
                    action => 'REVOKE',
                    project_name => :l_project_name,
                    active => 'TRUE'
                );
            exception
                when other then
                    null;
            end;
        end if;
    end for;

    delete from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access_rls
    where
        grantee like any (select value from (select to_array(:p_grantee) arr), lateral flatten(arr)) escape '\\"~"\\"~"' and
        (dist_database_name, dist_object_name) in
        (
            select
                dist_database_name,
                dist_object_name
            from
                " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel
        )
    ;

    delete from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access
    where
        grantee like any (select value from (select to_array(:p_grantee) arr), lateral flatten(arr)) escape '\\"~"\\"~"' and
        (dist_database_name, dist_object_name) in
        (
            select
                dist_database_name,
                dist_object_name
            from
                " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel
        )
    ;

    l_row_count := sqlrowcount;

    if (l_row_count = 0) then
        l_return := 'No rows were processed';
    elseif (l_row_count = 1) then
        l_return := 'Processed 1 row';
    else
        l_return := 'Processed '||l_row_count||' rows';
    end if;
    
    return l_return;
end;
$$        
        ", "
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access$set_framework_objects
(   
    p_updated_since timestamp_ntz
)
returns varchar
language sql
as
$$
declare
    l_row_count number default 0;
    l_return varchar;

    l_object_name varchar;
    l_project_name varchar;
    l_active varchar;

begin        
    if (:p_updated_since is null) then
        return 'The Updated Since is mandatory';
    end if;
    
    let cur_object_access cursor for
    select
        dist_database_name,
        dist_object_name,
        regexp_replace(grantee,'_DISTRIBUTE$') project_name,
        case when enabled = 'Y' then 'TRUE' else 'FALSE' end active
    from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access
    where
        aud_updated_info:user = current_user and
        aud_updated_info:cet_timestamp >= ? and
        dist_database_name = 'DISTRIBUTE'
    ;

    open cur_object_access using (
        p_updated_since::varchar
    );

    for rec in cur_object_access do
        l_object_name := '\"'||substr(current_database(),1,4)||rec.dist_database_name||'\".\"'||substr(current_database(),5)||'\".\"" ~ 
                        sdc_distribute.get_target_schema() | upper ~ "'||rec.dist_object_name ||'\"';
        l_project_name := rec.project_name;
        l_active := rec.active;
        
        if (rec.dist_database_name = 'DISTRIBUTE') then
            begin
                call common.distribute.prc_access_on_distribute_object_" ~ sdc_distribute.get_target_database() | lower ~ "
                (
                    object_name => :l_object_name,
                    object_type => 'VIEW',
                    action => 'GRANT',
                    project_name => :l_project_name,
                    active => :l_active
                );
                l_row_count := l_row_count + 1;
            exception
                when other then null;
            end;
        end if;
    end for;

    if (l_row_count = 0) then
        l_return := 'No rows were processed';
    elseif (l_row_count = 1) then
        l_return := 'Processed 1 row';
    else
        l_return := 'Processed '||l_row_count||' rows';
    end if;
    
    return l_return;
end;
$$        
        ", "
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access$sync
( 
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_grantee variant
)
returns varchar
language sql
as
$$
declare
    l_row_count number default 0;
    l_return varchar;

    l_source_object_name varchar;
    l_object_name varchar;
    l_project_name varchar;
    l_active varchar;

    {{ sdc_distribute.tags_declare() }}    
begin    
    if (:p_dist_object_name is null) then
        return 'The Distribution Object Name is mandatory';
    end if;
    
    if (:p_grantee is null) then
        return 'The Grantee is mandatory';
    end if;

    {{ sdc_distribute.tags_body() }}

    {{ sdc_distribute.set_sdc_distribute__object_sel() }}

    let cur_sync_status cursor for
    select
        *
    from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access_sync_status
    where
        grantee_name like any (select regexp_replace(value,'_DISTRIBUTE$') from (select to_array(parse_json(?)) arr), lateral flatten(arr)) escape '\\"~"\\"~"' and
        (object_name like any (select value from (select to_array(parse_json(?)) arr), lateral flatten(arr)) escape '\\"~"\\"~"' or
        object_name in
        (
            select
                '\"'||substr(current_database(),1,4)||dist_database_name||'\".\"'||substr(current_database(),5)||'\".\"" ~ 
                         sdc_distribute.get_target_schema() | upper ~ "'||dist_object_name ||'\"'
            from
                " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel
        )) and
        status in (
            'INVALID_RBAC_OBJECT',
            'INVALID_RBAC_GRANTEE',
            'MISSING_RBAC',
            'MISSING_SDC_DIST',
            'DIFF_ACTIVE_FLAG'
        )
    ;

    open cur_sync_status using (
        p_grantee::varchar,
        p_dist_object_name::varchar
    );

    for rec in cur_sync_status do
        l_source_object_name := '\"'||upper(current_database())||'\".\"'
            || '" ~ (sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema()) | upper ~ "\".\"'
            || regexp_replace(rec.object_name,'(.*\".\"){2}(.*)\"','\\2')
            || '__rbac_cleanup\"';
        l_object_name := rec.object_name;
        l_project_name := rec.grantee_name;
        l_active := case when rec.sdc_dist_active then 'TRUE' else 'FALSE' end;

        if (rec.status in ('INVALID_RBAC_OBJECT')) then
            begin
                execute immediate 'create view '||:l_source_object_name||' as select null col';

                call common.distribute.prc_distribute_mapping_table_" ~ sdc_distribute.get_target_database() | lower ~ "
                (
                    project_source_table_or_view => :l_source_object_name,
                    distribute_database_target_view => :l_object_name,
                    anonymized_columns => '',
                    rls_columns => '%',
                    rls_access_mgnt_table => '',
                    action => 'C'
                );

                call common.distribute.prc_distribute_view_processing_" ~ sdc_distribute.get_target_database() | lower ~ "();
            exception
                when other then null;
            end;
        end if;

        if (rec.status in ('INVALID_RBAC_OBJECT','INVALID_RBAC_GRANTEE','MISSING_SDC_DIST')) then
            begin
                call common.distribute.prc_access_on_distribute_object_" ~ sdc_distribute.get_target_database() | lower ~ "
                (
                    object_name => :l_object_name,
                    object_type => 'VIEW',
                    action => 'REVOKE',
                    project_name => :l_project_name,
                    active => 'TRUE'
                );

                l_row_count := l_row_count + 1;
            exception
                when other then null;
            end;
        elseif (rec.status in ('MISSING_RBAC','DIFF_ACTIVE_FLAG')) then
            begin
                call common.distribute.prc_access_on_distribute_object_" ~ sdc_distribute.get_target_database() | lower ~ "
                (
                    object_name => :l_object_name,
                    object_type => 'VIEW',
                    action => 'GRANT',
                    project_name => :l_project_name,
                    active => :l_active
                );

                l_row_count := l_row_count + 1;
            exception
                when other then null;
            end;
        end if;

        if (rec.status in ('INVALID_RBAC_OBJECT')) then
            begin
                call common.distribute.prc_distribute_mapping_table_" ~ sdc_distribute.get_target_database() | lower ~ "
                (
                    project_source_table_or_view => '',
                    distribute_database_target_view => :l_object_name,
                    anonymized_columns => '',
                    rls_columns => '',
                    rls_access_mgnt_table => '',
                    action => 'D'
                );

                call common.distribute.prc_distribute_view_processing_" ~ sdc_distribute.get_target_database() | lower ~ "();

                execute immediate 'drop view '||:l_source_object_name;
            exception
                when other then null;
            end;
        end if;

    end for;

    if (l_row_count = 0) then
        l_return := 'No rows were processed';
    elseif (l_row_count = 1) then
        l_return := 'Processed 1 row';
    else
        l_return := 'Processed '||l_row_count||' rows';
    end if;
    
    return l_return;
end;
$$
        "]
    )
}}

select
    null::varchar dist_database_name,
    null::varchar dist_object_name,
    null::varchar grantee,
    null::varchar(1) enabled,
    null::variant aud_created_info,
    null::variant aud_updated_info
where
    false
