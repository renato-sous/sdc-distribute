{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema(),
        materialized = 'incremental',
        full_refresh = false,
        transient = false,
        on_schema_change = 'append_new_columns',
        post_hook = ["
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access_rls$set
(   
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_grantee variant,
    p_enabled varchar" ~ 
    sdc_distribute.get_rls_columns_expression(',
    p_#COLUMN_NAME# varchar') ~ "
)
returns varchar
language sql
as
$$
declare
    l_row_count number;
    l_return varchar;

    l_invalid_grantee variant;

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

    merge into " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access_rls trg
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
                '\"cet_timestamp\":\"'||convert_timezone('Europe/Berlin',current_timestamp)::timestamp_ntz||'\"'||
            '}') audit_info" ~ 
            sdc_distribute.get_rls_columns_expression(',
            :p_#COLUMN_NAME# #COLUMN_NAME#') ~ "
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel
            cross join (select distinct value P_GRANTEE from (select to_array(:p_grantee) arr), lateral flatten(arr))
        where
            rls_columns is not null
        )
        qualify
            count(*) over (partition by dist_database_name,dist_object_name,grantee" ~ sdc_distribute.get_rls_columns_expression(',#COLUMN_NAME#') ~ ") = 1
    ) src
    on ( 
        src.dist_database_name = trg.dist_database_name and
        src.dist_object_name = trg.dist_object_name and
        src.grantee = trg.grantee" ~ 
        sdc_distribute.get_rls_columns_expression(' and
        decode(src.#COLUMN_NAME#,trg.#COLUMN_NAME#,1)=1') ~ "
    )
    when matched then update
    set
        aud_updated_info = src.audit_info,
        enabled = nvl(src.enabled,trg.enabled)" ~ 
        sdc_distribute.get_rls_columns_expression(',
        #COLUMN_NAME# = src.#COLUMN_NAME#') ~ "
    when not matched then
    insert
    (
        dist_database_name,
        dist_object_name,
        grantee,
        enabled,
        aud_created_info,
        aud_updated_info" ~ 
        sdc_distribute.get_rls_columns_expression(',
        #COLUMN_NAME#') ~ "
    )
    values
    (
        src.dist_database_name,
        src.dist_object_name,
        src.grantee,
        nvl(src.enabled,'Y'),
        src.audit_info,
        src.audit_info" ~ 
        sdc_distribute.get_rls_columns_expression(',
        src.#COLUMN_NAME#') ~ "
    );
    
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
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access_rls$remove
( 
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_grantee variant" ~ 
    sdc_distribute.get_rls_columns_expression(',
    p_#COLUMN_NAME# varchar') ~ "
)
returns varchar
language sql
as
$$
declare
    l_row_count number;
    l_return varchar;

    {{ sdc_distribute.tags_declare() }}
begin    
    if (:p_dist_object_name is null) then
        return 'The Distribution Object Name is mandatory';
    end if;
    
    if (:p_grantee is null) then
        return 'The grantee is mandatory';
    end if;

    {{ sdc_distribute.tags_body() }}

    {{ sdc_distribute.set_sdc_distribute__object_sel() }}

    delete from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access_rls
    where
        grantee like any (select value from (select to_array(:p_grantee) arr), lateral flatten(arr)) escape '\\"~"\\"~"'" ~ 
        sdc_distribute.get_rls_columns_expression(' and
        (#COLUMN_NAME# like :p_#COLUMN_NAME# escape \'\\\\\' or :p_#COLUMN_NAME# is null)') ~ " and
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
    {{- sdc_distribute.get_rls_columns_expression(',
    null::varchar #COLUMN_NAME#') }}
where
    false
