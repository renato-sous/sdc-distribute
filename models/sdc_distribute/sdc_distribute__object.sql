-- depends_on: {{ ref('sdc_distribute__data_distribute_mapping_table_sel') }}
{{
    config(
        schema = sdc_distribute.get_sdc_distribute_schema(),
        materialized = 'incremental',
        full_refresh = false,
        transient = false,
        on_schema_change = 'append_new_columns',
        post_hook = ["
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object$set
( 
    p_dist_database_name varchar,
    p_dist_object_name varchar,
    p_src_schema varchar,
    p_src_object_name variant,
    p_anonymized_columns varchar,
    p_rls_columns varchar,
    p_enabled varchar,
    p_rls_all_values variant,
    p_tags variant
)
returns varchar
language sql
as
$$
declare
    l_row_count number;
    l_return varchar;
begin    
    if (not regexp_like(:p_dist_database_name, 'DISTRIBUTE(_SF)?')) then
        return 'The Distribution Database must follow the rule DISTRIBUTE(_SF)';
    end if;
    
    if (:p_src_schema is null) then
        return 'The Source Schema is mandatory';
    end if;

    if (:p_src_object_name is null) then
        return 'The Source Object Name is mandatory';
    end if;

    merge into " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object trg
    using
    (
        select  *
        from (
        select
            :p_dist_database_name dist_database_name,
            decode(nvl(:p_dist_object_name,'%'),'%',table_name,replace(:p_dist_object_name,'#TABLE_NAME#',table_name)) dist_object_name,
            substr(table_schema,length('" ~ sdc_distribute.get_target_schema() | upper ~ "')+1) src_schema,
            table_name src_object_name,
            upper(:p_anonymized_columns) anonymized_columns,
            upper(:p_rls_columns) rls_columns,
            decode(upper(:p_enabled),'Y','Y',null,null,'N') enabled,
            :p_rls_all_values rls_all_values,
            :p_tags tags,
            parse_json('{'||
                '\"user\":\"'||current_user||'\",'||
                '\"cet_timestamp\":\"'||convert_timezone('Europe/Berlin',current_timestamp)::timestamp_ntz||'\"'||
            '}') audit_info
        from
            information_schema.tables sch
        where
            table_schema like '" ~ sdc_distribute.get_target_schema() | upper | replace('_','\\\\_') ~ "'||:p_src_schema escape '\\"~"\\"~"' and
            ('" ~ sdc_distribute.get_target_schema() | upper ~ "' like 'DBT\\"~"\\"~"_Z%' escape '\\"~"\\"~
             "' or table_schema not like 'DBT\\"~"\\"~"_Z%\\"~"\\"~"_%' escape '\\"~"\\"~"') and
            table_name like any (select value from (select to_array(:p_src_object_name) arr), lateral flatten(arr)) escape '\\"~"\\"~"'
        )
        qualify
            count(*) over (partition by dist_database_name,dist_object_name) = 1
    ) src
    on
    ( 
        src.dist_database_name = trg.dist_database_name and
        src.dist_object_name = trg.dist_object_name
    )
    when matched then update
    set
        aud_updated_info = src.audit_info,
        src_schema = nvl(src.src_schema,trg.src_schema),
        src_object_name = nvl(src.src_object_name,trg.src_object_name),
        anonymized_columns = decode(src.anonymized_columns,'#NULL#',null,null,trg.anonymized_columns,src.anonymized_columns),
        rls_columns = decode(src.rls_columns,'#NULL#',null,null,trg.rls_columns,src.rls_columns),
        enabled = nvl(src.enabled,trg.enabled),
        rls_all_values = decode(src.rls_all_values,['#NONE#'],null,null,trg.rls_all_values,src.rls_all_values),
        tags = decode(src.tags,['#NULL#'],null,null,trg.tags,src.tags)
    when not matched then
    insert
    (
        dist_database_name,
        dist_object_name,
        enabled,
        src_schema,
        src_object_name,
        anonymized_columns,
        rls_columns,
        rls_all_values,
        tags,
        aud_created_info,
        aud_updated_info
    )
    values
    (
        src.dist_database_name,
        src.dist_object_name,
        nvl(src.enabled,'Y'),
        src.src_schema,
        src.src_object_name,
        decode(src.anonymized_columns,'#NULL#',null,src.anonymized_columns),
        decode(src.rls_columns,'#NULL#',null,src.rls_columns),
        decode(src.rls_all_values,['#NONE#'],null,src.rls_all_values),
        decode(src.tags,['#NULL#'],null,src.tags),
        src.audit_info,
        src.audit_info
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
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object$remove
( 
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant
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

    if (:p_src_schema is null) then
        return 'The Source Schema is mandatory';
    end if;

    if (:p_src_object_name is null) then
        return 'The Source Object Name is mandatory';
    end if;    
        
    {{ sdc_distribute.tags_body() }}

    {{ sdc_distribute.set_sdc_distribute__object_sel() }}

    delete from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_access_rls
    where
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
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object
    where
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
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object$get_selected_objects
( 
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_action varchar,
    p_invocation_id varchar
)
returns variant
language sql
as
$$
declare
    l_return variant;
    l_invocation_id varchar;
    
    {{ sdc_distribute.tags_declare() }}
begin
    if (not regexp_like(:p_dist_database_name, '^DISTRIBUTE(_SF)?$')) then
        return 'The Distribution Database must follow the rule DISTRIBUTE(_SF)';
    end if;

    if (:p_dist_object_name is null) then
        return 'The Distribution Object Name is mandatory';
    end if;

    if (nvl(:p_action,'') not in ('C','D','FR')) then
        return 'The Action must be either C (create/replace), D (delete) or FR (force replace)';
    end if;

    {{ sdc_distribute.tags_body() }}

    {{ sdc_distribute.set_sdc_distribute__object_sel() }}

    select
        max(invocation_id)
    into
        l_invocation_id
    from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__data_distribute_mapping_table_sel
    ;

    if (l_invocation_id = p_invocation_id) then    
        null;
    else
        create or replace transient table " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__data_distribute_mapping_table_sel as
        select
            :p_invocation_id invocation_id,
            'DISTRIBUTE' dist_database_name,
            mapp.*
        from
            prd_distribute.snowflake_ops.data_distribute_mapping_table mapp
        where
            (action = '' or :p_action = 'D') and
            project_source_table_or_view like '" ~ '"' ~ sdc_distribute.get_target_database()|upper|replace('_','\\\\_') ~ '"' ~ ".%' escape '\\"~"\\"~"'
        union all
        select
            :p_invocation_id invocation_id,
            'DISTRIBUTE_SF' dist_database_name,
            mapp.*
        from
            prd_distribute.snowflake_ops.data_distribute_sf_mapping_table mapp
        where
            (action = '' or :p_action = 'D') and
            project_source_table_or_view like '" ~ '"' ~ sdc_distribute.get_target_database()|upper|replace('_','\\\\_') ~ '"' ~ ".%' escape '\\"~"\\"~"'
        ;
    end if;

    with diff_columns as (
        select distinct
            dist_database_name,
            dist_object_name
        from (
        select 
            sdc.dist_database_name,
            sdc.dist_object_name,
            inf_sch.column_name,
            inf_sch.data_type,
            inf_sch.ordinal_position,
            inf_sch.character_maximum_length,
            inf_sch.numeric_precision
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            inner join information_schema.columns inf_sch on
                inf_sch.table_catalog = '" ~ sdc_distribute.get_target_database()|upper ~ "' and
                inf_sch.table_schema = '" ~ sdc_distribute.get_target_schema()|upper ~ "'||sdc.src_schema and
                inf_sch.table_name = sdc.src_object_name
        where
            sdc.enabled = 'Y'
        union all
        select 
            sdc.dist_database_name,
            sdc.dist_object_name,
            inf_sch.column_name,
            inf_sch.data_type,
            inf_sch.ordinal_position,
            inf_sch.character_maximum_length,
            inf_sch.numeric_precision
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            inner join " ~ sdc_distribute.get_target_instance()|lower ~ "_distribute.information_schema.columns inf_sch on
                inf_sch.table_catalog = '" ~ sdc_distribute.get_target_instance()|upper ~ "_'||sdc.dist_database_name and
                inf_sch.table_schema = '" ~ sdc_distribute.get_target_project()|upper ~ "' and
                inf_sch.table_name = '" ~ sdc_distribute.get_target_schema()|upper ~ "'||sdc.dist_object_name
        where
            sdc.enabled = 'Y'
        union all
        select 
            sdc.dist_database_name,
            sdc.dist_object_name,
            inf_sch.column_name,
            inf_sch.data_type,
            inf_sch.ordinal_position,
            inf_sch.character_maximum_length,
            inf_sch.numeric_precision
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            inner join " ~ sdc_distribute.get_target_instance()|lower ~ "_distribute_sf.information_schema.columns inf_sch on
                inf_sch.table_catalog = '" ~ sdc_distribute.get_target_instance()|upper ~ "_'||sdc.dist_database_name and
                inf_sch.table_schema = '" ~ sdc_distribute.get_target_project()|upper ~ "' and
                inf_sch.table_name = '" ~ sdc_distribute.get_target_schema()|upper ~ "'||sdc.dist_object_name
        where
            sdc.enabled = 'Y'
        )
        qualify
            count(*) over (partition by
                dist_database_name,
                dist_object_name,           
                column_name,
                data_type,
                ordinal_position,
                character_maximum_length,
                numeric_precision
            ) != 2
    ),
    diff_params as (
        select distinct
            sdc.dist_database_name,
            sdc.dist_object_name
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            left outer join " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__data_distribute_mapping_table_sel mapp on
                mapp.dist_database_name = sdc.dist_database_name and
                mapp.distribute_database_target_view = '\"" ~ 
                    sdc_distribute.get_target_instance()|upper ~ "_'||sdc.dist_database_name||'\".\"" ~ 
                    sdc_distribute.get_target_project()|upper ~ "\".\"" ~ 
                    sdc_distribute.get_target_schema()|upper ~ "'||sdc.dist_object_name||'\"'
        where
            :p_action = 'D' and mapp.distribute_database_target_view is not null or
            mapp.project_source_table_or_view != '\"" ~ 
                    sdc_distribute.get_target_database()|upper ~ "\".\"" ~ 
                    sdc_distribute.get_target_schema()|upper ~ "'||sdc.src_schema||'\".\"" ~ 
                    "'||sdc.src_object_name||'\"' or
            nvl(sdc.anonymized_columns,'') != mapp.anonymized_columns or
            --nvl(sdc.rls_columns,'%') != mapp.rls_columns
            -- For the moment the RLS is not being passed to D2GO
            '%' != mapp.rls_columns
    )
    select 
        array_agg(object_construct(
            'src_schema',sdc.src_schema,
            'src_object_name',sdc.src_object_name,
            'dist_database_name',sdc.dist_database_name,
            'dist_object_name',sdc.dist_object_name,
            'anonymized_columns',sdc.anonymized_columns,
            'rls_columns',sdc.rls_columns
        ))
    into
        :l_return
    from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
        left outer join diff_columns on
            diff_columns.dist_database_name = sdc.dist_database_name and
            diff_columns.dist_object_name = sdc.dist_object_name
        left outer join diff_params on
            diff_params.dist_database_name = sdc.dist_database_name and
            diff_params.dist_object_name = sdc.dist_object_name
    where
        (   diff_columns.dist_database_name is not null or
            diff_params.dist_database_name is not null or
            :p_action = 'FR')
    ;

    return l_return;
end;
$$
        ", "
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object$set_framework_objects
(
    p_selected_objects_json varchar,
    p_action varchar
)
returns varchar
language sql
as
$$
declare
    l_return varchar;
    
    l_project_source_table_or_view varchar;
    l_database_target_view varchar;
    l_anonymized_columns varchar;
    l_rls_columns varchar;
    l_access_mgnt_table varchar;
    l_rls_access_mgnt_table varchar;

    l_row_count number default 0;
    l_count_distribute number default 0;
    l_count_distribute_sf number default 0;

begin
    if (nvl(:p_action,'') not in ('C','D','FR')) then
        return 'The Action must be either C (create/replace), D (delete) or FR (force replace)';
    end if;

    let cur_objects cursor for
    select
        value:src_schema::varchar src_schema,
        value:src_object_name::varchar src_object_name,
        value:dist_database_name::varchar dist_database_name,
        value:dist_object_name::varchar dist_object_name,
        value:anonymized_columns::varchar anonymized_columns,
        value:rls_columns::varchar rls_columns
    from
        table(flatten(parse_json(?)))
    where
        value:src_schema is not null and
        value:src_object_name is not null and
        value:dist_database_name is not null and
        value:dist_object_name is not null
    ;

    open cur_objects using (
        p_selected_objects_json
    );

    for rec in cur_objects do
        
        l_project_source_table_or_view := '\"'||current_database()||'\".\"" ~ sdc_distribute.get_target_schema() | upper ~ "'||rec.src_schema||'\".\"'||rec.src_object_name||'\"';
        l_database_target_view := '\"'||substr(current_database(),1,4)||rec.dist_database_name||'\".\"'||substr(current_database(),5)||'\".\"" ~ 
                                  sdc_distribute.get_target_schema() | upper ~ "'||rec.dist_object_name ||'\"';
        l_anonymized_columns := nvl(rec.anonymized_columns,'');
        --l_rls_columns := nvl(rec.rls_columns,'%');
        l_rls_columns := '%';
        l_access_mgnt_table := '\"'||current_database()||'\".\"" ~ sdc_distribute.get_target_schema() | upper ~ sdc_distribute.get_sdc_distribute_schema() | upper ~ "\".\"SDC_DISTRIBUTE__'||replace(rec.dist_database_name,'DISTRIBUTE','D2GO')||'_ACCESS_MGNT\"';
        l_rls_access_mgnt_table := '\"'||current_database()||'\".\"" ~ sdc_distribute.get_target_schema() | upper ~ sdc_distribute.get_sdc_distribute_schema() | upper ~ "\".\"SDC_DISTRIBUTE__'||replace(rec.dist_database_name,'DISTRIBUTE','D2GO')||'_RLS_ACCESS_MGNT\"';
        
        if (rec.dist_database_name = 'DISTRIBUTE') then
            l_count_distribute := l_count_distribute + 1;
            if (:p_action in ('C','FR')) then
                call common.distribute.prc_distribute_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => :l_project_source_table_or_view,
                    distribute_database_target_view => :l_database_target_view,
                    anonymized_columns => :l_anonymized_columns,
                    rls_columns => :l_rls_columns,
                    /*access_mgnt_table => :l_access_mgnt_table,*/
                    rls_access_mgnt_table => :l_rls_access_mgnt_table,
                    action => 'C'
                );
            elseif (:p_action = 'D') then
                call common.distribute.prc_distribute_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => '',
                    distribute_database_target_view => :l_database_target_view,
                    anonymized_columns => '',
                    rls_columns => '',
                    /*access_mgnt_table => '',*/
                    rls_access_mgnt_table => '',
                    action => 'D'
                );
            end if;
        elseif (rec.dist_database_name = 'DISTRIBUTE_SF') then
            l_count_distribute_sf := l_count_distribute_sf + 1;
            if (:p_action in ('C','FR')) then
                call common.distribute_sf.prc_distribute_sf_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => :l_project_source_table_or_view,
                    distribute_sf_database_target_view => :l_database_target_view,
                    rls_columns => :l_rls_columns,
                    access_mgnt_table => :l_access_mgnt_table,
                    rls_access_mgnt_table => :l_rls_access_mgnt_table,
                    action => 'C',
                    public => 'TRUE'
                );
            elseif (:p_action = 'D') then
                call common.distribute_sf.prc_distribute_sf_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => '',
                    distribute_sf_database_target_view => :l_database_target_view,
                    rls_columns => '',
                    access_mgnt_table => '',
                    rls_access_mgnt_table => '',
                    action => 'D',
                    public => 'TRUE'
                );
            end if;      
        end if;
        l_row_count := l_row_count+1;                
    end for;

    if (l_count_distribute > 0) then
        create or replace view " ~ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower ~ ".call_prc_distribute_view_processing as select true value;
    end if;

    if (l_count_distribute_sf > 0) then
        create or replace view " ~ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower ~ ".call_prc_distribute_sf_view_processing as select true value;
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
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object$rebuild_framework_objects
( 
    p_dist_database_name varchar,
    p_dist_object_name variant,
    p_src_schema varchar,
    p_src_object_name variant,
    p_action varchar
)
returns varchar
language sql
as
$$
declare
    l_row_count number;
    l_return varchar;
    
    l_project_source_table_or_view varchar;
    l_database_target_view varchar;
    l_anonymized_columns varchar;
    l_rls_columns varchar;
    l_access_mgnt_table varchar;
    l_rls_access_mgnt_table varchar;        
    l_count_distribute number default 0;
    l_count_distribute_sf number default 0;

    {{ sdc_distribute.tags_declare() }}
begin
    if (not regexp_like(:p_dist_database_name, '^DISTRIBUTE(_SF)?$')) then
        return 'The Distribution Database must follow the rule DISTRIBUTE(_SF)';
    end if;

    if (:p_dist_object_name is null) then
        return 'The Distribution Object Name is mandatory';
    end if;

    if (nvl(:p_action,'') not in ('C','D','FR')) then
        return 'The Action must be either C (create/replace), D (delete) or FR (force replace)';
    end if;

    {{ sdc_distribute.tags_body() }}

    {{ sdc_distribute.set_sdc_distribute__object_sel() }}

    l_row_count := 0;

    let cur_objects cursor for
    with diff_columns as (
        select distinct
            dist_database_name,
            dist_object_name
        from (
        select 
            sdc.dist_database_name,
            sdc.dist_object_name,
            inf_sch.column_name,
            inf_sch.data_type,
            inf_sch.ordinal_position,
            inf_sch.character_maximum_length,
            inf_sch.numeric_precision
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            inner join information_schema.columns inf_sch on
                inf_sch.table_catalog = '" ~ sdc_distribute.get_target_database()|upper ~ "' and
                inf_sch.table_schema = '" ~ sdc_distribute.get_target_schema()|upper ~ "'||sdc.src_schema and
                inf_sch.table_name = sdc.src_object_name
        where
            sdc.enabled = 'Y'
        union all
        select 
            sdc.dist_database_name,
            sdc.dist_object_name,
            inf_sch.column_name,
            inf_sch.data_type,
            inf_sch.ordinal_position,
            inf_sch.character_maximum_length,
            inf_sch.numeric_precision
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            inner join " ~ sdc_distribute.get_target_instance()|lower ~ "_distribute.information_schema.columns inf_sch on
                inf_sch.table_catalog = '" ~ sdc_distribute.get_target_instance()|upper ~ "_'||sdc.dist_database_name and
                inf_sch.table_schema = '" ~ sdc_distribute.get_target_project()|upper ~ "' and
                inf_sch.table_name = '" ~ sdc_distribute.get_target_schema()|upper ~ "'||sdc.dist_object_name
        where
            sdc.enabled = 'Y'
        union all
        select 
            sdc.dist_database_name,
            sdc.dist_object_name,
            inf_sch.column_name,
            inf_sch.data_type,
            inf_sch.ordinal_position,
            inf_sch.character_maximum_length,
            inf_sch.numeric_precision
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            inner join " ~ sdc_distribute.get_target_instance()|lower ~ "_distribute_sf.information_schema.columns inf_sch on
                inf_sch.table_catalog = '" ~ sdc_distribute.get_target_instance()|upper ~ "_'||sdc.dist_database_name and
                inf_sch.table_schema = '" ~ sdc_distribute.get_target_project()|upper ~ "' and
                inf_sch.table_name = '" ~ sdc_distribute.get_target_schema()|upper ~ "'||sdc.dist_object_name
        where
            sdc.enabled = 'Y'
        )
        qualify
            count(*) over (partition by
                dist_database_name,
                dist_object_name,           
                column_name,
                data_type,
                ordinal_position,
                character_maximum_length,
                numeric_precision
            ) != 2
    ),
    diff_params as (
        select distinct
            sdc.dist_database_name,
            sdc.dist_object_name
        from
            " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
            left outer join prd_distribute.snowflake_ops.data_distribute_mapping_table mapp on
                mapp.distribute_database_target_view = '\"" ~ 
                    sdc_distribute.get_target_instance()|upper ~ "_'||sdc.dist_database_name||'\".\"" ~ 
                    sdc_distribute.get_target_project()|upper ~ "\".\"" ~ 
                    sdc_distribute.get_target_schema()|upper ~ "'||sdc.dist_object_name||'\"'
        where
            ? = 'D' and mapp.distribute_database_target_view is not null or
            mapp.project_source_table_or_view != '\"" ~ 
                    sdc_distribute.get_target_database()|upper ~ "\".\"" ~ 
                    sdc_distribute.get_target_schema()|upper ~ "'||sdc.src_schema||'\".\"" ~ 
                    "'||sdc.src_object_name||'\"' or
            nvl(sdc.anonymized_columns,'') != mapp.anonymized_columns or
            --nvl(sdc.rls_columns,'%') != mapp.rls_columns
            -- For the moment the RLS is not being passed to D2GO
            '%' != mapp.rls_columns
    )
    select
        sdc.*
    from
        " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object_sel sdc
        left outer join diff_columns on
            diff_columns.dist_database_name = sdc.dist_database_name and
            diff_columns.dist_object_name = sdc.dist_object_name
        left outer join diff_params on
            diff_params.dist_database_name = sdc.dist_database_name and
            diff_params.dist_object_name = sdc.dist_object_name
    where
        (   diff_columns.dist_database_name is not null or
            diff_params.dist_database_name is not null or
            ? = 'FR')
    ;

    open cur_objects using
    (
        p_action,
        p_action
    );        

    for rec in cur_objects do
        
        l_project_source_table_or_view := '\"'||current_database()||'\".\"" ~ sdc_distribute.get_target_schema() | upper ~ "'||rec.src_schema||'\".\"'||rec.src_object_name||'\"';
        l_database_target_view := '\"'||substr(current_database(),1,4)||rec.dist_database_name||'\".\"'||substr(current_database(),5)||'\".\"" ~ 
                                  sdc_distribute.get_target_schema() | upper ~ "'||rec.dist_object_name ||'\"';
        l_anonymized_columns := nvl(rec.anonymized_columns,'');
        --l_rls_columns := nvl(rec.rls_columns,'%');
        l_rls_columns := '%';
        l_access_mgnt_table := '\"'||current_database()||'\".\"" ~ sdc_distribute.get_target_schema() | upper ~ sdc_distribute.get_sdc_distribute_schema() | upper ~ "\".\"SDC_DISTRIBUTE__'||replace(rec.dist_database_name,'DISTRIBUTE','D2GO')||'_ACCESS_MGNT\"';
        l_rls_access_mgnt_table := '\"'||current_database()||'\".\"" ~ sdc_distribute.get_target_schema() | upper ~ sdc_distribute.get_sdc_distribute_schema() | upper ~ "\".\"SDC_DISTRIBUTE__'||replace(rec.dist_database_name,'DISTRIBUTE','D2GO')||'_RLS_ACCESS_MGNT\"';
        
        if (rec.dist_database_name = 'DISTRIBUTE') then
            l_count_distribute := l_count_distribute + 1;
            if (:p_action in ('C','FR')) then
                call common.distribute.prc_distribute_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => :l_project_source_table_or_view,
                    distribute_database_target_view => :l_database_target_view,
                    anonymized_columns => :l_anonymized_columns,
                    rls_columns => :l_rls_columns,
                    /*access_mgnt_table => :l_access_mgnt_table,*/
                    rls_access_mgnt_table => :l_rls_access_mgnt_table,
                    action => 'C'
                );
            elseif (:p_action = 'D') then
                call common.distribute.prc_distribute_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => '',
                    distribute_database_target_view => :l_database_target_view,
                    anonymized_columns => '',
                    rls_columns => '',
                    /*access_mgnt_table => '',*/
                    rls_access_mgnt_table => '',
                    action => 'D'
                );
            end if;
        elseif (rec.dist_database_name = 'DISTRIBUTE_SF') then
            l_count_distribute_sf := l_count_distribute_sf + 1;
            if (:p_action in ('C','FR')) then
                call common.distribute_sf.prc_distribute_sf_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => :l_project_source_table_or_view,
                    distribute_sf_database_target_view => :l_database_target_view,
                    rls_columns => :l_rls_columns,
                    access_mgnt_table => :l_access_mgnt_table,
                    rls_access_mgnt_table => :l_rls_access_mgnt_table,
                    action => 'C',
                    public => 'TRUE'
                );
            elseif (:p_action = 'D') then
                call common.distribute_sf.prc_distribute_sf_mapping_table_" ~ sdc_distribute.get_target_database() | lower  ~ "
                (
                    project_source_table_or_view => '',
                    distribute_sf_database_target_view => :l_database_target_view,
                    rls_columns => '',
                    access_mgnt_table => '',
                    rls_access_mgnt_table => '',
                    action => 'D',
                    public => 'TRUE'
                );
            end if;      
        end if;
        l_row_count := l_row_count+1;                
    end for;

    if (l_count_distribute > 0) then
        create or replace view " ~ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower ~ ".call_prc_distribute_view_processing as select true value;
    end if;

    if (l_count_distribute_sf > 0) then
        create or replace view " ~ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower ~ ".call_prc_distribute_sf_view_processing as select true value;
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
create or replace procedure " ~ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() ~ ".sdc_distribute__object$drop_objects
( 
    p_schema varchar,
    p_object_name variant
)
returns varchar
language sql
as
$$
declare
    l_row_count number;
    l_return varchar;    
begin

    if (:p_schema is null) then
        return 'The Source Schema is mandatory';
    end if;

    if (:p_object_name is null) then
        return 'The Source Object Name is mandatory';
    end if;

    l_row_count := 0;

    let cur_objects cursor for
    with srch as (
        select
            prefix||value expression
        from
            (select to_array(parse_json(?)) arr), lateral flatten(arr),
            (select column1 prefix from values 
                (''),
                ('\\"~"\\"~"_'),
                ('DISTRIBUTE\\"~"\\"~"_'),
                ('DISTRIBUTE\\"~"\\"~"_\\"~"\\"~"_'),
                ('DISTRIBUTE\\"~"\\"~"_SF\\"~"\\"~"_'),
                ('DISTRIBUTE\\"~"\\"~"_SF\\"~"\\"~"_\\"~"\\"~"_')
            ) pre
    ) 
    select
        table_schema,
        table_name,
        case 
            when table_type = 'VIEW' then table_type
            else 'TABLE'
        end table_type
    from
        information_schema.tables sch
    where
        table_schema like '" ~ sdc_distribute.get_target_schema() | upper | replace('_','\\\\_') ~ "'||? escape '\\"~"\\"~"' and
        ('" ~ sdc_distribute.get_target_schema() | upper ~ "' like 'DBT\\"~"\\"~"_%' escape '\\"~"\\"~"' or table_schema not like 'DBT\\"~"\\_%' escape '\\"~"\\"~"') and
        table_name like any (select expression from srch) escape '\\"~"\\"~"'
    ;

    open cur_objects using
    (
        p_object_name::varchar,
        p_schema
    );        

    for rec in cur_objects do
        execute immediate 'drop '||rec.table_type||' if exists '||rec.table_schema||'.'||rec.table_name;
        l_row_count := l_row_count+1;
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
    null::varchar(1) enabled,
    null::varchar src_schema,
    null::varchar src_object_name,
    null::varchar anonymized_columns,
    null::varchar rls_columns,
    null::variant rls_all_values,
    null::variant tags,
    null::variant aud_created_info,
    null::variant aud_updated_info
where
    false
