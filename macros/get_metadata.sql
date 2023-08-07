{% macro get_target_database() -%}

    {{ return(adapter.dispatch('get_target_database', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__get_target_database() -%}

    {%- if target.database is none -%}

    {%- else -%}

        {{ target.database | trim }}

    {%- endif -%}

{%- endmacro %}

{% macro get_target_schema() -%}

    {{ return(adapter.dispatch('get_target_schema', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__get_target_schema() -%}

    {%- if target.schema is none or target.user.endswith('_TU_ETL_DBT') -%}

    {%- else -%}

        {{ target.schema | trim }}_

    {%- endif -%}

{%- endmacro %}

{% macro get_sdc_distribute_schema() -%}

    {{ return(adapter.dispatch('get_sdc_distribute_schema', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__get_sdc_distribute_schema() -%}

    {{ var('sdc_distribute__schema','sdc_distribute') }}

{%- endmacro %}

{% macro get_target_instance() -%}

    {{ return(adapter.dispatch('get_target_instance', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__get_target_instance() -%}

    {%- if target.database is none -%}

    {%- else -%}

        {{ target.database[0:3] }}

    {%- endif -%}

{%- endmacro %}

{% macro get_target_project() -%}

    {{ return(adapter.dispatch('get_target_project', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__get_target_project() -%}

    {%- if target.database is none -%}

    {%- else -%}

        {{ target.database[4:] }}

    {%- endif -%}

{%- endmacro %}

{% macro get_current_role() -%}

    {{ return(adapter.dispatch('get_current_role', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__get_current_role() -%}

    {%- set sql_query = "select current_role() current_role" -%}

    {%- set results = run_query(sql_query) -%}

    {%- if execute -%}
        {{ results.columns[0].values()[0] }}
    {%- endif -%}

{%- endmacro %}

{% macro get_current_account() -%}

    {{ return(adapter.dispatch('get_current_account', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__get_current_account() -%}

    {%- set sql_query = "select current_account() current_account" -%}

    {%- set results = run_query(sql_query) -%}

    {%- if execute -%}
        {{ results.columns[0].values()[0] }}
    {%- endif -%}

{%- endmacro %}

{% macro get_nodes(p_path=none,p_schema=none,p_select_any_tags=[],p_select_all_tags=[],p_exclude_any_tags=[],p_exclude_all_tags=[],p_exclude_distributed=true) -%}

    {{ return(adapter.dispatch('get_nodes', 'sdc_distribute')(p_path=p_path,
                                                              p_schema=p_schema,
                                                              p_select_any_tags=p_select_any_tags,
                                                              p_select_all_tags=p_select_all_tags,
                                                              p_exclude_any_tags=p_exclude_any_tags,
                                                              p_exclude_all_tags=p_exclude_all_tags,
                                                              p_exclude_distributed=p_exclude_distributed)) }}

{%- endmacro %}

{% macro default__get_nodes(p_path=none,p_schema=none,p_select_any_tags=[],p_select_all_tags=[],p_exclude_any_tags=[],p_exclude_all_tags=[],p_exclude_distributed=true) -%}

    {%- set l_node_list = graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("package_name", "equalto", project_name) -%}

    {%- if p_path or p_schema or p_select_any_tags or p_select_all_tags or p_exclude_any_tags or p_exclude_all_tags -%}
        {%- set l_aux = [] -%}
        {%- set l_select_any_tags = p_select_any_tags | unique | list -%}
        {%- set l_select_all_tags = p_select_all_tags | unique | list -%}
        {%- set l_exclude_any_tags = p_exclude_any_tags | unique | list -%}
        {%- set l_exclude_all_tags = p_exclude_all_tags | unique | list -%}

        {%- for l_node in l_node_list -%}
            {%- if  ( not p_path or (l_node.fqn[1:] | join(".") | upper ~ '.').startswith(p_path.upper() ~ '.') ) and
                    ( not p_schema or l_node.schema.upper() == p_schema.upper() ) and
                    ( not l_select_any_tags or sdc_distribute.array_intersect(l_select_any_tags,l_node.tags) | length > 0 ) and
                    ( not l_select_all_tags or sdc_distribute.array_intersect(l_select_all_tags,l_node.tags|unique|list) | length == l_select_all_tags | length ) and
                    ( not l_exclude_any_tags or sdc_distribute.array_intersect(l_exclude_any_tags,l_node.tags) | length == 0 ) and
                    ( not l_exclude_all_tags or sdc_distribute.array_intersect(l_exclude_all_tags,l_node.tags|unique|list) | length < l_exclude_all_tags | length ) and
                    ( not p_exclude_distributed or not sdc_distribute.node_attribute_contains(l_node,['unrendered_config','post-hook'],'sdc_distribute.post_hook_operations') ) -%}
                {%- do l_aux.append(l_node) -%}
            {%- endif -%}        
        {%- endfor -%}

        {%- set l_node_list = l_aux.copy() -%}
    {%- endif -%}

    {{ return(l_node_list) }}

{%- endmacro %}

{% macro get_node(p_name) -%}

    {{ return(adapter.dispatch('get_node', 'sdc_distribute')(p_name=p_name)) }}

{%- endmacro %}

{% macro default__get_node(p_name) -%}

    {%- set l_node_list = graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("package_name", "equalto", project_name) 
        | selectattr("name", "equalto", p_name) 
        | list -%}

    {%- if ( l_node_list ) -%}
        {{ return(l_node_list[0]) }}
    {%- else -%}
        {{ return(none) }}
    {%- endif -%}    

{%- endmacro %}

{% macro get_columns_info(p_table_catalog,p_table_schema,p_table_name) -%}

    {{ return(adapter.dispatch('get_columns_info', 'sdc_distribute')(p_table_catalog=p_table_catalog,
                                                                     p_table_schema=p_table_schema,
                                                                     p_table_name=p_table_name)) }}

{%- endmacro %}

{% macro default__get_columns_info(p_table_catalog,p_table_schema,p_table_name) -%}

    {%- set l_sql_query -%}
        select
            column_name "column_name",
            data_type "data_type"
        from
            {{ p_table_catalog }}.information_schema.columns
        where
            table_catalog = '{{ p_table_catalog }}' and
            table_schema = '{{ p_table_schema }}' and
            table_name = '{{ p_table_name }}'
    {%- endset -%}

    {%- set l_results = run_query(l_sql_query) -%}

    {%- if ( execute ) -%}
        {{ return(l_results.rows) }}
    {%- endif -%}

{%- endmacro %}
