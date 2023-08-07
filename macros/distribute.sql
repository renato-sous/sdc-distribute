{% macro post_hook_operations(p_model=model) -%}

    {{ return(adapter.dispatch('post_hook_operations', 'sdc_distribute')(p_model=p_model)) }}

{%- endmacro %}

{% macro default__post_hook_operations(p_model=model) -%}

    {%- if execute -%}

        {#%- set l_default_grantee_list = "'" ~ p_model.config.get('sdc_distribute__default_grantee_list',sdc_distribute.get_target_project() | upper ~ "_DISTRIBUTE") | join("','") ~ "'" -%#}
        {#%- set l_default_grantee_list = p_model.config.get('sdc_distribute__default_grantee_list',"'" ~ sdc_distribute.get_target_project() | upper ~ "_DISTRIBUTE'") -%#}
        {%- set l_default_grantee_list = p_model.config.get('sdc_distribute__default_grantee_list', [sdc_distribute.get_target_project() | upper ~ "_DISTRIBUTE"]) -%}
        {%- set l_restricted_access = p_model.config.get('sdc_distribute__restricted_access', false) -%}
        {%- set l_force_update = 'N' -%}

        {%- if ((p_model.name|lower).startswith('_schema_')) -%}
            {%- set l_model_name = '%' -%}
            {%- set l_src_schema = p_model.config.get('sdc_distribute__src_schema',p_model.name[8:]) | upper -%}
            {%- set l_src_object_name_list = ['%'] -%}
            {%- set l_distribute_anonymized_columns = "'#NULL#'" -%}
            {%- set l_prepared_rls_columns = "'#NULL#'" -%}
            {%- set l_prepared_rls_all_values = ['#NONE#'] -%}        
            {%- set l_distribute_path = p_model.config.get('sdc_distribute__src_path') -%}
            {%- set l_distribute_tags = p_model.config.get('sdc_distribute__src_tags',[]) -%}
            {%- set l_select_any_tags = p_model.config.get('sdc_distribute__src_select_any_tags',[]) -%}
            {%- set l_select_all_tags = p_model.config.get('sdc_distribute__src_select_all_tags',[]) -%}
            {%- set l_exclude_any_tags = p_model.config.get('sdc_distribute__src_exclude_any_tags',[]) -%}
            {%- set l_exclude_all_tags = p_model.config.get('sdc_distribute__src_exclude_all_tags',[]) -%}
            {%- set l_select_any_tags = l_select_any_tags + l_distribute_tags -%}

            {%- if ( execute ) -%}

                {%- set l_src_object_name_list = sdc_distribute.get_nodes(l_distribute_path,sdc_distribute.get_target_schema() ~ l_src_schema,
                                                                        l_select_any_tags, l_select_all_tags, l_exclude_any_tags, l_exclude_all_tags) -%}

                {%- if ( l_src_object_name_list ) -%}

                    {%- set l_src_object_name_list = ("_" ~ l_src_object_name_list
                            | map(attribute='name') | join(",_")).upper().split(",") -%}

                    {%- set l_all_object_name_list = (sdc_distribute.get_nodes() 
                            | map(attribute='name') | join(",")).upper().split(",") -%}

                    {# Remove the objects that have a _model existing #}
                    {%- set l_src_object_name_list = sdc_distribute.array_minus(l_src_object_name_list,l_all_object_name_list) -%}
                    {%- if l_src_object_name_list | length > 0 -%}
                        {%- set l_src_object_name_list = (l_src_object_name_list | join(",") | replace(",_",","))[1:].split(",") -%}
                    {%- endif -%}

                {%- endif -%}

                {%- set l_selected_models = l_src_object_name_list | length -%}

                {%- if ( not l_selected_models ) -%}
                    {{ log(p_model.name ~ ' has not selected any models', info=True) }}
                {%- elif l_selected_models == 1 -%}
                    {{ log(p_model.name ~ ' selected 1 model', info=True) }}
                {%- else -%}
                    {{ log(p_model.name ~ ' selected ' ~ l_src_object_name_list | length ~ ' models', info=True) }}
                {%- endif -%}
            {%- endif -%}
        {%- else -%}
            {%- if ( p_model.name[0] == '_' ) -%}
                {%- set l_model_name = p_model.name[1:] | upper -%}
            {%- else -%}
                {%- set l_model_name = p_model.name | upper -%}
            {%- endif -%}

            {%- set l_distribute_distributed_model = p_model.config.get('sdc_distribute__distributed_model','') | lower -%}
            {%- if ( l_distribute_distributed_model == 'auxiliary' or p_model.name[0] != '_') -%}
                {%- set l_src_object_name = p_model.name | upper -%}
                {%- set l_src_schema = p_model.schema[sdc_distribute.get_target_schema()|length:] | upper -%}
                {%- set l_distribute_path = p_model.fqn[1:-1] | join(".") -%}
            {%- else -%}
                {%- set l_src_object_name = p_model.name[1:] | upper -%}
                {%- set l_src_schema = ref(p_model.name[1:]).schema[sdc_distribute.get_target_schema()|length:] | upper -%}
                {%- if execute -%}
                    {%- set l_distribute_path = sdc_distribute.get_node(p_model.name[1:]).fqn[1:-1] | join(".") -%}
                {%- endif -%}
            {%- endif -%}

            {%- set l_src_object_name_list = [l_src_object_name] -%}

            {%- set l_distribute_anonymized_columns = p_model.config.get('sdc_distribute__anonymized_columns') -%}
            {%- if ( l_distribute_anonymized_columns ) -%}
                {%- set l_distribute_anonymized_columns = "'" ~ l_distribute_anonymized_columns | upper ~ "'" -%}
            {%- else -%}
                {%- set l_distribute_anonymized_columns = "'#NULL#'" -%}
            {%- endif -%}

            {%- set l_distribute_rls_columns = p_model.config.get('sdc_distribute__rls_columns') -%}
            {%- if ( l_distribute_rls_columns ) -%}
                {%- set l_prepared_rls_columns = "'" ~ l_distribute_rls_columns | upper ~ "'" -%}
            {%- else -%}
                {%- set l_prepared_rls_columns = "'#NULL#'" -%}
            {%- endif -%}

            {%- set l_distribute_rls_all_values = p_model.config.get('sdc_distribute__rls_all_values') -%}
            {%- if ( l_distribute_rls_all_values ) -%}
                {%- set l_prepared_rls_all_values = l_distribute_rls_all_values -%}
            {%- else -%}
                {%- set l_prepared_rls_all_values = ['#NONE#'] -%}
            {%- endif -%}

        {%- endif -%}

        {%- set l_distribute_tags = p_model.config.get('tags',[]) -%}
        {%- if ( l_distribute_path ) -%}
            {%- do l_distribute_tags.append(l_distribute_path) -%}
        {%- endif -%}
        {%- if ( l_distribute_tags ) -%}
            {%- set l_prepared_tags = l_distribute_tags -%}
        {%- else -%}
            {%- set l_prepared_tags = ['#NULL#'] -%}
        {%- endif -%}

        {%- set l_distribute_distribution_scope = p_model.config.get('sdc_distribute__distribution_scope','') | lower -%}
        {%- set l_dist_database_list = ['DISTRIBUTE'] -%}
        {%- if ( l_distribute_distribution_scope == 'external') -%}
            {%- set l_dist_database_list = ['DISTRIBUTE_SF'] -%}
        {%- elif ( l_distribute_distribution_scope == 'both') -%}
            {%- set l_dist_database_list = ['DISTRIBUTE','DISTRIBUTE_SF'] -%}
        {%- else -%}
            {%- set l_dist_database_list = ['DISTRIBUTE'] -%}
        {%- endif -%}

        {%- for l_dist_database_name in l_dist_database_list -%}

            {%- if l_distribute_rls_columns or l_restricted_access -%}
                {%- set l_src_object_name = (l_dist_database_name ~ '_' ~ l_model_name) | upper -%}
                {%- set l_src_schema = p_model.schema[sdc_distribute.get_target_schema()|length:] | upper -%}

                {%- set l_src_object_name_list = [l_src_object_name] -%}

                {%- call statement('exec', fetch_result=False) -%}
    create or replace view {{ p_model.database ~ '.' ~ p_model.schema ~ '.' ~ l_src_object_name | lower }} as (
        {{ sdc_distribute.get_distribute_rls_sql(l_distribute_rls_columns,l_dist_database_name,l_prepared_rls_all_values) }}
    );
                {%- endcall -%}
            {%- endif -%}

            {%- if l_dist_database_name == 'DISTRIBUTE_SF' -%}
                {#%- set l_grantee_list = "['" ~ sdc_distribute.get_current_account() | upper ~ "_SF_ACCOUNT']" -%#}
                {%- set l_grantee_list = "[]" -%}
            {%- else -%}
                {%- set l_grantee_list = l_default_grantee_list -%}
            {%- endif -%}

            {%- call statement('exec', fetch_result=False) -%}
            call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$set
            (
                p_dist_database_name => '{{ l_dist_database_name }}',
                p_dist_object_name => '{{ l_model_name }}',
                p_src_schema => '{{ l_src_schema }}',
                p_src_object_name => {{ l_src_object_name_list }},
                p_anonymized_columns => {{ l_distribute_anonymized_columns }},
                p_rls_columns => {{ l_prepared_rls_columns }},
                p_enabled => 'Y',
                p_rls_all_values => {{ l_prepared_rls_all_values }},
                p_tags => {{ l_prepared_tags }}
            )
            {%- endcall -%}

            {% if target.name in var('sdc_distribute__d2go_exclude_environments',[]) %}

                {# this is empty to disable the call to d2go if the environment is in the exclude list #}
                {{ log('Rebuild distribute layer objects was skipped because current target ('~target.name~') is on the exclude list', info=True) }}
                {%- set l_action = "" -%}
 
            {%- elif var('sdc_distribute__d2go_force_replace',false) -%}

                {{ log('Rebuild (FORCED) distribute layer objects started for model ' ~ p_model.name|lower, info=True) }}
                {%- set l_action = "FR" -%}

                {#%- call statement('exec', fetch_result=False) -%}
                call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$rebuild_framework_objects
                (
                    p_dist_database_name => '{{ l_dist_database_name }}',
                    p_dist_object_name => ['{{ l_model_name }}'],
                    p_src_schema => '{{ l_src_schema }}',
                    p_src_object_name => {{ l_src_object_name_list }},
                    p_action => 'FR'
                )
                {%- endcall -%#}

            {%- else -%}

                {{ log('Rebuild (IF CHANGED) distribute layer objects started for model ' ~ p_model.name|lower, info=True) }}
                {%- set l_action = "C" -%}

                {#%- call statement('exec', fetch_result=False) -%}
                call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$rebuild_framework_objects
                (
                    p_dist_database_name => '{{ l_dist_database_name }}',
                    p_dist_object_name => ['{{ l_model_name }}'],
                    p_src_schema => '{{ l_src_schema }}',
                    p_src_object_name => {{ l_src_object_name_list }},
                    p_action => 'C'
                )
                {%- endcall -%#}

            {%- endif -%}

            {% if l_action in ['FR','C'] %}

                {%- call statement('exec', fetch_result=True) -%}
                call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$get_selected_objects
                ( 
                    p_dist_database_name => '{{ l_dist_database_name }}',
                    p_dist_object_name => ['{{ l_model_name }}'],
                    p_src_schema => '{{ l_src_schema }}',
                    p_src_object_name => {{ l_src_object_name_list }},
                    p_action => '{{ l_action }}',
                    p_invocation_id => '{{ invocation_id }}'
                )
                {%- endcall -%}

                {%- set l_selected_objects_result = load_result('exec') -%}
                {%- set l_selected_objects_table = l_selected_objects_result['table'] -%}
                {%- set l_selected_objects_json = l_selected_objects_table.columns[0].values()[0] -%}
                {%- set l_selected_objects_count = fromjson(l_selected_objects_json) | length -%}

                {% if l_selected_objects_count == 0 %}
                    {{ log( 'No Rebuilding is necessary for model ' ~ p_model.name|lower, info=True) }}
                {% elif l_selected_objects_count == 1 %}
                    {{ log( 'Rebuilding object for model ' ~ p_model.name|lower, info=True) }}
                {% else %}
                    {{ log( 'Rebuilding ' ~ l_selected_objects_count ~ ' objects for model ' ~ p_model.name|lower, info=True) }}
                {% endif %}

                {%- call statement('exec', fetch_result=False) -%}
                call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$set_framework_objects
                ( 
                    p_selected_objects_json => '{{ l_selected_objects_json }}',
                    p_action => '{{ l_action }}'
                )
                {%- endcall -%}
            {%- endif -%}

            {% if var('sdc_distribute__d2go_force_replace',false) %}
                {%- set l_force_update = 'Y' -%}
            {%- endif -%}

            {%- call statement('exec', fetch_result=False) -%}
            call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access$set
            (
                p_dist_database_name => '{{ l_dist_database_name }}',
                p_dist_object_name => ['{{ l_model_name }}'],
                p_src_schema => '{{ l_src_schema }}',
                p_src_object_name => {{ l_src_object_name_list }},
                p_grantee => {{ l_grantee_list}},
                p_enabled => 'Y',
                p_defer_set_framework_objects => 'Y',
                p_force_update => '{{ l_force_update }}'
            )
            {%- endcall -%}

            {%- if ( l_distribute_rls_columns ) -%}
                {%- call statement('exec', fetch_result=False) -%}
                call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access_rls$set
                (
                    p_dist_database_name => '{{ l_dist_database_name }}',
                    p_dist_object_name => ['{{ l_model_name }}'],
                    p_src_schema => '{{ l_src_schema }}',
                    p_src_object_name => {{ l_src_object_name_list }},
                    p_grantee => {{ l_grantee_list}},
                    p_enabled => 'Y'
                    {{- sdc_distribute.get_rls_columns_expression(',
                    p_#COLUMN_NAME# => null') }}
                )
                {%- endcall -%}
            {%- endif -%}

        {%- endfor -%}

        {% if (p_model.name[0] == '_' and l_distribute_distributed_model != 'auxiliary') %}
            {%- call statement('exec', fetch_result=False) -%}
            drop view if exists {{ p_model.schema ~ "." ~ p_model.name }}
            {%- endcall -%}
        {% endif %}
    
    {% endif %}

{%- endmacro %}


{% macro on_run_end_operations() -%}

    {{ return(adapter.dispatch('on_run_end_operations', 'sdc_distribute')()) }}

{%- endmacro %}

{% macro default__on_run_end_operations() -%}
    {{ log( 'Executing SDC Distribute on_run_end_operations', info=True) }}
    {%- if
        sdc_distribute.get_columns_info(
            sdc_distribute.get_target_database() | upper,
            sdc_distribute.get_target_schema() | upper ~ sdc_distribute.get_sdc_distribute_schema() | upper,
            'CALL_PRC_DISTRIBUTE_VIEW_PROCESSING') | length > 0
    -%}
        {%- call statement('exec', fetch_result=False) -%}
            drop view if exists {{ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower }}.call_prc_distribute_view_processing
        {%- endcall -%}

        {{ log( 'Calling View Processing on Snowflake Ops framework', info=True) }}

        {%- call statement('exec', fetch_result=False) -%}
            call common.distribute.prc_distribute_view_processing_{{ sdc_distribute.get_target_database() | lower }}()
        {%- endcall -%}

        {%- call statement('exec', fetch_result=False) -%}
            truncate table if exists {{ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower }}.sdc_distribute__data_distribute_mapping_table_sel
        {%- endcall -%}

    {%- endif -%}

    {% if 
        sdc_distribute.get_columns_info(
            sdc_distribute.get_target_database() | upper,
            sdc_distribute.get_target_schema() | upper ~ sdc_distribute.get_sdc_distribute_schema() | upper,
            'CALL_PRC_DISTRIBUTE_SF_VIEW_PROCESSING') | length > 0
    %}
        {%- call statement('exec', fetch_result=False) -%}
            drop view if exists {{ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower }}.call_prc_distribute_sf_view_processing
        {%- endcall -%}
    
        {{ log( 'Calling SF View Processing on Snowflake Ops framework', info=True) }}

        {%- call statement('exec', fetch_result=False) -%}
            call common.distribute_sf.prc_distribute_sf_view_processing_{{ sdc_distribute.get_target_database() | lower }}()
        {%- endcall -%}

        {%- call statement('exec', fetch_result=False) -%}
            truncate table if exists {{ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower }}.sdc_distribute__data_distribute_mapping_table_sel
        {%- endcall -%}

    {% endif %}

    {%- if
        sdc_distribute.get_columns_info(
            sdc_distribute.get_target_database() | upper,
            sdc_distribute.get_target_schema() | upper ~ sdc_distribute.get_sdc_distribute_schema() | upper,
            'CALL_PRC_DISTRIBUTE_GRANT_ACCESS') | length > 0
    -%}
        {%- call statement('exec', fetch_result=False) -%}
            drop view if exists {{ sdc_distribute.get_target_schema() | lower ~ sdc_distribute.get_sdc_distribute_schema() | lower }}.call_prc_distribute_grant_access
        {%- endcall -%}

        {{ log( 'Calling Grant Access on Snowflake Ops framework', info=True) }}

        {%- call statement('exec', fetch_result=False) -%}
            call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access$set_framework_objects
            (   
                p_updated_since => '{{ run_started_at.astimezone(modules.pytz.timezone("Europe/Berlin")) }}'
            )
        {%- endcall -%}

    {%- endif -%}
    {{ log( 'Done executing SDC Distribute on_run_end_operations', info=True) }}
{%- endmacro %}


{% macro get_distribute_rls_sql(p_rls_columns,p_dist_database_name,p_rls_all_values,p_model=model) -%}

    {{ return(adapter.dispatch('get_distribute_rls_sql', 'sdc_distribute')(p_rls_columns=p_rls_columns,
                                                                           p_dist_database_name=p_dist_database_name,
                                                                           p_rls_all_values=p_rls_all_values,
                                                                           p_model=p_model)) }}
{%- endmacro %}

{% macro default__get_distribute_rls_sql(p_rls_columns,p_dist_database_name,p_rls_all_values,p_model=model) -%}

    {%- set l_model_name = p_model.name -%}
    {%- if (p_model.name[0] == '_') -%}
        {%- set l_model_name = l_model_name[1:] -%}
    {%- endif -%}

    {%- if (p_rls_columns) -%}
        {%- set l_rls_columns_list = p_rls_columns.split(',') | map('trim') | list -%}    
        {%- set l_rls_all_values_list = p_rls_all_values[:l_rls_columns_list | length] -%}
        {%- if (not l_rls_all_values_list) -%}
            {%- set l_rls_all_values_list = ['#NONE#'] -%}
        {%- endif -%}
        {%- for l_column in l_rls_columns_list if l_rls_all_values_list | length < l_rls_columns_list | length -%}
            {%- do l_rls_all_values_list.append('#NONE#') -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set l_select_expression -%}
      {%- for l_column in l_rls_columns_list -%}
        {{ l_column }}{%- if not loop.last -%},
            {% endif -%}
      {%- endfor %}
    {%- endset -%}    
    
    {%- set l_qualify_expression -%}
        row_number() over (partition by
        {%- for l_column in l_rls_columns_list %}            
                {{ l_column }}{%- if not loop.last -%},
                {%- endif -%}
        {%- endfor %}
            order by null) = 1 and
            dense_rank() over (order by sign(
        {%- for l_column in l_rls_columns_list %}
                nvl2({{ l_column }},1,0){%- if not loop.last -%}+
            {%- endif -%}
        {%- endfor %}
            ) desc) = 1 and
        {%- for l_column in l_rls_columns_list -%}
            {%- set l_other_columns_list = l_rls_columns_list.copy() -%}
            {%- do l_other_columns_list.remove(l_column) -%}
            {%- if l_other_columns_list %}
            dense_rank() over (partition by
                {%- for l_other_column in l_other_columns_list %}                    
                {{ l_other_column }}{%- if not loop.last -%},
            {%- endif -%}
                {%- endfor %}
            order by nvl2({{ l_column }},1,2)
            ) = 1 and
            dense_rank() over (partition by {{ l_column }} order by sign(
                {%- for l_other_column in l_other_columns_list %}
                nvl2({{ l_other_column }},1,0){%- if not loop.last -%}+
            {%- endif -%}
                {%- endfor %}
            ) desc) = 1 and  
            {%- endif -%}
        {%- endfor %}
    {%- endset -%}

    {%- if l_model_name != model.name -%}
        {%- set l_ref_model = ref(l_model_name) -%}
    {%- else -%}
        {%- set l_ref_model = adapter.get_relation(model.database, model.schema, model.name) -%}
        {%- if l_ref_model is none -%}
            {%- set l_ref_model = api.Relation.create(database=p_model.database, schema=p_model.schema, identifier=p_model.name) -%}
        {%- endif -%}
    {%- endif -%}

    {%- set l_column_info_list = sdc_distribute.get_columns_info(
            l_ref_model.database | upper,
            l_ref_model.schema | upper,
            l_ref_model.name | upper ) -%}

    {%- set l_join_expression -%}    

        {%- for l_column in l_rls_columns_list -%}
            {%- if ( l_rls_all_values_list[loop.index-1] == '#NONE#' ) -%}
                {%- set l_rls_all_values_expression = '' -%}
            {%- elif ( l_rls_all_values_list[loop.index-1] == '#NULL#' ) -%}
                {%- set l_rls_all_values_expression = "or src." ~ l_column ~ " is null" -%}
            {%- else -%}
                {%- set l_rls_all_values_expression = "or src." ~ l_column ~ " = '" ~ l_rls_all_values_list[loop.index-1] ~ "'" -%}
            {%- endif -%}

            {%- set ns_column = namespace(expression=none) -%}
            {%- for l_column_info in l_column_info_list
                if ( l_column_info.column_name == (l_column | upper) and l_column_info.data_type == 'VARIANT' ) -%}
                    {%- set ns_column.expression = "array_contains(rls." ~ l_column ~ "::variant,src." ~ l_column ~ ")" -%}                    
            {%- endfor -%}
            {%- if ( not ns_column.expression ) -%}
                {%- set ns_column.expression = "rls." ~ l_column ~ " = src." ~ l_column -%}
            {%- endif -%}

            ({{ ns_column.expression }} or rls.{{ l_column }} is null {{ l_rls_all_values_expression }})
            {%- if not loop.last %} and
            {% endif -%}
        {%- endfor %}
    {%- endset -%}

    {%- if (p_rls_columns) -%}

        with rls as (
            {% if ( p_dist_database_name | lower == 'distribute_sf') -%}

            select
                {{ l_select_expression }}
            from
                {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__d2go_sf_rls_access_mgnt
            where
                view_name = '"{{ sdc_distribute.get_target_instance()|upper }}_DISTRIBUTE_SF"."{{ sdc_distribute.get_target_project()|upper }}"."{{ sdc_distribute.get_target_schema()|upper }}{{ l_model_name|upper }}"' and
                account = current_account()||'_SF_ACCOUNT'
            qualify
                {{ l_qualify_expression }}
                true

            {%- else -%}

            select
                {{ l_select_expression }}
            from
                {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__d2go_rls_access_mgnt
            where
                view_name = '"{{ sdc_distribute.get_target_instance()|upper }}_DISTRIBUTE"."{{ sdc_distribute.get_target_project()|upper }}"."{{ sdc_distribute.get_target_schema()|upper }}{{ l_model_name|upper }}"' and
                user_or_role in (current_user(), regexp_replace(current_role(), '(DEV|QUA|PRD)_(.+)_(DEVELOPER|ETL|READONLY|ANALYST|DISTRIBUTE_CONSUMER)(_.*)?','\\2_DISTRIBUTE'))
            qualify
                {{ l_qualify_expression }}
                true

            {%- endif %}
        )
        select
            src.*
        from
            {{ l_ref_model }} src
        where exists (select 1 from rls where
            {{ l_join_expression }})

    {%- else -%}

        with axx as (
            {% if ( p_dist_database_name | lower == 'distribute_sf') -%}

            select
                1
            from
                {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__d2go_sf_access_mgnt
            where
                view_name = '"{{ sdc_distribute.get_target_instance()|upper }}_DISTRIBUTE_SF"."{{ sdc_distribute.get_target_project()|upper }}"."{{ sdc_distribute.get_target_schema()|upper }}{{ l_model_name|upper }}"' and
                account = current_account()||'_SF_ACCOUNT'
            having
                count(*) > 0

            {%- else -%}

            select
                1
            from
                {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__d2go_access_mgnt
            where
                view_name = '"{{ sdc_distribute.get_target_instance()|upper }}_DISTRIBUTE"."{{ sdc_distribute.get_target_project()|upper }}"."{{ sdc_distribute.get_target_schema()|upper }}{{ l_model_name|upper }}"' and
                user_or_role in (current_user(), regexp_replace(current_role(), '(DEV|QUA|PRD)_(.+)_(DEVELOPER|ETL|READONLY|ANALYST|DISTRIBUTE_CONSUMER)(_.*)?','\\2_DISTRIBUTE'))
            having
                count(*) > 0

            {%- endif %}
        )
        select
            src.*
        from
            {{ l_ref_model }} src
            cross join axx

    {%- endif -%}

{%- endmacro %}


{% macro get_distribute_sql(p_model=model) -%}

    {{ return(adapter.dispatch('get_distribute_sql', 'sdc_distribute')(p_model=p_model)) }}

{%- endmacro %}

{% macro default__get_distribute_sql(p_model=model) -%}

    {%- if (p_model is not iterable or 'name' not in p_model) -%}
        {{ return('') }}
    {%- endif -%}
    {%- set l_model_name = p_model.name -%}
    {%- if (p_model.name[0] == '_') -%}
        {%- set l_model_name = l_model_name[1:] -%}
    {%- endif -%}
    {%- if ((p_model.name|upper).startswith('_SCHEMA_')) -%}
        select * from dual
    {%- elif (p_model.name[0] == '_') -%}
        select * from {{ ref(l_model_name) }}
    {%- else -%}
        select * from dual
    {%- endif -%}

{%- endmacro %}


{% macro get_rls_columns_expression(p_expression) -%}

    {{ return(adapter.dispatch('get_rls_columns_expression', 'sdc_distribute')(p_expression=p_expression)) }}

{%- endmacro %}

{% macro default__get_rls_columns_expression(p_expression) -%}

    {%- for l_column_name in var('sdc_distribute__available_rls_columns',[]) -%}
{{- p_expression | replace('#COLUMN_NAME#',l_column_name) -}}
    {%- endfor -%}

{%- endmacro %}


{% macro grant_access(p_database_name,p_object_name_list,p_grantee_list,p_rls_value_list=none) -%}

    {{ return(adapter.dispatch('grant_access', 'sdc_distribute')(
        p_database_name=p_database_name,
        p_object_name_list=p_object_name_list,
        p_grantee_list=p_grantee_list,
        p_rls_value_list=p_rls_value_list))
    }}

{%- endmacro %}

{% macro default__grant_access(p_database_name,p_object_name_list,p_grantee_list,p_rls_value_list=none) -%}

    {%- call statement('exec', fetch_result=False) -%}
    call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access$set
    (   
        p_dist_database_name => '{{ p_database_name }}',
        p_dist_object_name => {{ p_object_name_list }},
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => {{ p_grantee_list }},
        p_enabled => 'Y'
    )
    {%- endcall -%}

    {%- if p_rls_value_list and var('sdc_distribute__available_rls_columns',[]) -%}

        {%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access_rls$set
        (   
            p_dist_database_name => '{{ p_database_name }}',
            p_dist_object_name => {{ p_object_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_grantee => {{ p_grantee_list }},
            p_enabled => 'Y'
            {%- for l_column_name in var('sdc_distribute__available_rls_columns',[]) -%},
            p_{{ l_column_name | lower }} =>{{- ' ' -}}
                {%- if p_rls_value_list[loop.index-1] -%}
                    '{{- p_rls_value_list[loop.index-1] -}}'
                {%- else -%}
                    null
                {%- endif -%}
            {%- endfor %}
        )
        {%- endcall -%}

    {%- endif -%}    

{%- endmacro %}


{% macro disable_access(p_database_name,p_object_name_list,p_grantee_list,p_rls_value_list=none) -%}

    {{ return(adapter.dispatch('disable_access', 'sdc_distribute')(
        p_database_name=p_database_name,
        p_object_name_list=p_object_name_list,
        p_grantee_list=p_grantee_list,
        p_rls_value_list=p_rls_value_list))
    }}

{%- endmacro %}

{% macro default__disable_access(p_database_name,p_object_name_list,p_grantee_list,p_rls_value_list=none) -%}

    {%- if not(p_rls_value_list and var('sdc_distribute__available_rls_columns',[])) -%}

        {%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access$set
        (   
            p_dist_database_name => '{{ p_database_name }}',
            p_dist_object_name => {{ p_object_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_grantee => {{ p_grantee_list }},
            p_enabled => 'N'
        )
        {%- endcall -%}

    {%- else -%}

        {%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access_rls$set
        (   
            p_dist_database_name => '{{ p_database_name }}',
            p_dist_object_name => {{ p_object_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_grantee => {{ p_grantee_list }},
            p_enabled => 'N'
            {%- for l_column_name in var('sdc_distribute__available_rls_columns',[]) -%},
            p_{{ l_column_name | lower }} =>{{- ' ' -}}
                {%- if p_rls_value_list[loop.index-1] -%}
                    '{{- p_rls_value_list[loop.index-1] -}}'
                {%- else -%}
                    null
                {%- endif -%}
            {%- endfor %}
        )
        {%- endcall -%}

    {%- endif -%}    

{%- endmacro %}


{% macro revoke_access(p_database_name,p_object_name_list,p_grantee_list,p_rls_value_list=none) -%}

    {{ return(adapter.dispatch('revoke_access', 'sdc_distribute')(
        p_database_name=p_database_name,
        p_object_name_list=p_object_name_list,
        p_grantee_list=p_grantee_list,
        p_rls_value_list=p_rls_value_list))
    }}

{%- endmacro %}

{% macro default__revoke_access(p_database_name,p_object_name_list,p_grantee_list,p_rls_value_list=none) -%}

    {%- if not(p_rls_value_list and var('sdc_distribute__available_rls_columns',[])) -%}

        {%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access$remove
        (   
            p_dist_database_name => '{{ p_database_name }}',
            p_dist_object_name => {{ p_object_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_grantee => {{ p_grantee_list }}
        )
        {%- endcall -%}

    {%- else -%}

        {%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access_rls$remove
        (   
            p_dist_database_name => '{{ p_database_name }}',
            p_dist_object_name => {{ p_object_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_grantee => {{ p_grantee_list }}
            {%- for l_column_name in var('sdc_distribute__available_rls_columns',[]) -%},
            p_{{ l_column_name | lower }} =>{{- ' ' -}}
                {%- if p_rls_value_list[loop.index-1] -%}
                    '{{- p_rls_value_list[loop.index-1] -}}'
                {%- else -%}
                    null
                {%- endif -%}
            {%- endfor %}
        )
        {%- endcall -%}

    {%- endif -%}    

{%- endmacro %}

{# Call example:
   dbt run-operation sdc_distribute.wipe_out_model --args '{p_model_name_list: [STG_GEN_CASE_RLS]}'
#}
{% macro wipe_out_model(p_model_name_list) -%}

    {{ return(adapter.dispatch('wipe_out_model', 'sdc_distribute')(p_model_name_list=p_model_name_list)) }}

{%- endmacro %}

{% macro default__wipe_out_model(p_model_name_list) -%}

    {{ log( 'Processing SDC Distribute objects', info=True) }}
    {%- for l_dist_database_name in ['DISTRIBUTE','DISTRIBUTE_SF'] -%}            
        
        {%- call statement('exec', fetch_result=True) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$get_selected_objects
        ( 
            p_dist_database_name => '{{ l_dist_database_name }}',
            p_dist_object_name => {{ p_model_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_action => 'D',
            p_invocation_id => '{{ invocation_id }}'
        )
        {%- endcall -%}

        {%- set l_selected_objects_result = load_result('exec') -%}
        {%- set l_selected_objects_table = l_selected_objects_result['table'] -%}
        {%- set l_selected_objects_json = l_selected_objects_table.columns[0].values()[0] -%}
        {%- set l_selected_objects_count = fromjson(l_selected_objects_json) | length -%}

        {% if l_selected_objects_count == 0 %}
            {{ log( l_dist_database_name ~ ': No Deleting is necessary', info=True) }}
        {% elif l_selected_objects_count == 1 %}
            {{ log( l_dist_database_name ~ ': Deleting one object', info=True) }}
        {% else %}
            {{ log( l_dist_database_name ~ ': Deleting ' ~ l_selected_objects_count ~ ' objects', info=True) }}
        {% endif %}

        {%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access$remove
        (
            p_dist_database_name => '{{ l_dist_database_name }}',
            p_dist_object_name => {{ p_model_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_grantee => ['%']
        )
        {%- endcall -%}

        {% if l_selected_objects_count > 0 %}
            {%- call statement('exec', fetch_result=False) -%}
            call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$set_framework_objects
            ( 
                p_selected_objects_json => '{{ l_selected_objects_json }}',
                p_action => 'D'
            )
            {%- endcall -%}

        {%- endif -%}

        {#%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$rebuild_framework_objects
        ( 
            p_dist_database_name => '{{ l_dist_database_name }}',
            p_dist_object_name => {{ p_model_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%'],
            p_action => 'D'
        )
        {%- endcall -%#}

    {%- endfor -%}

    {%- do sdc_distribute.on_run_end_operations() -%}

    {%- for l_dist_database_name in ['DISTRIBUTE','DISTRIBUTE_SF'] -%}

        {%- call statement('exec', fetch_result=False) -%}
        call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$remove
        (
            p_dist_database_name => '{{ l_dist_database_name }}',
            p_dist_object_name => {{ p_model_name_list }},
            p_src_schema => '%',
            p_src_object_name => ['%']
        )
        {%- endcall -%}

    {%- endfor -%}

    {{ log( 'Processing Database objects', info=True) }}

    {%- call statement('exec', fetch_result=False) -%}
    call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object$drop_objects
    (
        p_schema => '%',
        p_object_name => {{ p_model_name_list }}
    )
    {%- endcall -%}
    
{%- endmacro %}


{% macro tags_declare() -%}

    l_select_any_tag variant;
    l_select_all_tag variant;
    l_exclude_any_tag variant;
    l_exclude_all_tag variant;
    l_dist_object_name variant;

{%- endmacro %}

{% macro tags_body() -%}

    let cur_tags cursor for
    select
        array_agg(distinct regexp_substr(value, '^(select_any_tag:|tag:)(.*)',1,1,'e',2)) select_any_tag,
        array_agg(distinct regexp_substr(value, '^(select_all_tag:)(.*)',1,1,'e',2)) select_all_tag,
        array_agg(distinct regexp_substr(value, '^(exclude_any_tag:)(.*)',1,1,'e',2)) exclude_any_tag,
        array_agg(distinct regexp_substr(value, '^(exclude_all_tag:)(.*)',1,1,'e',2)) exclude_all_tag,
        array_agg(  
            distinct case when
                value not like 'tag:%' and
                value not like 'select_any_tag:%' and
                value not like 'select_all_tag:%' and
                value not like 'exclude_any_tag:%' and
                value not like 'exclude_all_tag:%'
            then value end
        ) dist_object_name
    from
        (select to_array(parse_json(?)) arr), lateral flatten(arr)
    ;
     
    open cur_tags using
    (
        p_dist_object_name::varchar
    );        
     
    for rec in cur_tags do        
        l_select_any_tag := rec.select_any_tag;
        l_select_all_tag := rec.select_all_tag;
        l_exclude_any_tag := rec.exclude_any_tag;
        l_exclude_all_tag := rec.exclude_all_tag;
        l_dist_object_name := rec.dist_object_name;
    end for;

    if (not (l_select_any_tag = [] and l_select_all_tag = [] and l_exclude_any_tag = [] and l_exclude_all_tag = [] ) and 
        l_dist_object_name = [] )
    then
        l_dist_object_name := '%';
    end if;

{%- endmacro %}

{% macro set_sdc_distribute__object_sel() -%}

    create or replace temporary table {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_sel as
    select
        *
    from
        {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object
    where
        dist_database_name like :p_dist_database_name escape '\\' and
        dist_object_name like any (select value from (select to_array(:l_dist_object_name) arr), lateral flatten(arr)) escape '\\' and
        src_schema like nvl(:p_src_schema,'%') escape '\\' and
        src_object_name like any (select value from (select to_array(nvl(:p_src_object_name,'%'::variant)) arr), lateral flatten(arr)) escape '\\' and
        (array_size(array_intersection(to_array(tags),to_array(:l_select_any_tag))) > 0 or :l_select_any_tag = [] ) and
        (array_size(array_intersection(to_array(tags),to_array(:l_select_all_tag))) = array_size(to_array(:l_select_all_tag)) or :l_select_all_tag = [] ) and
        (array_size(array_intersection(to_array(tags),to_array(:l_exclude_any_tag))) = 0 or :l_exclude_any_tag = [] ) and
        (array_size(array_intersection(to_array(tags),to_array(:l_exclude_all_tag))) < array_size(to_array(:l_exclude_all_tag)) or :l_exclude_all_tag = [] )
    ;

{%- endmacro %}


{% macro sync_access(p_database_name=none,p_object_name_list=none,p_grantee_list=none) -%}

    {{ return(adapter.dispatch('sync_access', 'sdc_distribute')(
        p_database_name=p_database_name,
        p_object_name_list=p_object_name_list,
        p_grantee_list=p_grantee_list))
    }}

{%- endmacro %}

{% macro default__sync_access(p_database_name=none,p_object_name_list=none,p_grantee_list=none) -%}

    {%- call statement('exec', fetch_result=False) -%}
    call {{ sdc_distribute.get_target_schema() ~ sdc_distribute.get_sdc_distribute_schema() }}.sdc_distribute__object_access$sync
    (   
        p_dist_database_name => '{{ p_database_name|default("DISTRIBUTE", true) }}',
        p_dist_object_name => {{ p_object_name_list|default(['%'], true) }},
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => {{ p_grantee_list|default(['%'], true) }}
    )
    {%- endcall -%}

{%- endmacro %}