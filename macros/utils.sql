{% macro array_intersect(p_array_1,p_array_2) -%}

    {{ return(adapter.dispatch('array_intersect', 'sdc_distribute')(p_array_1=p_array_1,
                                                                    p_array_2=p_array_2)) }}

{%- endmacro %}

{% macro default__array_intersect(p_array_1,p_array_2) -%}

    {%- set l_intersect = [] -%}
    {%- for l_element in p_array_1 -%}
        {%- if p_array_2 | select('equalto',l_element) | list | length > 0 -%}
            {%- do l_intersect.append(l_element) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(l_intersect) }}

{%- endmacro %}


{% macro array_minus(p_array_1,p_array_2) -%}

    {{ return(adapter.dispatch('array_minus', 'sdc_distribute')(p_array_1=p_array_1,
                                                                p_array_2=p_array_2)) }}

{%- endmacro %}

{% macro default__array_minus(p_array_1,p_array_2) -%}

    {%- set l_minus = p_array_1.copy() -%}
    {%- for l_element in p_array_1 -%}
        {%- if p_array_2 | select('equalto',l_element) | list | length > 0 -%}
            {%- do l_minus.remove(l_element) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(l_minus) }}

{%- endmacro %}


{% macro array_union(p_array_1,p_array_2) -%}

    {{ return(adapter.dispatch('array_union', 'sdc_distribute')(p_array_1=p_array_1,
                                                                p_array_2=p_array_2)) }}

{%- endmacro %}

{% macro default__array_union(p_array_1,p_array_2) -%}

    {%- set l_union = [] -%}
    {%- for l_element in p_array_1 -%}
        {%- if l_union | select('equalto',l_element) | list | length == 0 -%}
            {%- do l_union.append(l_element) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- for l_element in p_array_2 -%}
        {%- if l_union | select('equalto',l_element) | list | length == 0 -%}
            {%- do l_union.append(l_element) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(l_union) }}

{%- endmacro %}


{% macro node_attribute_contains(p_node,p_attribute_path,p_value) -%}

    {{ return(adapter.dispatch('node_attribute_contains', 'sdc_distribute')(p_node=p_node,
                                                                            p_attribute_path=p_attribute_path,
                                                                            p_value=p_value )) }}

{%- endmacro %}

{% macro default__node_attribute_contains(p_node,p_attribute_path,p_value) -%}

    {%- if not ( p_node is iterable and p_attribute_path is iterable and p_value is string ) -%}
        {{ return(false) }}
    {%- endif -%}

    {%- set ns_node = namespace(attribute=p_node) -%}

    {%- for l_attribute in p_attribute_path -%}
        {%- if ( not ns_node.attribute is iterable or ns_node.attribute is string ) -%}            
            {{ return(false) }}
        {%- elif ( ns_node.attribute | list | select("equalto",l_attribute) | list | length == 0 ) -%}
            {{ return(false) }}
        {%- else -%}
            {%- set ns_node.attribute = ns_node.attribute.get(l_attribute) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set l_attribute_list = ns_node.attribute -%}

    {%- if ( not l_attribute_list is iterable ) -%}
        {{ return(false) }}
    {%- elif ( l_attribute_list is string ) -%}
        {%- set l_attribute_list = [l_attribute_list] -%}
    {%- endif -%}    

    {%- for l_attribute in l_attribute_list if p_value in l_attribute -%}
        {{ return(true) }}
    {%- endfor -%}

    {{ return(false) }}

{%- endmacro %}
