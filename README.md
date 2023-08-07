This SDC Distribute package contains models and macros that wrap the Data Distribution configuration on Siemens Snowflake managed accounts.

----
# Contents

- [Installation](#installation)
- [Usage](#usage)
  - [D2GO Interface Configuration](#d2go-interface-configuration)
    - [sdc_distribute__d2go_exclude_environments](#sdc_distribute__d2go_exclude_environments)
    - [sdc_distribute__d2go_force_replace](#sdc_distribute__d2go_force_replace)
  - [Distribute Models](#distribute-models)
    - [Existing Model](#existing-model)
    - [New Model](#new-model)
    - [Schema](#schema)
    - [Distributing to other Snowflake accounts (sdc_distribute__distribution_scope)](#distributing-to-other-snowflake-accounts-sdc_distribute__distribution_scope)
    - [Set Restricted Access without RBAC activated (sdc_distribute__restricted_access)](#sdc_distribute__restricted_access)
  - [Configuration options for Discrete Distribution](#configuration-options-for-discrete-distribution)
    - [sdc_distribute__anonymized_columns](#sdc_distribute__anonymized_columns)
    - [sdc_distribute__rls_columns](#sdc_distribute__rls_columns)
    - [sdc_distribute__rls_all_values](#sdc_distribute__rls_all_values)
    - [sdc_distribute__distributed_model](#sdc_distribute__distributed_model)
  - [Standard Configuration options for Batch Distribution](#standard-configuration-options-for-batch-distribution)
    - [sdc_distribute__src_schema](#sdc_distribute__src_schema)
    - [sdc_distribute__src_path](#sdc_distribute__src_path)
    - [sdc_distribute__src_tags](#sdc_distribute__src_tags)
  - [Advanced Configuration options for Batch Distribution](#advanced-configuration-options-for-batch-distribution)
    - [sdc_distribute__src_select_any_tags](#sdc_distribute__src_select_any_tags)
    - [sdc_distribute__src_select_all_tags](#sdc_distribute__src_select_all_tags)
    - [sdc_distribute__src_exclude_any_tags](#sdc_distribute__src_exclude_any_tags)
    - [sdc_distribute__src_exclude_all_tags](#sdc_distribute__src_exclude_all_tags)
  - [Prepare Snowflake tables to use User Access RLS](#prepare-snowflake-tables-to-use-user-access-rls)
    - [Single RLS value columns](#single-rls-value-columns)
    - [Multiple RLS value columns](#multiple-rls-value-columns)
  - [Tags usage on Distributed Models](#tags-usage-on-distributed-models)
  - [Manage User Access](#manage-user-access)
    - [sdc_distribute__default_grantee_list](#sdc_distribute__default_grantee_list)
    - [Using dbt Macros](#using-dbt-macros)
    - [Using dbt Macros and Tag Filters](#using-dbt-macros-and-tag-filters)
    - [Using Snowflake Procedures](#using-snowflake-procedures)
    - [Using Snowflake Procedures and Tag Filters](#using-snowflake-procedures-and-tag-filters)
  - [External User Access Management](#external-user-access-management)
    - [sdc_distribute__external_access_mgnt](#sdc_distribute__external_access_mgnt)
    - [sdc_distribute__external_rls_access_mgnt](#sdc_distribute__external_rls_access_mgnt)
    - [sdc_distribute__external_sf_access_mgnt](#sdc_distribute__external_sf_access_mgnt)
    - [sdc_distribute__external_sf_rls_access_mgnt](#sdc_distribute__external_sf_rls_access_mgnt)
  - [Clean Up](#clean-up)
    - [Remove Models Using dbt Macros](#remove-models-using-dbt-macros)
    - [Remove Models Using Snowflake Procedures](#remove-models-using-snowflake-procedures)
  - [Snowsight Dashboard](#snowsight-dashboard)
  - [RBAC Migration](#migration-to-rbac)
  - [Fix RBAC grants if necessary](#fix-rbac-grants-if-necessary)

# Installation
In order to use this package in your dbt Cloud project, you need to add it to the `packages.yml` file.  
**Ensure that you are using the correct revision**

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc-distribute.git" # git URL
    revision: "v0.1.8" # get the revision from the Release Notes
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute/-/releases
```

After configuring the package location and revision, get it into your project.
Execute the following command in the dbt command line
```
dbt deps
```

The next step is to configure vars on your dbt_project.yml file. 
The following variables are necessary:
 - schema - where the sdc_distribute objects are installed (default value is **sdc_distribute** )
 - available_rls_columns - indicates what columns are going to be available to use on the rls access configurations

`dbt_project.yml`
```yaml
vars:
    # sdc_distribute vars
    sdc_distribute__schema: my_sdc_distribute_schema
    sdc_distribute__available_rls_columns: [rls_my_column_1,rls_my_column_2,rls_my_column_3]
```

Deploy the sdc_distribute models into the database. 
Execute the following command in the dbt command line
```
dbt run --select sdc_distribute
```

The following objects are going to be created on the configured schema:

### Tables
- SDC_DISTRIBUTE__OBJECT
- SDC_DISTRIBUTE__OBJECT_ACCESS
- SDC_DISTRIBUTE__OBJECT_ACCESS_RLS 

### Views
- SDC_DISTRIBUTE__D2GO_ACCESS_MGNT
- SDC_DISTRIBUTE__D2GO_RLS_ACCESS_MGNT
- SDC_DISTRIBUTE__D2GO_SF_ACCESS_MGNT
- SDC_DISTRIBUTE__D2GO_SF_RLS_ACCESS_MGNT


### Procedures
- SDC_DISTRIBUTE__OBJECT$SET
- SDC_DISTRIBUTE__OBJECT$REMOVE
- SDC_DISTRIBUTE__OBJECT$REBUILD_FRAMEWORK_OBJECTS
- SDC_DISTRIBUTE__OBJECT$DROP_OBJECTS
- SDC_DISTRIBUTE__OBJECT_ACCESS$SET
- SDC_DISTRIBUTE__OBJECT_ACCESS$REMOVE
- SDC_DISTRIBUTE__OBJECT_ACCESS_RLS$SET
- SDC_DISTRIBUTE__OBJECT_ACCESS_RLS$REMOVE

# Usage

## D2GO Interface Configuration

The SDC Distribute calls D2GO procedures to create the views and configure the access to the objects on DISTRIBUTE and DISTRIBUTE_SF databases.
This interface is only called if there are changes on the model columns or configuration (anonymized columns, rls_columns).
In order to make sure that the interface only when necessary there are 2 variables that control those calls.

`dbt_project.yml`
```yaml
vars:
    # sdc_distribute vars
    sdc_distribute__d2go_exclude_environments: ['default'] # Assuming that the jobs have the target name different from 'default'
    sdc_distribute__d2go_force_replace: false
```

This example is going to stop the dbt processes from creating views on the D2GO distribute databases when we are using the Development environment.
The false value for **force replace** is the default one, so no need to be on the dbt_project file.

If the jobs (defined on the deployment environments) do not change the 'default' target name to another one, then the objects on the distribute databases will not be created.

### sdc_distribute__d2go_exclude_environments
The dbt enviroments defined on a project usually are the following:
- default - is the one defined on the user's profile under the name **Target Name**
- DEV, QUA, PRD - are the **Target Name** used on jobs on each environment. Usually the **Target Name** is set to match the environement **Name**

### sdc_distribute__d2go_force_replace
If we want to rebuild the DISTRIBUTE / DISTRIBUTE_SF views even when there are no changes to trigger it, we can use the vars from the command line.
If the change is more permanent we can change the vars on the dbt project file.

```
dbt run --select _my_model --vars '{"sdc_distribute__d2go_force_replace":true}'
```

One of the multiple calls on the database is going to be following procedure, where the FR (force replace) parameter is passed

`Snowflake Worksheet`
```sql
call sdc_distribute.sdc_distribute__object$rebuild_framework_objects
( 
    p_dist_database_name => 'DISTRIBUTE',
    p_dist_object_name => ['MY_MODEL'],
    p_src_schema => 'STAGING',
    p_src_object_name => ['MY_MODEL'],
    p_action => 'FR'
)
```

## Distribute Models

This package's main feature is to streamline the models data distribution.

It is possible to distribute either to the **DISTRIBUTE** or **DISTRIBUTE_SF** databases. The **DISTRIBUTE_SF** is only used in very specific use cases, when we need to share data with other Snowflake accounts like Energy or Healthineers. 

The first step, is to define a new folder below the Models on the dbt project tree and also on dbt_project.yml. This is the place to keep the models that will be distributed.

As an example we use the name **Distribute**.

Configure that folder in the dbt_project file.

`dbt_project.yml`
```yaml
models:
    my_project:
        distribute:
            schema: distribute
            post-hook: "{{ sdc_distribute.post_hook_operations() if execute }}"
```

### Existing Model
In order to distribute an existing model, create a new model inside **Distribute**.

The new model must have the same name as the model to be distributed, **prefixed with an underscore (_)**.

As an example we use the name **test_model**.

`models/distribute/_test_model.sql`
```
{{ sdc_distribute.get_distribute_sql() }}
```

In order to test your model and check what statements are being issued on the database, execute

```
dbt run --select _test_model
```

### New Model

If the model to be distributed has it's own definition then the name **can't begin with underscore (_)**.

`models/distribute/new_model.sql`
```sql
select
    1 example_column
```

In order to test your model and check what statements are being issued on the database please execute
```
dbt run --select new_model
```

### Schema
This is used to distribute all the tables existing under a database schema. There are multiple filters that can be apllied in order to select the models to be distributed. The base attributes used on those filters are:
    - schema - e.g "staging" - the database schema where the objects to be distributed are implemented
    - model path - e.g "staging.source_1" - the dbt folder that contains the models to be distributed
    - tags - e.g ["tag_1","tag_2"] - the tags assigned to the objects to be distributed that will be used to select which models are part of the **Batch**

The new file name must use the **prefix \_schema\_**. There are two options to set the schema name:
    - using the config **sdc_distribute__src_schema**
    - when the config is not present then the string after the **prefix \_schema\_** is used to set the schema name. the file \_schema\_my_schema, sets the schema_name as **my_schema"

When the schema model is used, **no other configuration is taken into account**.

As an example we use the name **my_schema**.

`models/distribute/_schema_my_schema.sql`
```sql
{{ sdc_distribute.get_distribute_sql() }}
```

In order to test your model and check what statements are being issued on the database please execute
```
dbt run --select _schema_my_schema
```

There are 2 dedicated sections explaining the configuration parameters on this document:

- [Standard Configuration options for Batch Distribution](#standard-configuration-options-for-batch-distribution)
- [Advanced Configuration options for Batch Distribution](#advanced-configuration-options-for-batch-distribution)

### Distributing to other Snowflake accounts (sdc_distribute__distribution_scope)
In order to distribute to other Snowflake accounts the model has to set the configuration to one of the following values
 - external - the models will be created using the **DISTRIBUTE_SF** dist_database_name
 - both - the models will create two configurations, one using the **DISTRIBUTE** dist_database_name and another for **DISTRIBUTE_SF**

`models/distribute/_test_model.sql`
```
{{
    config(
        sdc_distribute__distribution_scope = 'external'
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

### sdc_distribute__restricted_access
If a model is being distributed in a schema that does not have RBAC activated, this config allows to use the previous aproach to access security,
using the Object Access configurations. This is implemented creating a view on the Distribute schema before distributing it to the Distribute Layer.

In order to activate this configuration follow the example below.

`models/distribute/_test_model.sql`
```
{{
    config(
        sdc_distribute__restricted_access = true
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```


## Configuration options for Discrete Distribution

The models can be configured using the ususal **dbt Model Configuration** mechanism. This is, by using:
- config macro on the model file
- configuration on the dbt_project yaml file

The (\_schema\_) models will ignore the configuration.

The available configurations are the following:
- sdc_distribute__anonymized_columns
- sdc_distribute__rls_columns
- sdc_distribute__rls_all_values
- sdc_distribute__distributed_model (options: auxiliary,base)

### sdc_distribute__anonymized_columns

This configuration takes the list of columns (comma separated) that will be anonymized by the D2GO base package.

Following is an example how we can set the configuration on the model file.

`models/distribute/_test_model.sql`
```
{{
    config(
        sdc_distribute__anonymized_columns = 'column_with_sensitive_data,another_column_to_be_anonymized'
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```


Alternatively we can use the dbt_ptoject file to set the same configuration. The example is done at the model level but it can also be done on any other level.

`dbt_project.yml`
```yaml
models:
    my_project:
        distribute:
            schema: distribute
            post-hook: "{{ sdc_distribute.post_hook_operations() if execute }}"
            _test_model:
                sdc_distribute__anonymized_columns: column_with_sensitive_data,another_column_to_be_anonymized
```


### sdc_distribute__rls_columns

This configuration takes the list of columns (comma separated) that will be used to set the RLS (Row Level Security).
The following assumptions are taken:
- These columns are a subset of the ones configured on the project vars **sdc_distribute__available_rls_columns**
- These columns are available on the current model


Following is an example how we can set the configuration on the model file.

`models/distribute/_test_model.sql`
```
{{
    config(
        sdc_distribute__rls_columns = 'rls_my_column_1,rls_my_column_2'
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

Alternatively we can use the dbt_ptoject file to set the same configuration. The example is done at the model level but it can also be done on any other level.

`dbt_project.yml`
```yaml
models:
    my_project:
        distribute:
            schema: distribute
            post-hook: "{{ sdc_distribute.post_hook_operations() if execute }}"
            _test_model:
                sdc_distribute__rls_columns: rls_my_column_1,rls_my_column_2
```

For the time being, the RLS security is implemented using a view created on the **Distribute** schema. It uses the name **DISTRIBUTE_<distributed_model_name>**.

### sdc_distribute__rls_all_values

This configuration defines for each rls_column the **ALL_VALUE** that will mark the rows that use it as **Public**, that is, it will be visible to all consumers.
The config uses a list of values, one element to each column, **in the same order** as the one defined on **sdc_distribute__rls_columns**.

The following special values can be used:
- #NONE# - this can be used as a filler, it means that there is no **ALL_VALUE** on that rls_column
- #NULL# - this special value must be used if we need to set the **ALL_VALUE** as null. This will create the condition **RLS_COLUMN IS NULL** on the RLS view

`models/distribute/_test_model.sql`
```
{{
    config(
        sdc_distribute__rls_columns = 'rls_my_column_1,rls_my_column_2,rls_my_column_3',
        sdc_distribute__rls_all_values = ['#NONE#','#NULL#','My All Value']
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```


### sdc_distribute__distributed_model

This configuration is only applicable for models using a name starting with underscore(_). It takes one of the following values:
- auxiliary
- base

When the **auxiliary** value is used the view created on the **Distribute** schema is used on the D2GO distribute process instead of the base model.

Following is an example how we can set the configuration on the model file.

`models/distribute/_test_model.sql`
```
{{
    config(
        sdc_distribute__distributed_model = 'auxiliary'
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

Alternatively we can use the dbt_ptoject file to set the same configuration. The example is done at the model level but it can also be done on any other level.

`dbt_project.yml`
```yaml
models:
    my_project:
        distribute:
            schema: distribute
            post-hook: "{{ sdc_distribute.post_hook_operations() if execute }}"
            _test_model:
                sdc_distribute__distributed_model: auxiliary
```

## Standard Configuration options for Batch Distribution

The models can be configured using the ususal **dbt Model Configuration** mechanism. This is, by using:
- config macro on the model file
- configuration on the dbt_project yaml file

The **Batch Distribution** models are defined on files with the prefix (\_schema\_).
This distributes models defined on other files using the **config** definition to set the filters of that **Batch**.

The available standard configurations are the following:
- sdc_distribute__src_schema
- sdc_distribute__src_path
- sdc_distribute__src_tags

There is another section explaining additional filters that can also be used:
- [Advanced Configuration options for Batch Distribution](#advanced-configuration-options-for-batch-distribution)

### sdc_distribute__src_schema

This configuration defines the source schema name from where the models will be selected. If this config is not defined then the src_schema is derived from the file_name using the rule \_schema\_<src_schema>. This means that the src_schema will always be defined.

`models/distribute/_schema_my_distribute_batch.sql`
```
{{
    config(
        sdc_distribute__src_schema = 'staging'
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

### sdc_distribute__src_path

This configuration is used to filter only the models defined under a specific folder or path. The used level separator is the dot(.).

`models/distribute/_schema_my_distribute_batch.sql`
```
{{
    config(
        sdc_distribute__src_schema = 'staging',
        sdc_distribute__src_path = 'staging.my_source_1'
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```


### sdc_distribute__src_tags

`models/distribute/_schema_my_distribute_batch.sql`
```
{{
    config(
        sdc_distribute__src_schema = 'staging',
        sdc_distribute__src_path = 'staging.my_source_1',
        sdc_distribute__src_tags = ['tag_1','tag_2']
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

## Advanced Configuration options for Batch Distribution

Additionally to the standard filters, there is a set of advanced filters that can be used to implement complex use cases not solveable by that set of filters.

The available advanced configurations are the following:
- sdc_distribute__src_select_any_tags
- sdc_distribute__src_select_all_tags
- sdc_distribute__src_exclude_any_tags
- sdc_distribute__src_exclude_all_tags

The filter include select and exclude options for tag filters, similar to the dbt command line options. The **select** is used to set the list of tags that a model must be assigned to, in order to be part of the current **Distribution Batch**. The **exclude** options are used to indicate which tags cannot be assigned to a model so that model is part of this **Distribution Batch**.

The **any** options (sdc_distribute__src_select_any_tags,sdc_distribute__src_exclude_any_tags) work in the same way as the **--select model_1 model_2** option on the dbt command line. That is, if a model has one **or** the other tag, then the rule is applied.

The **all** options (sdc_distribute__src_select_all_tags,sdc_distribute__src_exclude_all_tags) work in the same way as the **--select model_1,model_2** option on the dbt command line. That is, if a model has one **and** the other tag, then the rule is applied.

The options can all be applied on the same model, at the same time. Please bare in mind this is pure logic, so it is possible that the rules' combination result on an empty data set.

 :warning: **The models distributed using the Discrete method (_<base_model>) are removed from the result set**

 A good use case to the **Batch Distribution** is to use it as default for a group of models (database schema and dbt path). The exceptions can then be processes as **Discrete Distriution** models.

### sdc_distribute__src_select_any_tags

This config is the same as **sdc_distribute__src_tags**. It takes the list of tags and finds all models that are assigned to any of those tags.

`models/distribute/_schema_my_distribute_batch.sql`
```
{{
    config(
        sdc_distribute__src_schema = 'staging',
        sdc_distribute__src_path = 'staging.my_source_1',
        sdc_distribute__src_select_any_tags = ['tag_1','tag_2']
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```


### sdc_distribute__src_select_all_tags

This config differs from the **sdc_distribute__src_select_any_tags** as the models must have all of the tags on the list and not only one of them. 

`models/distribute/_schema_my_distribute_batch.sql`
```
{{
    config(
        sdc_distribute__src_schema = 'staging',
        sdc_distribute__src_path = 'staging.my_source_1',
        sdc_distribute__src_select_all_tags = ['tag_1','tag_2']
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

### sdc_distribute__src_exclude_any_tags

This exclude config selects all models that do not have any of the tags. This is, it takes the presence of any of the tags to exclude the model from the Batch.

`models/distribute/_schema_my_distribute_batch.sql`
```
{{
    config(
        sdc_distribute__src_schema = 'staging',
        sdc_distribute__src_path = 'staging.my_source_1',
        sdc_distribute__src_exclude_any_tags = ['tag_1','tag_2']
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

### sdc_distribute__src_exclude_all_tags


This config selects all models that are not assigned to all of the tags. This is, only if the model is assigned to all of the tags, it's going to be excluded.

`models/distribute/_schema_my_distribute_batch.sql`
```
{{
    config(
        sdc_distribute__src_schema = 'staging',
        sdc_distribute__src_path = 'staging.my_source_1',
        sdc_distribute__src_exclude_all_tags = ['tag_1','tag_2']
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

## Prepare Snowflake tables to use User Access RLS

User Access RLS matches the values existing on specific columns (defined using **sdc_distribute__rls_columns**) with the access configuration set using the features on section [Manage User Access](#manage-user-access), specifically the RLS configuration.

There are two steps on this process:
-  create the RLS columns on your model - let's imagine that this creates "locks" on your rows
-  grant the RLS user access to the users / roles - this will give them the "keys" to open the "locked" rows

Most of the times each row will have only one RLS value assigned.

### Single RLS value columns

This is the most common scenario when setting up the "locks" on a row. That specific row is assigned to a specific value on each of the RLS columns.
For example a specific Sales Order row will be assigned to a single Division and a single Sales Country.
All the users that have access to that "Division/Sales Country" combination will be able to access to that row.

### Multiple RLS value columns

When we want a row to be accessible to more than one value then we can use a **VARIANT** column and use an array to indicate all the values that should have access to that row.
Following the previous example, if we need that a row is accessible to both Divisions SDI and SSI then on that row we must use **['SDI','SSI']** as the value on the RLS_DIVISION column.

## Tags usage on Distributed Models  

The distribution process sets on the database metadata the tags that are assigned to the distribution models, being them Discrete or Batch.
It also sets a tag with the path of the source models:
- Discrete Distribution - the base model current path
- Batch Distribution - the value on the config **sdc_distribute__src_path**

This information can then be used as filter when granting or revoking access.

## Manage User Access

After the data model setup, including the models distribution to the **<environment>_DISTRIBUTE** database it is necessary to manage the user accesses.

This is done using the tables created on the schema informed on the variable **sdc_distribute__schema**:
- SDC_DISTRIBUTE__OBJECT_ACCESS
- SDC_DISTRIBUTE__OBJECT_ACCESS_RLS 

:warning: **It is not wise to manipulate the data directly on the table with DDL statements. The procedure calls make sure that the data coherence is not compromised**

The package offers two ways to manipulate the data:
- using dbt macros
- using Snowflake procedures

### sdc_distribute__default_grantee_list

When the models are distributed, by default, access is granted to the distributing project itself.
This config gives us the opportunity to override that behaviour and distribute automatically to a list of projects.
That list can even be empty and no access is given, not even to the project itself.

`models/distribute/<my_model>.sql`
```
{{
    config(
        sdc_distribute__default_grantee_list = ['<project_1>_DISTRIBUTE','<project_2>_DISTRIBUTE']
    )
}}

{{ sdc_distribute.get_distribute_sql() }}
```

If you want to set the list of grantees to all models under a folder you can use the definitions on dbt_project

`dbt_project.yml`
```
models:
  <my_project>:
    distribute:
      +sdc_distribute__default_grantee_list: ['<project_1>_DISTRIBUTE','<project_2>_DISTRIBUTE']

```

### Using dbt Macros

`dbt Scratchpad`
```
    {% do sdc_distribute.grant_access
        (
                p_database_name = 'DISTRIBUTE',
                p_object_name_list = ['TEST_MODEL','TEST_MODEL2'],
                p_grantee_list = ['PROJECT1_DISTRIBUTE','PROJECT1_DISTRIBUTE'],
                p_rls_value_list = ['DE','SDI',null]
        )
    %}
```

`dbt Scratchpad`
```
    {% do sdc_distribute.disable_access
        (
                p_database_name = 'DISTRIBUTE',
                p_object_name_list = ['TEST_MODEL','TEST_MODEL2'],
                p_grantee_list = ['PROJECT1_DISTRIBUTE','PROJECT1_DISTRIBUTE'],
                p_rls_value_list = ['DE','SDI',null]
        )
    %}
```

`dbt Scratchpad`
```
    {% do sdc_distribute.revoke_access
        (
                p_database_name = 'DISTRIBUTE',
                p_object_name_list = ['TEST_MODEL','TEST_MODEL2'],
                p_grantee_list = ['PROJECT1_DISTRIBUTE','PROJECT1_DISTRIBUTE'],
                p_rls_value_list = ['DE','SDI',null]
        )
    %}
```

### Using dbt Macros and Tag Filters

This extends the capability of filtering models by prefixing the **p_object_name_list** elemens with one of the following keywords:
- tag:
- select_any_tag:
- select_all_tag:
- exclude_any_tag:
- exclude_all_tag:

The logic applied is equivalent to the one used on the dbt command line also with tags or with the rules explained on the section
- [Advanced Configuration options for Batch Distribution](#advanced-configuration-options-for-batch-distribution)


`dbt Scratchpad`
```
    {% do sdc_distribute.grant_access
        (
                p_database_name = 'DISTRIBUTE',
                p_object_name_list = ['select_any_tag:TAG_1','select_any_tag:TAG_2'],
                p_grantee_list = ['PROJECT1_DISTRIBUTE','PROJECT1_DISTRIBUTE'],
                p_rls_value_list = ['DE','SDI',null]
        )
    %}
```

### Using Snowflake Procedures

The available procedures are:
- sdc_distribute.sdc_distribute__object_access$set
- sdc_distribute.sdc_distribute__object_access$remove

- sdc_distribute.sdc_distribute__object_access_rls$set
- sdc_distribute.sdc_distribute__object_access_rls$remove

The parameters used on the procedures are the following:
- p_dist_database_name - 'DISTRIBUTE' - for the time being this is the only value possible
- p_dist_object_name - ['TEST_MODEL', 'TEST_MODEL2'] - this is the list of distributed objects that we want to set the user access
- p_src_schema - '%' - change to a specific value if we want to use the source schema as filter (to grant access to all the base objects from a specific schema)
- p_src_object_name - ['%'] - list of base models to grant access
- p_grantee - ['PROJECT1_DISTRIBUTE', 'PROJECT2_DISTRIBUTE'] - to whom we want to grant access
- p_enabled - 'Y' - if we want to disable a user access temporarily this flag can be used
- p_rls_<attribute> - this parameters are only used on the RLS procedures. It sets the values on each one of the columns used on the RLS (row level security)

`Snowflake Worksheet`
```sql
call sdc_distribute.sdc_distribute__object_access$set
    (   
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['TEST_MODEL', 'TEST_MODEL2'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => ['PROJECT1_DISTRIBUTE', 'PROJECT2_DISTRIBUTE'],
        p_enabled => 'Y'
    )
```

`Snowflake Worksheet`
```sql
call sdc_distribute.sdc_distribute__object_access_rls$set
    (   
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['TEST_MODEL', 'STG_S4S_SISIC'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => ['CRM_DISTRIBUTE', 'DOM_SALES_DISTRIBUTE'],
        p_enabled => 'Y',
        p_rls_my_column_1 => 'DE',
        p_rls_my_column_2 => 'SDI',
        p_rls_my_column_3 => null
    )
```

`Snowflake Worksheet`
```sql
call sdc_distribute.sdc_distribute__object_access$remove
    (   
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['TEST_MODEL', 'TEST_MODEL2'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => ['PROJECT1_DISTRIBUTE', 'PROJECT2_DISTRIBUTE']
    )
```

`Snowflake Worksheet`
```sql
call sdc_distribute.sdc_distribute__object_access_rls$remove
    (   
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['TEST_MODEL', 'STG_S4S_SISIC'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => ['CRM_DISTRIBUTE', 'DOM_SALES_DISTRIBUTE']
        p_rls_my_column_1 => 'DE',
        p_rls_my_column_2 => 'SDI',
        p_rls_my_column_3 => null
    )
```

### Using Snowflake Procedures and Tag Filters

This extends the capability of filtering models by prefixing the **p_object_name_list** elemens with one of the following keywords:
- tag:
- select_any_tag:
- select_all_tag:
- exclude_any_tag:
- exclude_all_tag:

The logic applied is equivalent to the one used on the dbt command line also with tags or with the rules explained on the section
- [Advanced Configuration options for Batch Distribution](#advanced-configuration-options-for-batch-distribution)


`Snowflake Worksheet`
```sql
call sdc_distribute.sdc_distribute__object_access$set
    (   
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['select_any_tag:TAG_1','select_any_tag:TAG_2'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => ['PROJECT1_DISTRIBUTE', 'PROJECT2_DISTRIBUTE'],
        p_enabled => 'Y'
    )
```
## External User Access Management

If you are using an external process to manage your user access and rls (row-level security) like an Application Access Management you can still use the SDC Distribute package to manage and create your secured models. The models are created and distributed in the same way as when the user access is managed internally.
The change is on the User Access and User Access RLS metadata. To configure the external objects that should be used instead of the internal ones, please use:
- sdc_distribute__external_access_mgnt
- sdc_distribute__external_rls_access_mgnt
- sdc_distribute__external_sf_access_mgnt
- sdc_distribute__external_sf_rls_access_mgnt

Following is an example how we can set the configuration on the dbt_project file.

`dbt_project.yml`
```
vars:
  sdc_distribute__schema: sdc_distribute
  sdc_distribute__available_rls_columns: [rls_column_1,rls_column_2,rls_column_3,rls_column_4]
  sdc_distribute__external_access_mgnt: '"DEV_PROJECT_1"."PUBLIC"."EXTERNAL_ACCESS_MGNT"'
  sdc_distribute__external_rls_access_mgnt: '"DEV_PROJECT_1"."PUBLIC"."EXTERNAL_RLS_ACCESS_MGNT"'
  sdc_distribute__external_sf_access_mgnt: '"DEV_PROJECT_1"."PUBLIC"."EXTERNAL_SF_ACCESS_MGNT"'
  sdc_distribute__external_sf_rls_access_mgnt: '"DEV_PROJECT_1"."PUBLIC"."EXTERNAL_SF_RLS_ACCESS_MGNT"'
```

### sdc_distribute__external_access_mgnt

This configuration sets the location of the object that will be used to define the DISTRIBUTE's objects access.

The object must implement at least the following columns:
- user_or_role - project name foloowed by '_DISTRIBUTE'. Example : PROJECT_1_DISTRIBUTE
- view_name - distributed object name. Example: "DEV_DISTRIBUTE"."PROJECT_1"."OBJECT_1"
- active - must be either 'X' (if it's active) or null

### sdc_distribute__external_rls_access_mgnt

This configuration sets the location of the object that will be used to define the DISTRIBUTE's objects access rls.

The object must implement at least the following columns:
- user_or_role - project name foloowed by '_DISTRIBUTE'. Example : PROJECT_1_DISTRIBUTE
- view_name - distributed object name. Example: "DEV_DISTRIBUTE"."PROJECT_1"."OBJECT_1"
- rls_column_1 .. rls_column_n - columns declared on the variable sdc_distribute__available_rls_columns
    - these columns values define which row "locks" they open. There is a special value that can be used:
        - null (no value) - it means that no filter is applied on this column. A row with all rls_columns set to null gives access to all rows

### sdc_distribute__external_sf_access_mgnt

The object must implement at least the following columns:
- account - account name followed by '_SF_ACCOUNT'. Example : ACCOUNT_1_SF_ACCOUNT as described on [How does data distribution across Snowflake accounts work in Snowflake main account](https://wiki.siemens.com/pages/viewpage.action?pageId=367797299)
- view_name - distributed object name. Example: "DEV_DISTRIBUTE"."PROJECT_1"."OBJECT_1"
- active - must be either 'X' (if it's active) or null

### sdc_distribute__external_sf_rls_access_mgnt

The object must implement at least the following columns:
- account - account name followed by '_SF_ACCOUNT'
- view_name - distributed object name. Example: "DEV_DISTRIBUTE_SF"."PROJECT_1"."OBJECT_1"
- rls_column_1 .. rls_column_n - columns declared on the variable sdc_distribute__available_rls_columns
    - these columns values define which row "locks" they open. There is a special value that can be used:
        - null (no value) - it means that no filter is applied on this column. A row with all rls_columns set to null gives access to all rows


## Clean Up

When a model is removed from the dbt project it leaves behind three types of orphaned objects:
- the object on the **DISTRIBUTE** database if it exists
- the base object on the database
- the metadata used to manage the aser access

To clean up those objects, this package offers two alternative solutions:
- using dbt macros
- using Snowflake procedures

### Remove Models Using dbt Macros

Using dbt gives us two benefits over the Snowflake procedures: 
- controlled deployment - we will execute the correct scripts on the database without editing them between instances
- source control - if we use the **deployment macro method**, the executed code will be traceable 

**Direct run-operation**

`dbt command line`
```
dbt run-operation sdc_distribute.wipe_out_model --args '{p_model_name_list: [STG_GEN_CASE_RLS]}'
```

**Deployment Macro Method**

In order to be able to do version control on this operation, it is wise to use a macro where the current changes to the metadata (like user access) are stated. It will be deployed with the rest of the project and executed with a run-operation.

`macros/deploy_db_changes.sql`
```
{% macro deploy_db_changes() -%}

    {%- do sdc_distribute.wipe_out_model(['TEST_MODEL']) -%}

{%- endmacro %}
```

`dbt command line`
```
dbt run-operation deploy_db_changes
```

### Remove Models Using Snowflake Procedures

This option should only be used on database centric process automation.

When the sdc_distribute tables are installed, the procedures that manipulate their data are also installed.

The order of execution is very important, so please do it in the same sequence as shown in the example.


`Snowflake Worksheet`
```
call sdc_distribute.sdc_distribute__object_access$remove
    (
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['TEST_MODEL'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => ['%']
    )
  ;

call sdc_distribute.sdc_distribute__object$rebuild_framework_objects
    ( 
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['TEST_MODEL'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_action => 'D'
    )
  ;

call sdc_distribute.sdc_distribute__object$remove
    (
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['TEST_MODEL'],
        p_src_schema => '%',
        p_src_object_name => ['%']
    )
  ;

call sdc_distribute.sdc_distribute__object$drop_objects
    (
        p_schema => '%',
        p_object_name => ['TEST_MODEL']
    )
;
```

## Snowsight Dashboard

To easily have an overview of the user access we can build a dashboard using a reporting tool.
The suggestion is to use Snowsight. Following are three queries to check the status on each level of sdc_distribute configuration:
- Distribute Objects
- Distribute User Access
- Distribute Row Level Security

There are some adaptations necessary to suit to each one of the projects, environments, etc. Replace the \<tags\> with the correct values for your case.
The SDC_DISTRIBUTE schema is the default value. If you have changed this configuration, please adapt also that value.

`Distribute Objects`
```sql
select
    dist_database_name "Distribute Database",
    dist_object_name "Distribute Object",
    case
        when base.table_schema is null then 'Base Object missing'
        when dist.table_schema is null then 'Object pending deployment in Distribute Database'
        when dist.last_altered < base.last_altered then 'Base Object changed after last deployment in Distribute Database'
        else 'Object deployed in Distribute Database'
    end "Status",
    enabled "Enabled",
    src_schema "Source Schema",
    src_object_name "Source Object Name",
    anonymized_columns "Anonymized Columns",
    rls_columns "RLS Columns",
    aud_updated_info:cet_timestamp::timestamp_ntz "Last Altered At",
    aud_updated_info:user::varchar "Last Changed By",
    convert_timezone('Europe/Berlin',base.last_altered)::timestamp_ntz "Base Object Last Altered At",
    convert_timezone('Europe/Berlin',dist.last_altered)::timestamp_ntz "Distribute Object Last Altered At"
from
    <environment>_<my_project>.<dbt_prefix_if_necessary>sdc_distribute.sdc_distribute__object obj
    left outer join <environment>_<my_project>.information_schema.tables base on
        base.table_schema = '<DBT_PREFIX_IF_NECESSARY>'||obj.src_schema and
        base.table_name = obj.src_object_name
    left outer join <environment>_distribute.information_schema.tables dist on
        dist.table_schema = '<MY_PROJECT>' and
        dist.table_name = '<DBT_PREFIX_IF_NECESSARY>'||obj.dist_object_name
where
    aud_updated_info:cet_timestamp = :daterange
order by    
    dist_database_name,
    dist_object_name,
    src_schema,
    src_object_name,
    aud_updated_info:cet_timestamp::timestamp_ntz,
    aud_updated_info:user::varchar
```

`Distribute User Access`
```sql
select
    dist_database_name "Distribute Database",
    dist_object_name "Distribute Object",
    enabled "Enabled",
    grantee,
    aud_updated_info:cet_timestamp::timestamp_ntz "Last Changed At",
    aud_updated_info:user::varchar "Last Changed By"
from
    <environment>_<my_project>.<dbt_prefix_if_necessary>sdc_distribute.sdc_distribute__object_access
where
    aud_updated_info:cet_timestamp = :daterange
order by    
    dist_database_name,
    dist_object_name,
    grantee,
    aud_updated_info:cet_timestamp::timestamp_ntz,
    aud_updated_info:user::varchar
```

`Distribute Row Level Security`
```sql
select
    dist_database_name "Distribute Database",
    dist_object_name "Distribute Object",
    enabled "Enabled",
    grantee,
    <rls_column_1>,
    <rls_column_2>,
    <rls_column_3>,
    aud_updated_info:cet_timestamp::timestamp_ntz "Last Changed At",
    aud_updated_info:user::varchar "Last Changed By"
from
    <environment>_<my_project>.<dbt_prefix_if_necessary>sdc_distribute.sdc_distribute__object_access_rls
where
    aud_updated_info:cet_timestamp = :daterange
order by    
    dist_database_name,
    dist_object_name,
    grantee,
    <rls_column_1>,
    <rls_column_2>,
    <rls_column_3>,
    aud_updated_info:cet_timestamp::timestamp_ntz,
    aud_updated_info:user::varchar
```

# Migration to RBAC
The base Distribute Layer is being migrated to Role-Based Access Control as described in
[Migration to Role-Based Access Control in the Distribute Layer - a step by step guide](
https://wiki.siemens.com/display/en/Migration+to+Role-Based+Access+Control+in+the+Distribute+Layer+-+a+step+by+step+guide)

This is impacting the SDC Distribute dbt package users in the following points:
- The projects using the SDC Distribute dbt package must follow the 3 steps described on the previous paragraph

    - Phase 1: until 03-Feb-2023 : review the usage of TU (Technical Users) and GID on the User Access configuration
        - The new solution (RBAC) only allows to grant access at the project level, so if currently your project uses Technical Users (TU) or GID to give access to a specific group of objects, please create new Light Projects and assign those TU or GID to it. On the user access table replace then the TU or GID with the Light Project. The migration procedure (on the Phase 2) will ignore the TU and GID user accesses.

    - Phase 2: between 03-Feb-2023 and 10-Feb-2023 : Migrate the current user access configuration tables to RBAC
        - Use the migration procedure to convert the existing user access records into roles (an example is provided below). This new configuration(RBAC) will coexist with the current one (Distributed views still join with Access Management Table). On the 12th February, the overall role that allows all projects to have access to all objects in the `XXX_DISTRIBUTE` databases will be revoked and each project will just have access to the objects that a role was assigned to (using the migration procedure). Only the new SDC Distribute (v0.1.5) is able to assign the user access using the RBAC. This means that the user access assignments done after running the migration process will not be immediatly available after Phase 2. A new macro (and db procedure will be available to synchronize if necessary). __It is advisable not to grant any new accesses after running the migration procedure until installing the v0.1.5.__

    - Phase 3: after 12-Feb-2023 : Install v0.1.5 and Rebuild the Distribute views on the Distribute Layer
        - Install the version v0.1.5
        - Execute a job to recreate all the views on the Distribute Layer
        - Check the User Access configuration using the new view **sdc_distribute__object_access_sync_status**
        - Execute a sync procedure to realign RBAC with the User Access on SDC Distribute if necessary

 
`Phase 2 - Call the migration procedure before the Feb 10th, for each environment (even development (DBT_<GID>) ones if mecessary)`
```sql
call common.distribute.prc_migration_local_to_central_access_mgnt_table_<ENV>_<PROJECT_NAME>('"<ENV>_<PROJECT_NAME>"."<DISTRIBUTE_SCHEMA>"."SDC_DISTRIBUTE__D2GO_ACCESS_MGNT"');
--call common.distribute.prc_migration_local_to_central_access_mgnt_table_DEV_DOM_SALES('"DEV_DOM_SALES"."DBT_Z003KWBJ_SDC_DISTRIBUTE"."SDC_DISTRIBUTE__D2GO_ACCESS_MGNT"');
--call common.distribute.prc_migration_local_to_central_access_mgnt_table_DEV_DOM_SALES('"DEV_DOM_SALES"."SDC_DISTRIBUTE"."SDC_DISTRIBUTE__D2GO_ACCESS_MGNT"');
--call common.distribute.prc_migration_local_to_central_access_mgnt_table_QUA_DOM_SALES('"QUA_DOM_SALES"."SDC_DISTRIBUTE"."SDC_DISTRIBUTE__D2GO_ACCESS_MGNT"');
--call common.distribute.prc_migration_local_to_central_access_mgnt_table_PRD_DOM_SALES('"PRD_DOM_SALES"."SDC_DISTRIBUTE"."SDC_DISTRIBUTE__D2GO_ACCESS_MGNT"');
```

`Phase 3 - packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc-distribute.git" # git URL
    revision: "v0.1.5" # get the revision from the Release Notes
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

After configuring the package location and revision, get it into your project.
Execute the following command in the dbt command line

`Phase 3 - Call dbt deps`
```
dbt deps
```

Create a manual (not schedulled job) in each environment to execute the migration steps

`Phase 3 - Create a dbt job`
```
dbt run --select sdc_distribute
dbt run --select distribute --vars '{"sdc_distribute__d2go_force_replace":true}'
```

Check the User Access configuration

`Phase 3 - Check User Access`
```sql
select * from <env>_<project>.<distribute_schema>.sdc_distribute__object_access_sync_status
--select * from dev_dom_sales.dbt_z003kwbj_sdc_distribute.sdc_distribute__object_access_sync_status
--select * from dev_dom_sales.sdc_distribute.sdc_distribute__object_access_sync_status
--select * from qua_dom_sales.sdc_distribute.sdc_distribute__object_access_sync_status
--select * from prd_dom_sales.sdc_distribute.sdc_distribute__object_access_sync_status
```

Sync the User Access configuration if necessary

`Phase 3 - Sync if necessary`
```sql
call <env>_<project>.<distribute_schema>.sdc_distribute__object_access$sync
    (   
        p_dist_database_name => 'DISTRIBUTE',
        p_dist_object_name => ['%'],
        p_src_schema => '%',
        p_src_object_name => ['%'],
        p_grantee => ['%']
    );
```

# Fix RBAC grants if necessary

Due to the asynchronous implementation on the user access to allow parallel processing, it may happen that the grant access requests have some delay, 
causing incoherence between what is shown on the view prd_distribute.snowflake_ops.v_rbac_on_project_objects and the grants effectively assigned to the objects.

Please check Snowflake Distribute Layer documentation on this topic [here](https://wiki.siemens.com/pages/viewpage.action?pageId=326606988#HowdoesdatadistributioninSnowflakemainaccountwork?-Grant/Revokeaccessto/fromdataofviewin%22%3Cenvironment%3E_DISTRIBUTE%22database).

Check grants requested on RBAC process
```
select *
from   prd_distribute.snowflake_ops.v_rbac_on_project_objects
where  object_name like '"DEV%' -- it also may be QUA or PRD
and    object_name like '%MY_OBJECT_NAME%';

``` 

Check grants effectivly assigned to the object
```
show grants on DEV_DISTRIBUTE.MY_PROJECT_NAME.MY_OBJECT_NAME;

select *
from   table ( result_scan ( select last_query_id() ) )
where "grantee_name" like 'A_PROJECT_NAME';

``` 


If the select on view prd_distribute.snowflake_ops.v_rbac_on_project_objects shows that the grant was assigned to the project
but you still get the error for insufficient privileges, then please execute this macro

Execute the following commands in the dbt command line
```
dbt run-operation fix_rbac_grants
``` 
