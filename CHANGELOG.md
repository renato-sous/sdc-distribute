# sdc_distribute v0.1.8

This release includes corrections to the previous release.

## Corrections
- changed the rls view from inner join to an exists condition
- changed the distribute_sf view to use the correct sql instead of reusing the distribute code
- changed wipe-out to take also distribute_sf models

## Installation instructions
- To use this version, include the package with revision `v0.1.8` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc-distribute.git" # git URL
    revision: "v0.1.8"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following commands in the dbt command line
```
dbt deps
dbt run --select sdc_distribute
```

## Recreate the distribute models if necessary

If the project models are using an RLS access control please recreate the distribute layer to avoid the rows duplication issue existing on specific conditions

Execute the following commands in the dbt command line or in a one-time job
```
dbt run --select distribute --vars '{"sdc_distribute__d2go_force_replace":true}'
``` 

# sdc_distribute v0.1.7

This release includes new features and corrections to the previous release.

## New features
- added config sdc_distribute__restricted_access
  - allows to use the object_access (replaced by RBAC) on special use cases where a project has all objects accessible to everyone, 
    except for the "restricted access" ones
- added config sdc_distribute__default_grantee_list
  - allows to define a list of projects to whom the objects are going to grant access automatically when the object is distributed
    if this config is not defined, the access is granted only the owner project (like it already happened in the past)

Please refer to the readme file for a full features descriptions

## Corrections
- fixed the missing default grant access when distributing the objects
  - version 0.1.5 introduced the RBAC grant process but it was being called before the view processing that effectively creates the objects    
- fixed macro wipe_out_model
  - removes the grants before removing the objects so there are no orphan user accesses
- added macro fix_rbac_grants
  - calling the macro searches for missing grants and creates them

## Installation instructions
- To use this version, include the package with revision `v0.1.7` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.1.7"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following commands in the dbt command line
```
dbt deps
dbt run --select sdc_distribute
```

## Fix RBAC grants if necessary

If the select on view prd_distribute.snowflake_ops.v_rbac_on_project_objects shows that the grant was assigned to the project
but you still get the error for insufficient privileges, then please execute this macro

Execute the following commands in the dbt command line
```
dbt run-operation fix_rbac_grants
``` 

# sdc_distribute v0.1.6

This release includes corrections to the previous release.

## Corrections
- the project names are considered uppercase
- only the user access that change status are being updated
- the error occurring when granting access when the models are not being distributed is now muted
- no validation on the RLS grantee
- dependency issues when installing sdc_distribute for the first time is now fixed
- validation on grantee only if the distribution database is "DISTRIBUTE"

## Migration instructions
- To use this version, include the package with revision `v0.1.6` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.1.6"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following commands in the dbt command line
```
dbt deps
dbt run --select sdc_distribute
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

# sdc_distribute v0.1.4

This release includes corrections to the previous release.

## Corrections
- there was a bug on the get_node macro leading to an object has no config error

## Migration instructions
- To use this version, include the package with revision `v0.1.4` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.1.4"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following commands in the dbt command line
```
dbt deps
```


# sdc_distribute v0.1.3

This release includes corrections and a change to address performance enhancement.
It also includes a change on call to the d2go packages implementation to allow new Log messages.

## Corrections
- :warning:  there was a bug on the macro that calls sdc_distribute__object$set. The parameter name was not aligned with procedure definition.

## Migration instructions
- To use this version, include the package with revision `v0.1.3` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.1.3"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following commands in the dbt command line
```
dbt deps
dbt run --select sdc_distribute
```

# sdc_distribute v0.1.2

This release includes only a correction on a macro implementation.

## Corrections
- :warning:  there was a bug when distributing models that were neither starting _schema_ or _(underscore).

## Migration instructions
- To use this version, include the package with revision `v0.1.2` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.1.2"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following command in the dbt command line
```
dbt deps
```


# sdc_distribute v0.1.1

This release includes only a correction on a procedure implementation.

## Corrections
- :warning:  there was a bug when executing the rebuild_framework_objects procedure raising an error for the unknown object sdc_distribute__object_sel.

## Migration instructions
- To use this version, include the package with revision `v0.1.1` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.1.1"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following command in the dbt command line
```
dbt deps
```

- Deploy the sdc_distribute models into the database. 
Execute the following command in the dbt command line
```
dbt run --select sdc_distribute
```

# sdc_distribute v0.1.0

This release has not included new features, it's focused on performance enhancements and consolidation.

## Corrections
- :warning:  there was a bug on the rls_all_values that was not being considered when creating the rls secured distribution view.

## Performance enhancements
- :warning:  The sdc_distribute__object$rebuild_framework_objects has been changed to enhance the performance on 2 solutions:
    - when checking if there were changes on the model columns the join with the information_schema was slow when the project had a large number of objects
    - the call to the procedure common.distribute.prc_distribute_view_processing_<project_name> was being called for each model being distributed, now it's called only once if necessary

## Breaking changes
- :white_check_mark: to implement a performance ehancement, the on-end-run post hook is being called, creating an entry on the logs like the following example

```
> sdc_distribute-on-run-end-0
```

This is expected and will call the  common.distribute.prc_distribute_view_processing_<project_name> when there were models distributed during the run.


## Migration instructions
- To use this version, include the package with revision `v0.1.0` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.1.0"
    # package documentation available on https://code.siemens.com/dbt-cloud-at-siemens/sdc-distribute
```

- Reload the packages
Execute the following command in the dbt command line
```
dbt deps
```

- Deploy the sdc_distribute models into the database. 
Execute the following command in the dbt command line
```
dbt run --select sdc_distribute
```


# sdc_distribute v0.0.2

## Breaking changes
- :new: Project variables to control the d2go interface calls
    - `sdc_distribute__d2go_exclude_environments` - which environments disregard the object creation in distribute databases
    - `sdc_distribute__d2go_force_replace` - force the d2go to be called even if the table definition was not changed

- :new: Config **Distribution Scope option** to inform to which databases (DISTRIBUTE / DISTRBUTE_SF) the models should be distributed to
    - `sdc_distribute__distribution_scope` - internal (DISTRIBUTE - this is the default), external (DISTRIBUTE_SF) or both

- :new: Config **Batch Distribute standard options** to select models
    - `sdc_distribute__src_schema` - defines the **database schema** from where the models will be selected
    - `sdc_distribute__src_path` - defines the **dbt model path** from where the models will be selected
    - `sdc_distribute__src_tags` - lists the tags used to select models to the **Distribute Batch**

- :new: Config **Distribute Batch advanced options** to select models
    - `sdc_distribute__src_select_any_tags` - Selects models that use at least one of the tags in the list
    - `sdc_distribute__src_select_all_tags` - Selects models that use all of the tags in the list
    - `sdc_distribute__src_exclude_any_tags` - Excludes models that use at least one of the tags in the list
    - `sdc_distribute__src_exclude_all_tags` - Excludes models that use all of the tags in the list

- :new: Added flexibility on **RLS Control**
    - The config option `sdc_distribute__rls_all_values` allows the definition of special values used on the **RLS columns** that mark that row as `Public`
    - The **RLS columns** can be defined as lists of values (Variant) to allow each row the be visible to **more than one** `Security Value`

- :new: Added filter options on **dbt macros** and **Database Procedures** on the **p_dist_object_name** parameter
    - folowing the same logic as on **dbt tags**, when the model names are prepended with special prefixes the search is done by tags instead of names
      - `tag:`
      - `select_any_tag:`
      - `select_all_tag:`
      - `exclude_any_tag:`
      - `exclude_all_tag:`

## Migration instructions
- To use this version, include the package with revision `v0.0.2` in the packages file:

`packages.yml`
```yaml
packages:
  - git: "git@code.siemens.com:dbt-cloud-at-siemens/sdc_distribute.git" # git URL
    revision: "v0.0.2"
```

- Deploy the sdc_distribute models into the database. 
Execute the following command in the dbt command line
```
dbt run --select sdc_distribute
```

## Features

The new features are described in the package's `README` file.
