# databricks-sql-start-stop

This repository contains a Databricks notebook that can be used to help schedule and automate the starting and stopping of Databricks SQL Warehouses. This solution is designed be used with Databricks Workflows for scheduling and notifications.


## How to use this notebook

This notebook should be triggered using Databricks Workflows. Users can either import this notebook (`update_sql_warehouse.py`) into their Workspace, or use a Git reference to this repository.
<br>
<img src="img/git_reference.png" alt="git reference example" width="500"/>

Once a job is created, add a task to point to the `update_sql_warehouse.py` notebook.


### Parameters

This notebook uses [Databricks widgets](https://docs.databricks.com/notebooks/widgets.html) to configure the how the SQL warehouse should be updated.

For `Optional` parameters below, if included; the Warehouse definition will be updated to reflect the new values.

 ###### [Required] `warehouse_id `
 
 This is the unique ID for the SQL warehouse that should be updated.

<img src="img/warehouse_id.png" alt="git reference example" width="500"/>
</br>

 ###### [Required] `start_stop_update ` 
 This parameter supports to following options:
 - `start`: Start the warehouse.
 - `stop`: Stop the warehouse.
 - `update`: Only update the warehouse definition.

 ###### [Optional] `auto_stop_mins ` 
 This parameter sets the auto-terminate time window. If set to 0, the Warehouse will not be set to never auto-terminate.

 ###### [Optional] `size ` 
 This parameter sets the cluster size: 
 - `XXSMALL`: 2X-Small
 - `XSMALL`: X-Small
 - `SMALL`: Small
 - `MEDIUM`: Medium
 - `LARGE`: :Large
 - `XLARGE`: X-Large
 - `XXLARGE`: 2X-Large
 - `XXXLARGE`: 3X-Large
 - `XXXXLARGE`: 4X-Large

 ###### [Optional] `min_num_clusters ` 
 This parameter sets the minimum number of clusters in an endpoint. `max_num_clusters` must be >= `min_num_clusters ` 


 ###### [Optional] `max_num_clusters ` 
 This parameter sets the maximum number of clusters in an endpoint. `max_num_clusters` must be >= `min_num_clusters ` 

 ###### [Optional] `spot_instance_policy ` 
 This parameter sets the spot instance policy:
 - `COST_OPTIMIZED`: Use spot instances when possible.
 - `RELIABILITY_OPTIMIZED`: Do not use spot instances.


###### [Optional] `enable_serverless_compute ` 
 This parameter supports to following options:
 - `True`: Use serverless compute.
 - `False`: Use classic compute.




### Starting a SQL Warehouse 

### Stopping a SQL Warehouse 

### Updating a SQL Warehouse 

### Basic Example

### Advanced Example

