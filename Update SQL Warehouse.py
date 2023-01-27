# Databricks notebook source
# Setup input variables
dbutils.widgets.text("start_stop", "start")
dbutils.widgets.text("warehouse_id", "123456790abcdef")
dbutils.widgets.text("auto_stop_mins", "720")
dbutils.widgets.text("size", "default")
dbutils.widgets.text("max_num_clusters", "default")
dbutils.widgets.text("min_num_clusters", "default")
dbutils.widgets.text("spot_instance_policy", "default")
dbutils.widgets.text("enable_serverless_compute", "default")

start_stop = dbutils.widgets.get("start_stop")
warehouse_id = dbutils.widgets.get("warehouse_id")
auto_stop_mins = dbutils.widgets.get("auto_stop_mins")
size = dbutils.widgets.get("size")
max_num_clusters = dbutils.widgets.get("max_num_clusters")
min_num_clusters = dbutils.widgets.get("min_num_clusters")
spot_instance_policy = dbutils.widgets.get("spot_instance_policy")
enable_serverless_compute = dbutils.widgets.get("enable_serverless_compute")

# COMMAND ----------

# Hashmap for cluster sizes
cluster_sizes = {
  "XXSMALL":"2X-Small",
  "XSMALL":"X-Small",
  "SMALL":"Small",
  "MEDIUM":"Medium",
  "LARGE":"Large",
  "XLARGE":"X-Large",
  "XXLARGE":"2X-Large",
  "XXXLARGE":"3X-Large",
}

# COMMAND ----------

import json
import requests
import pprint

# Get Databricks Workspace URL
databricksURL = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)

# Get Databricks PAT 
myToken = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

header = {"Authorization": "Bearer {}".format(myToken)}

# COMMAND ----------

import time


def is_integer(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()


def get_warehouse_details(warehouse_id):
    details_endpoint = f"/api/2.0/sql/warehouses/{warehouse_id}"
    details_resp = requests.get(databricksURL + details_endpoint, headers=header)
    return details_resp.json()


def edit_warehouse(
    warehouse_id,
    auto_stop_mins,
    size,
    min_num_clusters,
    max_num_clusters,
    spot_instance_policy,
):
    header = {"Authorization": "Bearer {}".format(myToken)}
    endpoint = f"/api/2.0/sql/warehouses/{warehouse_id}/edit"
    warehouse_details = get_warehouse_details(warehouse_id)

    # Update Cluster Settings
    warehouse_details["auto_stop_mins"] = auto_stop_mins
    warehouse_details["spot_instance_policy"] = spot_instance_policy

    if is_integer(min_num_clusters):
        warehouse_details["min_num_clusters"] = min_num_clusters

    if is_integer(max_num_clusters):
        warehouse_details["max_num_clusters"] = max_num_clusters

    if spot_instance_policy != "default":
        warehouse_details["spot_instance_policy"] = spot_instance_policy

    if enable_serverless_compute != "default":
        warehouse_details["enable_serverless_compute"] = enable_serverless_compute

    if size != "default":
        warehouse_details["size"] = size
        warehouse_details["cluster_size"] = cluster_sizes[size]

    resp = requests.post(
        databricksURL + endpoint, json=warehouse_details, headers=header
    )

    if resp.status_code == 200:
        print("Warehouse Definition Updated Successfully")
    else:
        raise Exception(resp.content)
        


def start_warehouse(warehouse_id):
    header = {"Authorization": "Bearer {}".format(myToken)}
    endpoint = f"/api/2.0/sql/warehouses/{warehouse_id}/start"
    resp = requests.post(databricksURL + endpoint, headers=header)

    start_time = time.time()
    while True:
        warehouse_details = get_warehouse_details(warehouse_id)
        if warehouse_details["state"] == "STARTING":
            b = f"Starting ({round(time.time() - start_time)})"
            print(b, end="\r")
            time.sleep(5)

        elif warehouse_details["state"] == "RUNNING":
            print("SQL Warehouse Started Succesfully.")
            break


def stop_warehouse(warehouse_id):
    header = {"Authorization": "Bearer {}".format(myToken)}
    endpoint = f"/api/2.0/sql/warehouses/{warehouse_id}/stop"
    resp = requests.post(databricksURL + endpoint, headers=header)

    start_time = time.time()
    while True:
        warehouse_details = get_warehouse_details(warehouse_id)
        if warehouse_details["state"] != "STOPPED":
            b = f"Starting ({round(time.time() - start_time)})"
            print(b, end="\r")
            time.sleep(5)
        elif warehouse_details["state"] == "STOPPED":
            print("SQL Warehouse Terminated Succesfully.")
            break


def start_stop_warehouse(
    start_stop,
    warehouse_id,
    auto_stop_mins,
    size,
    min_num_clusters,
    max_num_clusters,
    spot_instance_policy,
    enable_serverless_compute,
):
    warehouse_details = get_warehouse_details(warehouse_id)

    if start_stop.lower() == "start":
        # check to see if cluster is running
        if warehouse_details["state"] == "STOPPED":
            print("Starting SQL Warehouse")
            edit_warehouse(
                warehouse_id,
                auto_stop_mins,
                size,
                min_num_clusters,
                max_num_clusters,
                spot_instance_policy,
            )
            start_warehouse(warehouse_id)

        else:
            edit_warehouse(
                warehouse_id,
                auto_stop_mins,
                size,
                min_num_clusters,
                max_num_clusters,
                spot_instance_policy,
            )

    elif start_stop.lower() == "stop":
        if warehouse_details["state"] == "RUNNING":
            print("Stopping SQL Warehouse")
            stop_warehouse(warehouse_id)
        edit_warehouse(
            warehouse_id,
            auto_stop_mins,
            size,
            min_num_clusters,
            max_num_clusters,
            spot_instance_policy,
        )

# COMMAND ----------

# MAGIC %md Start SQL Warehouse

# COMMAND ----------

start_stop_warehouse(
    start_stop,
    warehouse_id,
    auto_stop_mins,
    size,
    min_num_clusters,
    max_num_clusters,
    spot_instance_policy,
    enable_serverless_compute,
)
