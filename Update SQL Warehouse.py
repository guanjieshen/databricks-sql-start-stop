# Databricks notebook source
# Setup widgetsutils.widgets.text("start_stop_update", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("auto_stop_mins", "")
dbutils.widgets.text("size", "")
dbutils.widgets.text("max_num_clusters", "")
dbutils.widgets.text("min_num_clusters", "")
dbutils.widgets.text("spot_instance_policy", "")
dbutils.widgets.text("enable_serverless_compute","")


# COMMAND ----------

import time


class Warehouse:
    def __init__(self, warehouse_id, databricksURL, databricksToken):
        self.warehouse_id = warehouse_id
        self.databricksURL = databricksURL
        self.databricksToken = databricksToken
        self.header = {"Authorization": "Bearer {}".format(databricksToken)}
        self.cluster_sizes = {
            "XXSMALL": "2X-Small",
            "XSMALL": "X-Small",
            "SMALL": "Small",
            "MEDIUM": "Medium",
            "LARGE": "Large",
            "XLARGE": "X-Large",
            "XXLARGE": "2X-Large",
            "XXXLARGE": "3X-Large",
        }

    def get_warehouse_details(self):
        details_endpoint = f"/api/2.0/sql/warehouses/{self.warehouse_id}"
        details_resp = requests.get(
            databricksURL + details_endpoint, headers=self.header
        )
        return details_resp.json()

    def edit_warehouse(
        self,
        auto_stop_mins,
        size,
        min_num_clusters,
        max_num_clusters,
        spot_instance_policy,
        enable_serverless_compute,
    ):
        endpoint = f"/api/2.0/sql/warehouses/{self.warehouse_id}/edit"
        warehouse_details = self.get_warehouse_details()

        # Update Cluster Settings
        warehouse_details["auto_stop_mins"] = auto_stop_mins
        warehouse_details["spot_instance_policy"] = spot_instance_policy

        if is_integer(min_num_clusters):
            warehouse_details["min_num_clusters"] = min_num_clusters

        if is_integer(max_num_clusters):
            warehouse_details["max_num_clusters"] = max_num_clusters

        if spot_instance_policy:
            warehouse_details["spot_instance_policy"] = spot_instance_policy

        if enable_serverless_compute :
            warehouse_details["enable_serverless_compute"] = enable_serverless_compute

        if size:
            warehouse_details["size"] = size
            warehouse_details["cluster_size"] = self.cluster_sizes[size]

        resp = requests.post(
            databricksURL + endpoint, json=warehouse_details, headers=self.header
        )

        if resp.status_code == 200:
            print("Warehouse Definition Updated Successfully")
        else:
            raise Exception(resp.content)

    def start_warehouse(self):
        endpoint = f"/api/2.0/sql/warehouses/{self.warehouse_id}/start"
        resp = requests.post(databricksURL + endpoint, headers=self.header)

        start_time = time.time()
        while True:
            warehouse_details = self.get_warehouse_details()
            if warehouse_details["state"] == "STARTING":
                b = f"Starting ({round(time.time() - start_time)})"
                print(b, end="\r")
                time.sleep(5)

            elif warehouse_details["state"] == "RUNNING":
                print("SQL Warehouse Started Succesfully.")
                break

    def stop_warehouse(self):
        endpoint = f"/api/2.0/sql/warehouses/{self.warehouse_id}/stop"
        resp = requests.post(databricksURL + endpoint, headers=self.header)

        start_time = time.time()
        while True:
            warehouse_details = self.get_warehouse_details()
            if warehouse_details["state"] != "STOPPED":
                b = f"Stopping ({round(time.time() - start_time)})"
                print(b, end="\r")
                time.sleep(5)
            elif warehouse_details["state"] == "STOPPED":
                print("SQL Warehouse Terminated Succesfully.")
                break

# COMMAND ----------

import json
import requests
import pprint
import time


# Check if string is integer
def is_integer(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()

# Handle update to the warehouse
def start_stop_update_warehouse(
    warehouse,
    start_stop_update,
    auto_stop_mins,
    size,
    min_num_clusters,
    max_num_clusters,
    spot_instance_policy,
    enable_serverless_compute,
):
    warehouse_details = warehouse.get_warehouse_details()
    
    if start_stop_update.lower() == "update":
        print("Updating SQL Warehouse Definition")
        warehouse.edit_warehouse(
            auto_stop_mins,
            size,
            min_num_clusters,
            max_num_clusters,
            spot_instance_policy,
            enable_serverless_compute,
        )

    if start_stop_update.lower() == "start":
        # check to see if cluster is running
        if warehouse_details["state"] == "STOPPED":
            print("Starting SQL Warehouse")
            warehouse.edit_warehouse(
                auto_stop_mins,
                size,
                min_num_clusters,
                max_num_clusters,
                spot_instance_policy,
                enable_serverless_compute,
            )
            warehouse.start_warehouse()

        else:
            warehouse.edit_warehouse(
                auto_stop_mins,
                size,
                min_num_clusters,
                max_num_clusters,
                spot_instance_policy,
                enable_serverless_compute,
            )

    elif start_stop_update.lower() == "stop":
        if warehouse_details["state"] == "RUNNING":
            print("Stopping SQL Warehouse")
            warehouse.stop_warehouse()
        warehouse.edit_warehouse(
            auto_stop_mins,
            size,
            min_num_clusters,
            max_num_clusters,
            spot_instance_policy,
            enable_serverless_compute,
        )

# COMMAND ----------

# Get inputs vars
start_stop_update = dbutils.widgets.get("start_stop_update")
warehouse_id = dbutils.widgets.get("warehouse_id")
auto_stop_mins = dbutils.widgets.get("auto_stop_mins")
size = dbutils.widgets.get("size")
max_num_clusters = dbutils.widgets.get("max_num_clusters")
min_num_clusters = dbutils.widgets.get("min_num_clusters")
spot_instance_policy = dbutils.widgets.get("spot_instance_policy")
enable_serverless_compute = dbutils.widgets.get("enable_serverless_compute")

# Get Databricks Workspace URL
databricksURL = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)

# Get Databricks PAT
databricksToken = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

# Create instance of warehouse
target_warehouse = Warehouse(warehouse_id, databricksURL, databricksToken)

# Update warehouse
start_stop_update_warehouse(
    target_warehouse,
    start_stop_update,
    auto_stop_mins,
    size,
    min_num_clusters,
    max_num_clusters,
    spot_instance_policy,
    enable_serverless_compute,
)
