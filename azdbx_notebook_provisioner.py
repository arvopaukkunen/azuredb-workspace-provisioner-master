# This is a sample solution for how to provision existing notebooks
# (like from an existing git repo) into user sandbox folders in an
# Azure Databricks workspace

# This script expects that the following environment vars are set:
#
# AZURE_SUBSCRIPTION_ID: with your Azure Subscription Id
# AZURE_RESOURCE_GROUP: with your Azure Resource Group

import os
import json

from base64 import b64encode

from azdbx_api_client import DatabricksAPIClient

# Get the Azure Databricks template parameters to get the deployed workspace's name
adb_template_parameters = None
adb_template_params_path = os.path.join(
    os.path.dirname(__file__), 'arm_template_params', 'azure_databricks_npip_template_params.json')
with open(adb_template_params_path, 'r') as adb_template_params_file:
    adb_template_parameters = json.load(adb_template_params_file)

# Form the full resource id of the Azure Databricks workspace
adb_workspace_resource_id = "/subscriptions/" + os.environ.get(
    'AZURE_SUBSCRIPTION_ID', '11111111-1111-1111-1111-111111111111') + "/resourceGroups/" + \
    os.environ.get('AZURE_RESOURCE_GROUP', 'my-adb-e2-rg') + "/providers/Microsoft.Databricks/workspaces/" + \
    adb_template_parameters['workspaceName']
print("The workspace resource id is {}".format(adb_workspace_resource_id))

# Create the Databricks API client
databricks_api_client = DatabricksAPIClient(adb_workspace_resource_id)
print("The workspace URL is {}".format(databricks_api_client.get_url_prefix()))

# Import the notebooks to user sandbox folders in the Azure Databricks workspace
create_mount_point_nb_path = os.path.join(
    os.path.dirname(__file__), 'notebooks', 'Create_Mount_Point_on_ADLS_Gen2.dbc')
with open(create_mount_point_nb_path, 'rb') as create_mount_point_nb_file:
    content = b64encode(create_mount_point_nb_file.read()).decode()
    databricks_api_client.import_notebook('/Users/a.g@databricks.com/Create_Mount_Point_on_ADLS_Gen2', 
        'PYTHON', 'DBC', content)

read_adls_gen2_nb_path = os.path.join(
    os.path.dirname(__file__), 'notebooks', 'Read_Data_From_ADLS_Gen2.dbc')
with open(read_adls_gen2_nb_path, 'rb') as read_adls_gen2_nb_file:
    content = b64encode(read_adls_gen2_nb_file.read()).decode()
    databricks_api_client.import_notebook('/Users/a.g@databricks.com/Read_Data_From_ADLS_Gen2', 
        'PYTHON', 'DBC', content)
    databricks_api_client.import_notebook('/Users/ag@gmail.com/Read_Data_From_ADLS_Gen2', 
        'PYTHON', 'DBC', content)

test_spark_configs_nb_path = os.path.join(
    os.path.dirname(__file__), 'notebooks', 'test_spark_configs.dbc')
with open(test_spark_configs_nb_path, 'rb') as test_spark_configs_nb_file:
    content = b64encode(test_spark_configs_nb_file.read()).decode()
    databricks_api_client.import_notebook('/Users/a.g@databricks.com/test_spark_configs', 
        'PYTHON', 'DBC', content)
