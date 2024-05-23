# This is a sample solution for how to provision AAD users and groups into a 
# Azure Databricks workspace in an automated manner. The same action could be done in
# a semi-automated manner via AAD app-based provisioning or in a manual way via
# Databricks admin console.

# This script expects that the following environment vars are set:
#
# AZURE_SUBSCRIPTION_ID: with your Azure Subscription Id
# AZURE_RESOURCE_GROUP: with your Azure Resource Group

import os
import json

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

# Create a list of AAD users and groups to be added to the workspace
groups_to_add = ["non_admin_cluster_creators","non_admin_cluster_users"]
admins = ["a.g@databricks.com", "b.k@databricks.com"]
non_admin_cluster_creators = ["a.s@databricks.com","v.w@databricks.com"]
non_admin_cluster_users = ["ag@gmail.com","k.p@gmail.com"]

# Add AAD users to the workspace
print("Starting to add users to the workspace")
admin_ids = []
for user in admins:
    admin_ids.append(databricks_api_client.create_user(user, True))
non_admin_cluster_creator_ids = []
for user in non_admin_cluster_creators:
    non_admin_cluster_creator_ids.append(databricks_api_client.create_user(user, True))
non_admin_cluster_user_ids = []
for user in non_admin_cluster_users:
    non_admin_cluster_user_ids.append(databricks_api_client.create_user(user, False))
print("Added all users to the workspace")

non_admin_cluster_creators_grp = "non_admin_cluster_creators"
non_admin_cluster_users_grp = "non_admin_cluster_users"

# Add AAD groups to the workspace
print("Starting to add groups to the workspace")
# Admin group already exists so getting the reference id for it
admin_group_id = databricks_api_client.get_admin_group()
print("The admin group id is {}".format(admin_group_id))
non_admin_cluster_creators_grp_id = databricks_api_client.create_group(non_admin_cluster_creators_grp)
non_admin_cluster_users_grp_id = databricks_api_client.create_group(non_admin_cluster_users_grp)
print("Added all groups to the workspace")

# Add AAD users to relevant AAD groups in the workspace
print("Adding users to relevant groups")
for user_id in admin_ids:
    databricks_api_client.add_user_to_group(user_id, admin_group_id)
for user_id in non_admin_cluster_creator_ids:
    databricks_api_client.add_user_to_group(user_id, non_admin_cluster_creators_grp_id)
for user_id in non_admin_cluster_user_ids:
    databricks_api_client.add_user_to_group(user_id, non_admin_cluster_users_grp_id)
print("Added all users to relevant groups")
