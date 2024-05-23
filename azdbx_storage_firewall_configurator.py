# This is a sample solution for how to add service endpoints to the subnets of an
# Azure Databricks workspace and then configure those subnets in the storage firewall
# rules of an ADLS Gen2 Storage.

# This script expects that the following environment vars are set:
#
# AZURE_SUBSCRIPTION_ID: with your Azure Subscription Id
# AZURE_RESOURCE_GROUP: with your Azure Resource Group
# ADLS_GEN2_RESOURCE_GROUP: with your ADLS Gen 2 Storage Resource Group
# ADLS_GEN2_STORAGE_NAME: with your ADLS Gen 2 Storage Name

import os
import json
import time

from azdbx_azure_oauth2_client import AzureOAuth2Client

# Get the Azure Databricks template parameters to get the deployed workspace's parameters
adb_template_parameters = None
adb_template_params_path = os.path.join(
    os.path.dirname(__file__), 'arm_template_params', 'azure_databricks_npip_template_params.json')
with open(adb_template_params_path, 'r') as adb_template_params_file:
    adb_template_parameters = json.load(adb_template_params_file)

# Form the full resource id of the host subnet
host_subnet_resource_id = "/subscriptions/" + os.environ.get(
    'AZURE_SUBSCRIPTION_ID', '11111111-1111-1111-1111-111111111111') + "/resourceGroups/" + \
    os.environ.get('AZURE_RESOURCE_GROUP', 'my-adb-e2-rg') + "/providers/Microsoft.Network/virtualNetworks/" + \
    adb_template_parameters['vnetName'] + "/subnets/" + adb_template_parameters['publicSubnetName']
host_subnet_address_prefix = adb_template_parameters['publicSubnetCidr']
host_subnet_delegation_name = adb_template_parameters['publicSubnetDelegationName']

# Form the full resource id of the container subnet
container_subnet_resource_id = "/subscriptions/" + os.environ.get(
    'AZURE_SUBSCRIPTION_ID', '11111111-1111-1111-1111-111111111111') + "/resourceGroups/" + \
    os.environ.get('AZURE_RESOURCE_GROUP', 'my-adb-e2-rg') + "/providers/Microsoft.Network/virtualNetworks/" + \
    adb_template_parameters['vnetName'] + "/subnets/" + adb_template_parameters['privateSubnetName']
container_subnet_address_prefix = adb_template_parameters['privateSubnetCidr']
container_subnet_delegation_name = adb_template_parameters['privateSubnetDelegationName']

# For the full resource id of the NSG
nsg_name = adb_template_parameters['nsgName']
nsg_resource_id = "/subscriptions/" + os.environ.get(
    'AZURE_SUBSCRIPTION_ID', '11111111-1111-1111-1111-111111111111') + "/resourceGroups/" + \
    os.environ.get('AZURE_RESOURCE_GROUP', 'my-adb-e2-rg') + "/providers/Microsoft.Network/networkSecurityGroups/" + \
    nsg_name

# Create the Azure OAuth2 client
azdbx_azure_oauth2_client = AzureOAuth2Client()

# Add the Storage service endpoint for Azure Databricks workspace subnets
azdbx_azure_oauth2_client.add_service_endpoint_for_subnet(host_subnet_resource_id, "2020-04-01", 
    host_subnet_address_prefix, "Microsoft.Storage", host_subnet_delegation_name, nsg_resource_id, nsg_name)
time.sleep(30)
azdbx_azure_oauth2_client.add_service_endpoint_for_subnet(container_subnet_resource_id, "2020-04-01", 
    container_subnet_address_prefix, "Microsoft.Storage", container_subnet_delegation_name, nsg_resource_id, nsg_name)
time.sleep(30)

# Add the storage firewall rules to your ADLS Gen2 storage account for Azure Databricks subnets
storage_resource_id = "/subscriptions/" + os.environ.get(
    'AZURE_SUBSCRIPTION_ID', '11111111-1111-1111-1111-111111111111') + "/resourceGroups/" + \
    os.environ.get('ADLS_GEN2_RESOURCE_GROUP', 'my-adls-gen2-rg') + "/providers/Microsoft.Storage/storageAccounts/" + \
    os.environ['ADLS_GEN2_STORAGE_NAME']
workspace_subnet_ids = [host_subnet_resource_id, container_subnet_resource_id]
azdbx_azure_oauth2_client.add_firewall_rules_to_storage(storage_resource_id, "2019-06-01", 
    "eastus2", workspace_subnet_ids)
