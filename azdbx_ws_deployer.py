# This script is a non-modular sample solution for how to deploy an Log Analytics workspace,
# and then deploy an Azure Databricks NPIP workspace with diagnostic logs configured to be sent
# to the Log Analytics workspace.

import os.path
import json
import time

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode, Deployment, DeploymentProperties

# This script expects that the following environment vars are set:
#
# AZURE_TENANT_ID: with your Azure Active Directory tenant id
# AZURE_CLIENT_ID: with your Azure Active Directory Application / Service Principal Client ID
# AZURE_CLIENT_SECRET: with your Azure Active Directory Application / Service Principal Secret
# AZURE_SUBSCRIPTION_ID: with your Azure Subscription Id
# AZURE_RESOURCE_GROUP: with your Azure Resource Group

# Your Azure Subscriptipn Id
subscription_id = os.environ.get(
    'AZURE_SUBSCRIPTION_ID', '11111111-1111-1111-1111-111111111111')
# Your Resource Group for the deployments
resource_group = os.environ.get('AZURE_RESOURCE_GROUP', 'my-adb-e2-rg')

# Create the ARM client with Service Principal Credentials
credentials = ServicePrincipalCredentials(
    client_id=os.environ['AZURE_CLIENT_ID'],
    secret=os.environ['AZURE_CLIENT_SECRET'],
    tenant=os.environ['AZURE_TENANT_ID']
)
client = ResourceManagementClient(credentials, subscription_id)

# Get the Log Analytics Workspace Template
la_template_body = None
la_template_path = os.path.join(
    os.path.dirname(__file__), 'arm_templates', 'log_analytics_template.json')
with open(la_template_path, 'r') as la_template_file:
    la_template_body = json.load(la_template_file)

# Get the Log Analytics Workspace Template Parameters
la_template_parameters = None
la_template_params_path = os.path.join(
    os.path.dirname(__file__), 'arm_template_params', 'log_analytics_template_params.json')
with open(la_template_params_path, 'r') as la_template_params_file:
    la_template_parameters = json.load(la_template_params_file)
la_template_parameters = {k: {'value': v} for k, v in la_template_parameters.items()}

# Deploy the Log Analytics Workspace Template
la_deployment_properties = DeploymentProperties(mode=DeploymentMode.incremental, 
    template=la_template_body, parameters=la_template_parameters)

print("Deploying Log Analytics Workspace {} in resource group {}".format(
    la_template_parameters['name']['value'], resource_group))
start_time = time.time()
la_deployment_async_operation = client.deployments.create_or_update(
    resource_group,
    'adb-e2-automation-la-deploy',
    Deployment(properties=la_deployment_properties)
)
la_deployment_async_operation.wait()
end_time = time.time()
print("Deployed the Log Analytics Workspace in {} seconds".format(str(int(end_time - start_time))))

# Get the Azure Databricks Workspace Template
adb_template_body = None
adb_template_path = os.path.join(
    os.path.dirname(__file__), 'arm_templates', 'azure_databricks_npip_template.json')
with open(adb_template_path, 'r') as adb_template_file:
    adb_template_body = json.load(adb_template_file)

# Get the Azure Databricks Workspace Template Parameters
adb_template_parameters = None
adb_template_params_path = os.path.join(
    os.path.dirname(__file__), 'arm_template_params', 'azure_databricks_npip_template_params.json')
with open(adb_template_params_path, 'r') as adb_template_params_file:
    adb_template_parameters = json.load(adb_template_params_file)
adb_template_parameters['logAnalyticsWorkspaceId'] = '/subscriptions/' + subscription_id + \
    "/resourceGroups/" + resource_group + "/providers/Microsoft.OperationalInsights/workspaces/" + \
    la_template_parameters['name']['value']
adb_template_parameters = {k: {'value': v} for k, v in adb_template_parameters.items()}

# Deploy the Azure Databricks Workspace Template
adb_deployment_properties = DeploymentProperties(mode=DeploymentMode.incremental, 
    template=adb_template_body, parameters=adb_template_parameters)

print("Deploying Azure Databricks Workspace {} in resource group {}".format(
    adb_template_parameters['workspaceName']['value'], resource_group))
start_time = time.time()
adb_deployment_async_operation = client.deployments.create_or_update(
    resource_group,
    'adb-e2-automation-adbws-deploy',
    Deployment(properties=adb_deployment_properties)
)
adb_deployment_async_operation.wait()
end_time = time.time()
print("Deployed the Azure Databricks Workspace in {} seconds".format(str(int(end_time - start_time))))
