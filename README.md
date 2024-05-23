# azuredb-workspace-provisioner
Sample project provisioning and bootstrapping an Azure Databricks workspace

## Project Structure
The project is composed of separate scripts reusing common objects and configuration, where each could be run on its own at any point of your workspace provisioning/bootstrapping lifecycle. All actions against Azure Management API and Databricks API are performed using a previously configured Service Principal (AAD App).
* azdbx_ws_deployer.py: Deploys a Log Analytics workspace, and then a Azure Databricks _No Public IP (NPIP)_ workspace that uses the Log Analytics workspace as its Audit/Diagnostic Logs target. We utilized the [Azure Deployment Sample](https://github.com/Azure-Samples/resource-manager-python-template-deployment) as inspiration.
* azdbx_storage_firewall_configurator.py (OPTIONAL): Configures the [Storage Service Endpoint](https://docs.microsoft.com/en-us/azure/virtual-network/virtual-network-service-endpoints-overview) for the new workspace subnets, and then configures those subnets in the [Storage Firewall](https://docs.microsoft.com/en-us/azure/storage/common/storage-network-security) of an existing ADLS Gen2 Storage Account.
* azdbx_user_n_group_provisioner.py: Provisions AAD users and groups in the Azure Databricks workspace using the [Databricks SCIM API](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/scim/).
* azdbx_notebook_provisioner.py: Provisions existing notebooks in user sandbox folders in the Azure Databricks workspace using the [Databricks Workspace API](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/workspace).
* azdbx_cluster_n_job_provisioner.py: Creates a [high-concurrency cluster](https://docs.microsoft.com/en-us/azure/databricks/clusters/configure#--high-concurrency-clusters) for data science/analysis, and a on-demand job for ad-hoc execution, in the Azure Databricks workspace using [Databricks Cluster API](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/clusters) and [Jobs API](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/jobs) respectively. It also sets user permissions for the cluster and job using a `preview` _Permissions API_.
* azdbx_azure_oauth2_client.py: A client to get the AAD access and management tokens for the service principal identity, and to perform operations on the Azure Management API for relevant resources.
* azdbx_api_client.py: A client to perform different above mentioned operations against the Databricks REST API. Currently it uses the python `requests` module to invoke the API directly. But it's highly recommended to use the [Databricks CLI API Client](https://github.com/abhinavg6/databricks-cli/blob/master/databricks_cli/sdk/api_client.py) to achieve the same without the need to write boilerplate HTTPS client code, and you get access to all Databricks APIs implicitly.

## Flow of the Execution
Recommended execution steps in this order:
* `python azdbx_ws_deployer.py` to deploy the ARM resources - Log Analytics and Azure Databricks workspaces.
* `azdbx_storage_firewall_configurator.py` (OPTIONAL) to configure the VNET service endpoint and ADLS Gen2 storage firewall.
* `python azdbx_user_n_group_provisioner.py` to provision users and groups in the Azure Databricks workspace.
* `python azdbx_notebook_provisioner.py` to import existing notebooks in the Azure Databricks workspace.
* `python azdbx_cluster_n_job_provisioner.py` to create the cluster & job and set user permissions in the Azure Databricks workspace.

## Requirements
* `pip install azure-mgmt-resource` - To get Azure management & deployment tooling
* `pip install requests` - To get _HTTP for Humans_ package to invoke the Azure Management * Databricks APIs. This is available by default in modern python distros.
* Export/Set these [service principal credentials](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) in your OS environment as `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET`.
* Export/Set the AAD Tenant Id in your OS environment as `AZURE_TENANT_ID`.
* Export/Set the Azure Subscription Id and Resource Group Name in your OS environment as `AZURE_SUBSCRIPTION_ID` and `AZURE_RESOURCE_GROUP`.
* If using the Storage Firewall Configurator, export/set the ADLS Gen2 Resource Group Name and the Storage Name as `ADLS_GEN2_RESOURCE_GROUP` and `ADLS_GEN2_STORAGE_NAME`.
* Set relevant parameters in the ARM templates and related parameter files for your resource deployments.
* Set relevant AAD users and related sandbox folder paths in the scripts, parameter files and object JSONs.
* The default provided data science/analysis notebooks use processed data on a private ADLS Gen 2 storage account. Please feel free to use your own [notebook DBC(s)](https://docs.databricks.com/notebooks/notebooks-manage.html#databricks-archive) or change the existing ones to provide your own ADLS Gen 2 reference.

**Note:** The _No Public IP (NPIP)_ mode for an Azure Databricks workspace and the _Permissions API_ are in `preview`. Please reach out to your Databricks or Microsoft account team for access, before starting to use this solution.
