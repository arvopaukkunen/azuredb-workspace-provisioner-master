# This is a simple Azure OAuth2 client that could be used to retrieve the different
# AAD tokens for a service principal identity, assuming its credentials and tenant_id
# are set in the OS environment. Plus it's also used to run management operations on
# relevant Azure resources.

# This client expects that the following environment vars are set:
#
# AZURE_TENANT_ID: with your Azure Active Directory tenant id
# AZURE_CLIENT_ID: with your Azure Active Directory Application / Service Principal Client ID
# AZURE_CLIENT_SECRET: with your Azure Active Directory Application / Service Principal Secret

import os
import json
import requests
import ssl

from requests.adapters import HTTPAdapter

try:
    from requests.packages.urllib3.poolmanager import PoolManager
    from requests.packages.urllib3 import exceptions
except ImportError:
    from urllib3.poolmanager import PoolManager
    from urllib3 import exceptions

class TlsV1HttpAdapter(HTTPAdapter):
    """
    A HTTP adapter implementation that specifies the ssl version to be TLS1.
    This avoids problems with openssl versions that
    use SSL3 as a default (which is not supported by the server side).
    """

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_version=ssl.PROTOCOL_TLSv1_2)

class AzureOAuth2Client(object):

    def __init__(self):
        self.session = requests.Session()
        self.session.mount('https://', TlsV1HttpAdapter())
        self.headers = {'Content-Type':'application/x-www-form-urlencoded'}

        # Get the service principal credentials and tenant id
        self.client_id = os.environ['AZURE_CLIENT_ID']
        self.client_secret = os.environ['AZURE_CLIENT_SECRET']
        self.tenant_id = os.environ['AZURE_TENANT_ID']
        self.url = "https://login.microsoftonline.com/" + self.tenant_id + "/oauth2/token"

        self.aad_access_token = None
        self.aad_mgmt_token = None

    # Get the AAD access token for the service principal
    def get_aad_access_token(self):
        if self.aad_access_token is None:
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'resource': '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'
            }
            resp = self.session.request('GET', self.url, data=payload, verify = True, headers = self.headers)
            resp_json = resp.json()
            self.aad_access_token = resp_json['access_token']
        return self.aad_access_token

    # Get the Azure management resource endpoint token for the service principal 
    def get_aad_mgmt_token(self):
        if self.aad_mgmt_token is None:
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'resource': 'https://management.core.windows.net/'
            }
            resp = self.session.request('GET', self.url, data=payload, verify = True, headers = self.headers)
            resp_json = resp.json()
            self.aad_mgmt_token = resp_json['access_token']
        return self.aad_mgmt_token

    # Get the URL of an Azure Databricks workspace with its full resource id
    def get_azdbx_workspace_url(self, resource_id, api_version):
        azdbx_mgmt_api_url = "https://management.azure.com" + resource_id + "?api-version=" + api_version
        azdbx_mgmt_api_resp = self.session.request('GET', azdbx_mgmt_api_url, verify = True, 
            headers = {'Authorization': 'Bearer ' + self.get_aad_mgmt_token()})
        azdbx_mgmt_api_resp_json = azdbx_mgmt_api_resp.json()
        azdbx_workspace_url = "https://" + azdbx_mgmt_api_resp_json['properties']['workspaceUrl'] + "/api/2.0"
        return azdbx_workspace_url

    # Add the service endpoint for a service type to a Azure Databriks subnet with its full resource id
    # The Subnet Update API needs the existing delegation and NSG to be set, else it'll overwrite existing settings
    def add_service_endpoint_for_subnet(self, resource_id, api_version, address_prefix, service_type,
            delegation_name, nsg_resource_id, nsg_name):
        network_mgmt_api_url = "https://management.azure.com" + resource_id + "?api-version=" + api_version
        payload = {
            "properties": {
                "addressPrefix": address_prefix,
                "delegations": [{
                    "name": delegation_name,
                    "properties": {
                        "serviceName": "Microsoft.Databricks/workspaces"
                    }
                }],
                "networkSecurityGroup": {
			        "id": nsg_resource_id,
			        "name": nsg_name
		        },
                "serviceEndpoints": [{
                    "service": service_type
                }]
            }
        }
        network_mgmt_api_resp = self.session.request('PUT', network_mgmt_api_url, data=json.dumps(payload), 
            verify = True, headers = {'Authorization': 'Bearer ' + self.get_aad_mgmt_token(), 
            "Content-Type": "application/json"})
        print("The API status code is {}".format(network_mgmt_api_resp.status_code))
        print(network_mgmt_api_resp.json())
        print("Added the service endpoint {} for resource {}".format(service_type, resource_id))

    # Add the storage firewall rules to a ADLS Gen2 storage account for source Azure Databricks subnets
    # This Management API overwrites any existing storage firewall rules, so you'll have to provide all
    # existing rules in the payload JSON
    def add_firewall_rules_to_storage(self, resource_id, api_version, location, subnet_resource_ids):
        storage_mgmt_api_url = "https://management.azure.com" + resource_id + "?api-version=" + api_version

        another_workspace_subnet_1 = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/" + \
            "my-another-rg/providers/Microsoft.Network/virtualNetworks/my-another-workspace-vnet/" + \
            "subnets/host-subnet"
        another_workspace_subnet_2 = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/" + \
            "my-another-rg/providers/Microsoft.Network/virtualNetworks/my-another-workspace-vnet/" + \
            "subnets/container-subnet"

        # If you don't have any existing storage firewall rules for other subnets, please remove the Allow rules
        # below for another_workspace_subnet_1 and another_workspace_subnet_2
        payload = {
            "location": location,
            "properties": {
                "networkAcls": {
                    "virtualNetworkRules": [
                        {
                            "action": "Allow",
                            "id": subnet_resource_ids[0]
                        },
                        {
                            "action": "Allow",
                            "id": subnet_resource_ids[1]
                        },
                        {
                            "action": "Allow",
                            "id": another_workspace_subnet_1
                        },
                        {
                            "action": "Allow",
                            "id": another_workspace_subnet_2
                        }
                    ]
                }
            }
        }
        storage_mgmt_api_resp = self.session.request('PUT', storage_mgmt_api_url, data=json.dumps(payload), 
            verify = True, headers = {'Authorization': 'Bearer ' + self.get_aad_mgmt_token(),
            "Content-Type": "application/json"})
        print("The API status code is {}".format(storage_mgmt_api_resp.status_code))
        print(storage_mgmt_api_resp.json())
        print("Added the storage firewall rules for resource {}".format(resource_id))
