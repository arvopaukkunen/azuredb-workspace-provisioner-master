# This is a simple Azure Databricks API client that could be used to invoke the different
# API endpoints like SCIM (to manage users & groups), clusters, workspace (to upload notebooks), 
# jobs, permissions (preview) etc.

import os
import json
import requests
import ssl

from requests.adapters import HTTPAdapter

from azdbx_azure_oauth2_client import AzureOAuth2Client

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

class DatabricksAPIClient(object):

    def __init__(self, adb_workspace_resource_id):
        self.session = requests.Session()
        self.session.mount('https://', TlsV1HttpAdapter())

        azure_oauth2_client = AzureOAuth2Client()
        self.aad_access_token = azure_oauth2_client.get_aad_access_token()
        self.aad_mgmt_token = azure_oauth2_client.get_aad_mgmt_token()
        self.headers = {
            'Authorization': 'Bearer ' +  self.aad_access_token,
            'X-Databricks-Azure-SP-Management-Token': self.aad_mgmt_token,
            'X-Databricks-Azure-Workspace-Resource-Id': adb_workspace_resource_id
        }
        
        # Get the URL of the deployed Azure Databricks workspace
        self.url_prefix = azure_oauth2_client.get_azdbx_workspace_url(adb_workspace_resource_id, "2018-04-01")

    # Get the Azure Databricks workspace base endpoint
    def get_url_prefix(self):
        return self.url_prefix

    # Utility method to invoke different APIs on the Azure Databricks workspace base endpoint
    def invoke_request(self, method, api_endpoint, payload):
        resp = self.session.request(method, self.url_prefix + api_endpoint, data=json.dumps(payload), 
            verify = True, headers = self.headers)
        print("API response status code is {}".format(resp.status_code))
        resp_json = resp.json()
        return resp_json

    # Invoke the SCIM /Users API to provision a user in the Azure Databricks workspace
    def create_user(self, user_name, assign_cluster_create):
        api_endpoint = "/preview/scim/v2/Users"
        if assign_cluster_create:
            payload = {
                "schemas":[
                    "urn:ietf:params:scim:schemas:core:2.0:User"
                ],
                "userName": user_name,
                "entitlements":[
                    {
                        "value":"allow-cluster-create"
                    }
                ]
            }
        else:
            payload = {
                "schemas":[
                    "urn:ietf:params:scim:schemas:core:2.0:User"
                ],
                "userName": user_name
            }
        resp_json = self.invoke_request('POST', api_endpoint, payload)
        print("Added the user {} with id {}".format(user_name, resp_json['id']))
        return resp_json['id']

    # Invoke the SCIM /Groups API to provision a group in the Azure Databricks workspace
    def create_group(self, group_name):
        api_endpoint = "/preview/scim/v2/Groups"
        payload = {
            "schemas":[
                "urn:ietf:params:scim:schemas:core:2.0:Group"
            ],
            "displayName": group_name
        }
        resp_json = self.invoke_request('POST', api_endpoint, payload)
        print("Added the group {} with id {}".format(group_name, resp_json['id']))
        return resp_json['id']

    # Invoke the SCIM /Groups API to get the "admins" group id
    def get_admin_group(self):
        api_endpoint = "/preview/scim/v2/Groups"
        resp_json = self.invoke_request('GET', api_endpoint, {})
        resources = resp_json['Resources']
        for resource in resources:
            if resource['displayName'] == 'admins':
                return resource['id']
        return None

    # Invoke the SCIM /Groups API to add a user to a group in the Azure Databricks workspace
    def add_user_to_group(self, user_id, group_id):
        api_endpoint = "/preview/scim/v2/Groups"
        payload = {
            "schemas":[
                "urn:ietf:params:scim:api:messages:2.0:PatchOp"
            ],
            "Operations":[
                {
                    "op": "add",
                    "value": {
                        "members":[
                            {
                                "value": user_id
                            }
                        ]
                    }
                }
            ]
        }
        self.invoke_request('PATCH', api_endpoint + "/" + group_id, payload)
        print("Added the user {} to group {}".format(user_id, group_id))

    # Invoke the /workspace/import API to import a notebook into a user's sandbox 
    # in a Azure Databricks workspace
    def import_notebook(self, dest_nb_path, language, format, src_nb_content):
        api_endpoint = '/workspace/import'
        payload = {
            "path": dest_nb_path,
            "format": format,
            "language": language,
            "content": src_nb_content,
            "overwrite": False
        }
        self.invoke_request('POST', api_endpoint, payload)
        print("Imported the notebook {} in the workspace".format(dest_nb_path))

    # Invoke the /clusters/create API to create a cluster in a Azure Databricks workspace
    def create_cluster(self, cluster_source_file):
        api_endpoint = '/clusters/create'
        payload = None
        cluster_json_path = os.path.join(
            os.path.dirname(__file__), 'workspace_object_src', cluster_source_file)
        with open(cluster_json_path, 'r') as cluster_json_file:
            payload = json.load(cluster_json_file)
        resp_json = self.invoke_request('POST', api_endpoint, payload)
        print("Created the cluster for source json in {} with id {}".format(cluster_source_file, 
            resp_json['cluster_id']))
        return resp_json['cluster_id']

    # Invoke the /jobs/create API to create a job in a Azure Databricks workspace
    def create_job(self, job_source_file):
        api_endpoint = '/jobs/create'
        payload = None
        job_json_path = os.path.join(
            os.path.dirname(__file__), 'workspace_object_src', job_source_file)
        with open(job_json_path, 'r') as job_json_file:
            payload = json.load(job_json_file)
        resp_json = self.invoke_request('POST', api_endpoint, payload)
        print("Created the job for source json in {} with id {}".format(job_source_file, 
            resp_json['job_id']))
        return str(resp_json['job_id'])

    # Invoke the preview /permission/clusters API to set permission for a user on a cluster
    def set_permission_on_cluster(self, cluster_id, user_name, permission):
        api_endpoint = "/preview/permissions/clusters/" + cluster_id
        payload = {
            "access_control_list": [
                {
                    "user_name": user_name,
                    "permission_level": permission
                }
            ]
        }
        self.invoke_request('PUT', api_endpoint, payload)
        print ("Applied permission {} for user {} on cluster {}".format(permission, user_name, cluster_id))
    
    # Invoke the preview /permission/jobs API to set permission for a user on a job
    def set_permission_on_job(self, job_id, user_name, permission):
        api_endpoint = "/preview/permissions/jobs/" + job_id
        payload = {
            "access_control_list": [
                {
                    "user_name": user_name,
                    "permission_level": permission
                }
            ]
        }
        self.invoke_request('PATCH', api_endpoint, payload)
        print ("Applied permission {} for user {} on job {}".format(permission, user_name, job_id))
