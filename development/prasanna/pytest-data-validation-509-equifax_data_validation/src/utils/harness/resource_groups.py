import logging
import pytest
import json
import pandas as pd
from pandas.io.json import json_normalize

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class ResourceGroupHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.connection = self.generic.get_connect_info()
        self.config_info = self.generic.get_api_info()

    def create_resource_group(self, resource_group_name, cleanup=True):
        ## resource group creation
        try:
            if cleanup:
                resource_group_exists = self.resource_groups_list(
                                            resource_group=resource_group_name)
                if resource_group_exists:
                    deletion_status = self.delete_resource_group(
                                            resource_group_name=resource_group_name)
            payload = {"resourceGroup": {
                "identifier": resource_group_name,
                "name": resource_group_name,
                "description": "",
                "tags": {},
                "color": "#0063f7",
                "accountIdentifier": pytest.tenant_name,
                "includedScopes": [],
                "resourceFilter": {}}}
            url = self.connection["base_url"] + self.config_info["resourcegroup"]
            resp = self.generic.execute_api_call(url, "post", data=payload,
                                            status_code_info=True, 
                                            params={"filterType":"INCLUDE_INHERITED_GROUPS"})
            assert resp.status_code == 200, "resource group creation failed --" + resource_group_name
        except Exception as e:
            LOG.error(f" ===== create_resource_group: Error occurred. Error: {e}")
            return False
        return True


    def update_resource_group(self, resource_group_name, 
                    config_settings="All", insights="All", 
                    collections="All"):
        ## resource group creation
        try:
            resources = []
            ## config settings
            if config_settings == "All":        
                resources.append({"resourceType": "SEI_CONFIGURATION_SETTINGS"})
            else: 
                breakpoint()
                print("logic needs to be filled")
            ## insights 
            if insights == "All":
                resources.append({"resourceType": "SEI_INSIGHTS"})
            else: 
                breakpoint()
                print("logic needs to be filled")
            ## collections
            if collections == "All":
                resources.append({"resourceType": "SEI_COLLECTIONS"})
            else: 
                breakpoint()
                print("logic needs to be filled")

            payload = {"resourceGroup":{"accountIdentifier": pytest.tenant_name,
                    "identifier": resource_group_name,"name": resource_group_name,
                    "color": "#0063f7", "tags": {},
                    "description": "created using pytest rbac automation script",
                    "allowedScopeLevels": ["account"],
                    "includedScopes": [ {
                        "filter": "EXCLUDING_CHILD_SCOPES", "accountIdentifier": pytest.tenant_name}],
                    "resourceFilter": {
                    "resources": resources,
                    "includeAllResources": False}}}
            url = self.connection["base_url"] + self.config_info["resourcegroup"] + "/" + resource_group_name
            resp = self.generic.execute_api_call(url, "put", data=payload,
                                            status_code_info=True, 
                                            params={"filterType":"INCLUDE_INHERITED_GROUPS"})
            assert resp.status_code == 200, "resource group creation failed --" + resource_group_name
        except Exception as e:
            LOG.error(f" ===== create_resource_group: Error occurred. Error: {e}")
            return False
        return True

    def delete_resource_group(self, resource_group_name):
        ## resource group deletion
        try:
            payload = {}
            url = self.connection["base_url"] + self.config_info["resourcegroup"] + "/" + resource_group_name
            resp = self.generic.execute_api_call(url, "delete", data=payload, status_code_info=True)
            assert resp.status_code == 200, "resource group deletion failed --" + resource_group_name

        except Exception as e:
            LOG.error(f" ===== delete_resource_group: Error occurred. Error: {e}")
            return False
        return True

    def resource_groups_list(self, resource_group=None):
        """will return the complete resource groups list by default.

        Args:
            resource_group (_type_, optional): to check if resource group is exists or not. 
            Defaults to None. if None -- it will return complete list of resource groups
        """
        try:
            payload = {}
            url = self.connection["base_url"] + self.config_info["resourcegroup"]
            resp = self.generic.execute_api_call(url, "get", data=payload, status_code_info=True)
            assert resp.status_code == 200, "resource group list api call failed"
            resp_data = json.loads(resp.text)
            ## Resource list retrieve
            resp_df = pd.DataFrame(resp_data["data"]["content"])
            # Extract the "identifier" column
            identifier_tags = resp_df["resourceGroup"].apply(lambda x: x["identifier"])
            resource_groups = identifier_tags.tolist()
            ## resource group existence check 
            if resource_group:
                if resource_group in resource_groups: return True
                else: return False
            return resource_groups
        except Exception as e:
            LOG.error(f" ===== resource_groups_list: Error occurred. Error: {e}")
            return False

