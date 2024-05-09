import logging
import json
import pandas as pd


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class UserGroupHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.connection = self.generic.get_connect_info()
        self.config_info = self.generic.get_api_info()
        self.role_mapping = self.config_info["rbac_role_identifier"]
        
    def create_user_group(self, user_group_name, cleanup=True):
        ## resource group creation
        try:
            if cleanup:
                resource_group_exists = self.user_groups_list(
                                            user_group=user_group_name)
                if resource_group_exists:
                    deletion_status = self.delete_user_group(
                                            user_group_name=user_group_name)
            payload = { "name": user_group_name,"identifier": user_group_name,
                "description": "created using pytest automation script","tags": {}}            
            url = self.connection["base_url"] + self.config_info["usergroup"]
            resp = self.generic.execute_api_call(url, "post", data=payload,
                                            status_code_info=True)
            assert resp.status_code == 200, "resource group creation failed --" + user_group_name
        except Exception as e:
            LOG.error(f" ===== create_user_group: Error occurred. Error: {e}")
            return False
        return True


    def update_user_group(self, user_group_name, role_and_rg_mapping):
        ## resource group creation
        try:
            roleAssignments = []
            for each_mapping in role_and_rg_mapping:
                 roleAssignments.append({"resourceGroupIdentifier":each_mapping[1],
                    "roleIdentifier":self.role_mapping[each_mapping[0]],
                    "principal":{"identifier":user_group_name,"type":"USER_GROUP"}})                
            payload = {"roleAssignments":roleAssignments}
            url = self.connection["base_url"] + self.config_info["roleassignments"]
            resp = self.generic.execute_api_call(url, "post", data=payload, status_code_info=True)
            assert resp.status_code == 200, "user group creation failed --" + user_group_name
        except Exception as e:
            LOG.error(f" ===== update_user_group: Error occurred. Error: {e}")
            return False
        return True


    def delete_user_group(self, user_group_name):
        ## resource group deletion
        try:
            payload = {}
            url = self.connection["base_url"] + self.config_info["user_group_deletion"] + "/" + user_group_name
            resp = self.generic.execute_api_call(url, "delete", data=payload, status_code_info=True)
            assert resp.status_code == 200, "resource group deletion failed --" + user_group_name

        except Exception as e:
            LOG.error(f" ===== delete_resource_group: Error occurred. Error: {e}")
            return False
        return True


    def user_groups_list(self, user_group=None):
        """will return the complete resuserource groups list by default.

        Args:
            user_group (_type_, optional): to check if resource group is exists or not. 
            Defaults to None. if None -- it will return complete list of resource groups
        """
        try:
            payload = {}
            url = self.connection["base_url"] + self.config_info["user_groups_list"]
            resp = self.generic.execute_api_call(url, "get", data=payload, status_code_info=True)
            assert resp.status_code == 200, "user group list api call failed"
            resp_data = json.loads(resp.text)
            ## Resource list retrieve
            resp_df = pd.DataFrame(resp_data["data"]["content"])
            # Extract the "identifier" column
            identifier_tags = resp_df["userGroupDTO"].apply(lambda x: x["identifier"])
            user_groups = identifier_tags.tolist()
            ## resource group existence check 
            if user_group:
                if user_group in user_groups: return True
                else: return False
            return user_groups
        except Exception as e:
            LOG.error(f" ===== resource_groups_list: Error occurred. Error: {e}")
            return False

