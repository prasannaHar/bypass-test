import logging
import pytest

from src.utils.OU_helper import Ouhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TrellisProfileMappingHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.baseurl = self.generic.connection["base_url"]
        self.api_info = self.generic.get_api_info()
        self.trellis_v2_flag = False
        if pytest.tenant_name in self.generic.api_data["trellis_v2_tenants"]:
            self.trellis_v2_flag = True
        self.ou_helper = Ouhelper(generic_helper)
            

    def retreive_ou_trellis_profile(self, collection_id):
        payload = {"filter": {"ou_ref_ids": [collection_id]}}
        url = f"{self.baseurl}{self.api_info['trellis_parent_profiles_list']}"
        response = self.generic.execute_api_call(url, "post", data=payload)
        profile_response = response["records"][0]
        return profile_response

    def retrieve_ou_trellis_profile_v1(self, collection_id):
        url =  f"{self.baseurl}{self.api_info['create_update_ou']}/{collection_id}"
        response = self.generic.execute_api_call(url, "get", data={})
        if "trellis_profile_id" in response.keys():
            return response["trellis_profile_id"]
        return ""

    def retrieve_contributor_trellis_profile(self, collection_id, contributor_uuid):
        ## retreive contributor role
        contributor_role = self.ou_helper.retrieve_valid_users(
                                            user_id=contributor_uuid, 
                                            custom_field="contributor_role")
        ## retreive ou profile response
        ou_profile_response = self.retreive_ou_trellis_profile(
                                                collection_id= collection_id)
        ## retrieve contributor role specific subprofile
        sub_profiles = ou_profile_response["sub_profiles"]
        default_sub_profile = ""
        for sub_profile in sub_profiles:
            if sub_profile["name"] == contributor_role:
                return sub_profile
            if sub_profile["name"] == "Default":
                default_sub_profile = sub_profile
        return default_sub_profile
