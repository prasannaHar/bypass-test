import logging
import pytest
import json
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestDashboardAccessibility:
    rbacusertype = ["PUBLIC_DASHBOARD", "ADMIN"]
    if not pytest.standalone_app_flag:
        rbacusertype = ["ADMIN"]

    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_org_unit_api_accessibility(self, rbacusertype,create_generic_object):
        """Validate trellis score report functionality"""
        api_info = create_generic_object.get_api_info()
        ou_id = (create_generic_object.env["set_ous"])[0]
        url = create_generic_object.connection["base_url"] + api_info["create_update_ou"] +  "/" + ou_id 
        payload={}
        resp = create_generic_object.rbac_user(url, "get", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"

    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_dashboards_list_api_accessibility(self, rbacusertype,create_generic_object):
        """Validate trellis score report functionality"""
        api_info = create_generic_object.get_api_info()
        env_info = create_generic_object.get_env_based_info()
        url = create_generic_object.connection["base_url"] + (api_info["dashboard_list"])[1:]
        payload = {"filter": {"workspace_id": int(env_info["workspace_id"])}}
        resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"


    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_org_units_list_api_accessibility(self, rbacusertype,create_generic_object):
        """Validate trellis score report functionality"""
        api_info = create_generic_object.get_api_info()
        env_info = create_generic_object.get_env_based_info()

        ## org groups list -- retrieve ou category id
        url = create_generic_object.connection["base_url"] + api_info["ou_categories"]
        payload = {"filter": {"workspace_id": int(env_info["workspace_id"])}}
        org_groups_resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert org_groups_resp.status_code == 200, "org groups list api call is failing"
        org_groups_response = json.loads(org_groups_resp.text)
        org_groups_response_df = pd.json_normalize(org_groups_response['records'], max_level=1)
        ou_category_id = (org_groups_response_df["root_ou_id"].to_list())[0]
        ## validate org units list api
        url = create_generic_object.connection["base_url"] + api_info["ou_list"]
        payload = {"filter": {"ou_category_id": [ou_category_id]}}
        resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"

    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=4)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_org_units_dashboards_list_api_accessibility(self, rbacusertype,
                        create_generic_object, ou_helper_object):
        """Validate trellis score report functionality"""
        api_info = create_generic_object.get_api_info()
        env_info = create_generic_object.get_env_based_info()
        org_id = (create_generic_object.env["set_ous"])[0]
        ou_uuid = ou_helper_object.retrieve_ou_uuid(ou_id=org_id)
        url = create_generic_object.connection["base_url"] + "ous/" + ou_uuid + api_info["dashboard_list"]
        payload = {"filter":{"inherited":True,"has_rbac_access":True}}
        resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"


    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=5)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_public_dashboard_accessibility(self, rbacusertype,create_generic_object):
        """Validate trellis score report functionality"""
        if not pytest.standalone_app_flag:
            pytest.skip("not applicable for SEI on Harness")
        api_info = create_generic_object.get_api_info()
        env_info = create_generic_object.get_env_based_info()
        url = create_generic_object.connection["base_url"] + (api_info["dashboard_list"])[1:]
        payload = {"filter": {"workspace_id": int(env_info["workspace_id"])}}
        resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"
        response = json.loads(resp.text)
        resp_df = pd.json_normalize(response['records'], max_level=1)
        public_dashbards = (resp_df[resp_df['public']==True])['id'].to_list()
        if len(public_dashbards) == 0:
            pytest.skip("no public dashboards exists")
        dashboard_url = create_generic_object.connection["base_url"] + "dashboards/" + public_dashbards[0] 
        payload = {}
        dashboard_resp = create_generic_object.rbac_user(dashboard_url, "get", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert dashboard_resp.status_code == 200, "unable to access dashboard"
        

    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=6)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_integrations_configs_list_api_accessibility(self, rbacusertype,
                        create_generic_object, get_integration_obj):
        """Validate trellis score report functionality"""
        url = create_generic_object.connection["base_url"] + create_generic_object.api_data["integration_configs_list"]
        payload = {"filter":{"integration_ids":get_integration_obj}}
        resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"


    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=7)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_velocity_configs_list_api_accessibility(self, rbacusertype,create_generic_object):
        """Validate trellis score report functionality"""
        url = create_generic_object.connection["base_url"] + create_generic_object.api_data["integration_configs_list"]
        payload = {}
        resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"


    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=8)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_configs_list_api_accessibility(self, rbacusertype,create_generic_object):
        """Validate trellis score report functionality"""
        url = create_generic_object.connection["base_url"] + create_generic_object.api_data["configs_list"] 
        payload = {}
        resp = create_generic_object.rbac_user(url, "post", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"

    @pytest.mark.regression
    @pytest.mark.sanity
    @pytest.mark.run(order=9)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_non_public_dashboard_accessibility(self, rbacusertype,create_generic_object):
        """Validate trellis score report functionality"""
        if not pytest.standalone_app_flag:
            pytest.skip("not applicable for SEI on Harness")
        api_info = create_generic_object.get_api_info()
        env_info = create_generic_object.get_env_based_info()
        url = create_generic_object.connection["base_url"] + (api_info["dashboard_list"])[1:]
        payload = {"filter": {"workspace_id": int(env_info["workspace_id"])}}
        resp = create_generic_object.execute_api_call(url, "post", data=payload, status_code_info=True)
        assert resp.status_code == 200, "api call is failing"
        response = json.loads(resp.text)
        resp_df = pd.json_normalize(response['records'], max_level=1)
        public_dashbards = (resp_df[resp_df['public']==False])['id'].to_list()
        if len(public_dashbards) == 0:
            pytest.skip("no non-public dashboards exists")
        dashboard_url = create_generic_object.connection["base_url"] + "dashboards/" + public_dashbards[0] 
        payload = {}
        dashboard_resp = create_generic_object.rbac_user(dashboard_url, "get", data=payload,
                                        user_type=rbacusertype, status_code_info=True)
        assert dashboard_resp.status_code == 200, "unable to access dashboard"
