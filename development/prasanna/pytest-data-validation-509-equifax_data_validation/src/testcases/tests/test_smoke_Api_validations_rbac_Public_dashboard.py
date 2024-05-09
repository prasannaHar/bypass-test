import calendar
import json
import random
import time
import pytest
import logging
from src.utils.generate_Api_payload import GenericPayload
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

api_payload = GenericPayload()
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

user_types = [("ADMIN"), ("LIMITED_USER"), ("AUDITOR"), ("RESTRICTED_USER"), ("ASSIGNED_ISSUES_USER"),
              ("PUBLIC_DASHBOARD")]
generic_object = TGhelper()


class TestRbacApiValidation:

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    def test_verify_workspace_creation_Public_dashboard_user(self, create_project_object, get_integration_obj, ):
        ts = calendar.timegm(time.gmtime())
        workspace_name = "smoke_Workspace-" + str(ts)
        create_workspace_response = create_project_object.create_workspace(
            arg_project_name=workspace_name,
            integrationID=get_integration_obj,
            user_type="PUBLIC_DASHBOARD"
        )
        assert create_workspace_response.status_code == 403

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    def test_verify_create_dashboard_public_dashboard_user(self, create_generic_object, create_dashboard_object,
                                                           create_project_object, get_integration_obj):
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            user_type="PUBLIC_DASHBOARD"
        )
        assert get_create_dashboard_response.status_code == 403

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_verify_associated_dashboard_list_public_dashboard_user(self, create_generic_object,
                                                                    create_dashboard_object,
                                                                    create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ou_categories = create_generic_object.get_category(workspaceID=workspace_id, return_type="root_ou_id")
        associated_dashboards = create_dashboard_object.get_associated_dashboards(ou_categories[0],
                                                                                  user_type="PUBLIC_DASHBOARD")
        assert len(associated_dashboards) > 0

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_create_child_ou_public_dashboard_user(self, create_generic_object, create_dashboard_object,
                                                   create_ou_object,
                                                   create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=parent_ref_id, user_type="PUBLIC_DASHBOARD")
        assert child_ou_resp.status_code == 403

    @pytest.mark.run(order=5)
    @pytest.mark.regression
    def test_update_integration_to_ou_public_dashboard_user(self, create_generic_object, create_dashboard_object,
                                                            create_ou_object,
                                                            create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']

        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="response")
        ou_name = ou_list['records'][0]['name']
        ou_id = ou_list['records'][0]['id']
        workspace_resp = create_project_object.get_workspace_details(workspace_id)
        integration = workspace_resp["integrations"][0]['id']
        integration_type = workspace_resp["integrations"][0]['application']
        add_integration = create_ou_object.ou_filters(id=str(ts), integration=integration,
                                                      integration_type=integration_type)

        update_ou_integrations = create_ou_object.create_update_ou(name=ou_name, ou_category_id=cat_id,
                                                                   parent_ref_id=str(parent_ref_id),
                                                                   ou_id=str(ou_id),
                                                                   sections=[add_integration], req_type="put",
                                                                   user_type="PUBLIC_DASHBOARD")
        assert update_ou_integrations.status_code == 403

    @pytest.mark.run(order=6)
    @pytest.mark.regression
    @pytest.mark.parametrize("user_type", user_types)
    def test_create_new_propelo_user_public_dashboard_user(self, create_project_object, user_type,
                                                           create_generic_object, create_ou_object):
        new_user_response = create_generic_object.create_user(arg_user_type=user_type,login_user_type="PUBLIC_DASHBOARD")
        assert new_user_response.status_code == 403

    @pytest.mark.run(order=7)
    @pytest.mark.regression
    @pytest.mark.parametrize("user_type", user_types)
    def test_create_new_propelo_user_sso_enabled_public_dashboard_user(self, create_project_object, user_type,
                                                                       create_generic_object,
                                                                       create_ou_object):
        global id, new_user_type, sso_enabled
        new_user_response = create_generic_object.create_user(arg_user_type=user_type, sso_enabled=True,login_user_type="PUBLIC_DASHBOARD")
        assert new_user_response.status_code == 403

    @pytest.mark.run(order=8)
    @pytest.mark.regression
    def test_verify_existing_workspace_public_dashboard_user(self, create_generic_object, create_dashboard_object,
                                                             create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        workspace_resp = create_project_object.get_workspace_details(workspace_id,user_type="PUBLIC_DASHBOARD")
        assert not workspace_resp['disabled']

    @pytest.mark.run(order=9)
    @pytest.mark.regression
    def test_scm_prs_report_public_dashboard(self, create_scm_prs_report_object_public, get_integration_obj):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        create_scm_files_report = create_scm_prs_report_object_public.scm_prs_report(
            integration_id=get_integration_obj,user_type="PUBLIC_DASHBOARD"
        )
        assert create_scm_files_report, "widget is not created"
