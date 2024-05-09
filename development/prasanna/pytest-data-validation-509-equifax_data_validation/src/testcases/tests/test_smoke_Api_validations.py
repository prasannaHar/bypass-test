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
root_ous = ['All Teams', 'All Sprints', 'All Projects']
basic_category = ['Teams', 'Sprints', 'Projects']

user_types = [("ADMIN"), ("LIMITED_USER"), ("AUDITOR"), ("RESTRICTED_USER"), ("ASSIGNED_ISSUES_USER"),
              ("PUBLIC_DASHBOARD")]
generic_object = TGhelper()


class TestApiValidation:

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    def test_verify_new_workspace_root_ou_category_dashboard_creation(self,
                            create_project_object, get_integration_obj, create_ou_object, create_generic_object,create_dashboard_object):
        ## get user details
        ou_names = []
        ts = calendar.timegm(time.gmtime())
        workspace_name = "smoke_Workspace-" + str(ts)
        create_workspace_response = create_project_object.create_workspace(
            arg_project_name=workspace_name,
            integrationID=get_integration_obj
        )
        assert len(create_workspace_response) > 0
        assert create_workspace_response['id']
        workspace_id = create_workspace_response['id']

        ou_categories = create_generic_object.get_category(workspaceID=workspace_id)
        ou_categories_name = create_generic_object.get_category(workspaceID=workspace_id, return_type="name")
        assert set(ou_categories_name) == set(basic_category), "category dont match"

        for each_ou_categories in ou_categories:
            ou_names.append(create_ou_object.get_org_units(ou_categories=each_ou_categories, return_type="name")[0])
        assert set(ou_names) == set(root_ous), "Root Ou's not found"
        dashboard_name = "automation-dashboard" + str(ts)
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,workspace_id=workspace_id)
        assert get_create_dashboard_response["id"]

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    def test_verify_create_dashboard_on_existing_workspace(self, create_generic_object, create_dashboard_object,
                                                          create_project_object, get_integration_obj):
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
        )
        assert get_create_dashboard_response["id"]

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_verify_associated_dashboard_list_existing_OU(self, create_generic_object, create_dashboard_object,
                                                          create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ou_categories = create_generic_object.get_category(workspaceID=workspace_id, return_type="root_ou_id")
        associated_dashboards = create_dashboard_object.get_associated_dashboards(ou_categories[0])
        assert len(associated_dashboards) > 0

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_verify_associated_dashboards_after_move_to_available(self, create_generic_object, create_dashboard_object,
                                                                  create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        ou_categories = create_generic_object.get_category(workspaceID=workspace_id, return_type="root_ou_id")
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
        )

        dashboard_id = get_create_dashboard_response["id"]
        get_available_dashboards = create_dashboard_object.get_available_dashboard(workspace_id=workspace_id)
        associated_dashboards = create_dashboard_object.get_associated_dashboards(ou_categories[0])
        assert dashboard_id in get_available_dashboards
        assert int(dashboard_id) in associated_dashboards
        associated_dashboards_response = create_dashboard_object.get_associated_dashboards(ou_categories[0],
                                                                                           return_type="response")
        move_payload = []
        for each_dash in associated_dashboards_response['records']:
            if each_dash["dashboard_id"] != int(dashboard_id):
                move_payload.append(each_dash)
        create_dashboard_object.move_associated_dashboard(ou_categories[0], move_payload)
        associated_dashboards_after_move = create_dashboard_object.get_associated_dashboards(ou_categories[0])
        assert int(dashboard_id) not in associated_dashboards_after_move, "dashboard not moved"
        create_dashboard_object.delete_dashboard(arg_dashboard_id=dashboard_id)

    @pytest.mark.run(order=5)
    def test_make_dashboard_default(self, create_generic_object, create_dashboard_object, create_ou_object,
                                    create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        ou_categories_id = ou_categories_response["records"][0]['id']
        root_ou_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
        )
        dashboard_id = get_create_dashboard_response["id"]
        ou_response = create_ou_object.get_org_units(ou_categories=ou_categories_id, return_type="response")
        ou_name = ou_response["records"][0]['name']
        ou_id = ou_response["records"][0]['id']
        cat_id = ou_response["records"][0]['ou_category_id']
        if str(root_ou_ref_id) == ou_id:
            mark_default = create_ou_object.create_update_ou(ou_name, cat_id, ou_id, dashboard_id, req_type="put")
        else:
            mark_default = create_ou_object.create_update_ou(ou_name, cat_id, ou_id, dashboard_id,
                                                             parent_ref_id=str(root_ou_ref_id), req_type="put")

        assert mark_default.text == "ok"
        create_dashboard_object.delete_dashboard(arg_dashboard_id=dashboard_id)

    @pytest.mark.run(order=6)
    @pytest.mark.regression
    def test_create_child_ou(self, create_generic_object, create_dashboard_object, create_ou_object,
                             create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=parent_ref_id)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")
        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])

    @pytest.mark.run(order=7)
    @pytest.mark.regression
    def test_delete_child_ou(self, create_generic_object, create_dashboard_object, create_ou_object,
                             create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")

        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']

        cat_id = ou_categories_response["records"][0]['id']

        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=parent_ref_id)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")
        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=8)
    @pytest.mark.regression
    def test_update_integration_to_ou(self, create_generic_object, create_dashboard_object, create_ou_object,
                                      create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=parent_ref_id)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")

        workspace_resp = create_project_object.get_workspace_details(workspace_id)
        integration = workspace_resp["integrations"][0]['id']
        integration_type = workspace_resp["integrations"][0]['application']
        add_integration = create_ou_object.ou_filters(id=str(ts), integration=integration,
                                                      integration_type=integration_type)

        update_ou_integrations = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                                   parent_ref_id=str(parent_ref_id),
                                                                   ou_id=str(child_ou_resp['success'][0]),
                                                                   sections=[add_integration], req_type="put")

        assert update_ou_integrations.text == "ok"
        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=9)
    @pytest.mark.regression
    def test_update_integration_filter_to_ou(self, create_generic_object, create_dashboard_object, create_ou_object,
                                             create_project_object, get_integration_obj):
        global filters, integration_type, integration
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=parent_ref_id)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")

        workspace_resp = create_project_object.get_workspace_details(workspace_id)
        for eachint in workspace_resp["integrations"]:
            if eachint["application"] in ["github", "jira"]:
                integration = eachint['id']
                integration_type = eachint["application"]

        value = []
        if integration_type == "github":
            value = create_generic_object.get_filter_options_scm(arg_filter_type="repo_id")
            filters = {"repo_id": value}
        elif integration_type == "jira":
            get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                           arg_integration_ids=get_integration_obj)
            if len(get_filter_response['records'][0]["project_name"]) == 0:
                pytest.skip("Filter Doesnot have Any values")
            all_filter_records = [get_filter_response['records'][0]["project_name"]]
            value = []
            ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
            for eachissueType in ran_value:
                value.append(eachissueType['key'])
            filters = {"projects": value}
        add_integration = create_ou_object.ou_filters(id=str(ts), integration=integration,
                                                      integration_type=integration_type, var_filters=filters)

        update_ou_integrations = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                                   parent_ref_id=str(parent_ref_id),
                                                                   ou_id=str(child_ou_resp['success'][0]),
                                                                   sections=[add_integration], req_type="put")

        assert update_ou_integrations.text == "ok"
        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=10)
    @pytest.mark.regression
    def test_update_user_attribute_to_ou(self, create_generic_object, create_dashboard_object, create_ou_object,
                                         create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=parent_ref_id)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")

        user_attribute_key = "full_name"
        get_users_attribute_names = create_ou_object.get_OU_mangers(fields=[user_attribute_key])

        user_attribute = get_users_attribute_names["records"][0][user_attribute_key]["records"][0]["key"]
        dynamic_user_definition = {user_attribute_key: [user_attribute]}
        update_ou_integrations = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                                   parent_ref_id=str(parent_ref_id),
                                                                   ou_id=str(child_ou_resp['success'][0]),
                                                                   dynamic_user_definition=dynamic_user_definition,
                                                                   req_type="put")
        assert update_ou_integrations.text == "ok"
        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=11)
    @pytest.mark.regression
    def test_create_child_ou_with_integration_defination(self, create_generic_object, create_dashboard_object,
                                                         create_ou_object,
                                                         create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']

        workspace_resp = create_project_object.get_workspace_details(workspace_id)
        integration = workspace_resp["integrations"][0]['id']
        integration_type = workspace_resp["integrations"][0]['application']
        add_integration = create_ou_object.ou_filters(id=str(ts), integration=integration,
                                                      integration_type=integration_type)

        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=str(parent_ref_id),
                                                          sections=[add_integration])
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")

        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=12)
    @pytest.mark.regression
    def test_create_child_ou_with_integration_filter_defination(self, create_generic_object, create_dashboard_object,
                                                                create_ou_object,
                                                                create_project_object, get_integration_obj):
        global filters, integration_type, integration
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']

        workspace_resp = create_project_object.get_workspace_details(workspace_id)
        for eachint in workspace_resp["integrations"]:
            if eachint["application"] in ["github", "jira"]:
                integration = eachint['id']
                integration_type = eachint["application"]

        value = []
        if integration_type == "github":
            value = create_generic_object.get_filter_options_scm(arg_filter_type="repo_id")
            filters = {"repo_id": value}
        elif integration_type == "jira":
            get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                           arg_integration_ids=get_integration_obj)
            if len(get_filter_response['records'][0]["project_name"]) == 0:
                pytest.skip("Filter Doesnot have Any values")
            all_filter_records = [get_filter_response['records'][0]["project_name"]]
            value = []
            ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
            for eachissueType in ran_value:
                value.append(eachissueType['key'])
            filters = {"projects": value}
        add_integration = create_ou_object.ou_filters(id=str(ts), integration=integration,
                                                      integration_type=integration_type, var_filters=filters)

        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=str(parent_ref_id),
                                                          sections=[add_integration])

        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")
        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=13)
    @pytest.mark.regression
    def test_create_child_ou_with_user_attribute_defination(self, create_generic_object, create_dashboard_object,
                                                            create_ou_object,
                                                            create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        user_attribute_key = "full_name"
        get_users_attribute_names = create_ou_object.get_OU_mangers(fields=[user_attribute_key])

        user_attribute = get_users_attribute_names["records"][0][user_attribute_key]["records"][0]["key"]
        dynamic_user_definition = {user_attribute_key: [user_attribute]}
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=str(parent_ref_id),
                                                          dynamic_user_definition=dynamic_user_definition)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")
        assert str(child_ou_resp['success'][0]) in ou_list

        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=14)
    @pytest.mark.regression
    def test_create_child_ou_with_user_manually_defination(self, create_generic_object, create_dashboard_object,
                                                           create_ou_object,
                                                           create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        all_users = create_ou_object.get_org_users_list()
        valid_users = []
        for each_user in all_users['records']:
            if len(each_user["email"]) != 0:
                valid_users.append(each_user['id'])

        dynamic_user_definition = {"users": valid_users}
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=str(parent_ref_id),
                                                          dynamic_user_definition=dynamic_user_definition)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")
        assert str(child_ou_resp['success'][0]) in ou_list

        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=15)
    @pytest.mark.regression
    def test_update_user_manually_defination_to_ou(self, create_generic_object, create_dashboard_object, create_ou_object,
                                        create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        ts = calendar.timegm(time.gmtime())
        child_ou_name = "automation-child_ou" + str(ts)
        ou_categories_response = create_generic_object.get_category(workspaceID=workspace_id, return_type="response")
        parent_ref_id = ou_categories_response["records"][0]['root_ou_ref_id']
        cat_id = ou_categories_response["records"][0]['id']
        child_ou_resp = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                          parent_ref_id=parent_ref_id)
        ou_list = create_ou_object.get_org_units(ou_categories=cat_id,
                                                 return_type="id")

        all_users = create_ou_object.get_org_users_list()
        valid_users = []
        for each_user in all_users['records']:
            if len(each_user["email"]) != 0:
                valid_users.append(each_user['id'])

        dynamic_user_definition = {"users": valid_users}
        update_ou_integrations = create_ou_object.create_update_ou(name=child_ou_name, ou_category_id=cat_id,
                                                                   parent_ref_id=str(parent_ref_id),
                                                                   ou_id=str(child_ou_resp['success'][0]),
                                                                   dynamic_user_definition=dynamic_user_definition,
                                                                   req_type="put")
        assert update_ou_integrations.text == "ok"
        assert str(child_ou_resp['success'][0]) in ou_list
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(child_ou_resp['success'][0])])
        ou_list_after_delete = create_ou_object.get_org_units(ou_categories=cat_id,
                                                              return_type="id")
        assert str(child_ou_resp['success'][0]) not in ou_list_after_delete

    @pytest.mark.run(order=16)
    @pytest.mark.regression
    @pytest.mark.parametrize("user_type", user_types)
    def test_create_new_propelo_user(self, create_project_object, user_type, create_generic_object, create_ou_object):
        global id, new_user_type
        new_user_response = create_generic_object.create_user(arg_user_type=user_type)
        new_user_id = new_user_response["id"]
        all_users = create_ou_object.get_users_list()
        user_ids = []
        for each_user in all_users['records']:
            user_ids.append(each_user['id'])
            if each_user['id'] == new_user_id:
                id = each_user['id']
                new_user_type = each_user['user_type']
                break

        assert new_user_id in user_ids, "user not created"
        assert new_user_id == id, "User not created"
        assert new_user_type == user_type, "user_type don't match"

        create_generic_object.delete_user(new_user_id)

    @pytest.mark.run(order=17)
    @pytest.mark.regression
    @pytest.mark.parametrize("user_type", user_types)
    def test_create_new_propelo_user_sso_enabled(self, create_project_object, user_type, create_generic_object,
                                             create_ou_object):
        global id, new_user_type, sso_enabled
        new_user_response = create_generic_object.create_user(arg_user_type=user_type, sso_enabled=True)
        new_user_id = new_user_response["id"]
        all_users = create_ou_object.get_users_list()
        user_ids = []
        for each_user in all_users['records']:
            user_ids.append(each_user['id'])
            if each_user['id'] == new_user_id:
                id = each_user['id']
                new_user_type = each_user['user_type']
                sso_enabled = each_user['saml_auth_enabled']
                break

        assert new_user_id in user_ids, "user not created"
        assert new_user_id == id, "User not created"
        assert new_user_type == user_type, "user_type don't match"
        assert sso_enabled, "user_type don't match"
        create_generic_object.delete_user(new_user_id)

    @pytest.mark.run(order=18)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_verify_existing_workspace(self, create_generic_object, create_dashboard_object,
                                                                  create_project_object, get_integration_obj):
        workspace_id = create_generic_object.env["workspace_id"]
        workspace_resp = create_project_object.get_workspace_details(workspace_id)
        assert not workspace_resp['disabled']



