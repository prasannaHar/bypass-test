import logging

from src.utils.OU_helper import Ouhelper
from src.utils.generate_Api_payload import *
from src.utils.widget_reusable_functions import WidgetReusable
from src.utils.generate_Api_payload import GenericPayload
from src.utils.investment_profiles_reusable_functions import Investmentprofile

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class DashboardHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.widgetreusable = WidgetReusable(self.generic)
        self.dashboard_url = self.generic.connection["base_url"] + "dashboards"
        self.ou_group_list = self.generic.connection["base_url"] + "org/groups/list"
        self.export_dashboard_url = self.generic.connection["base_url"] + "dashboard_reports"
        self.api_payload = GenericPayload()
        self.ou_helper = Ouhelper(self.generic)
        self.api_info = self.generic.get_api_info()
        self.investment_profile = Investmentprofile(self.generic)

    def create_dashboard(self, arg_dashboard_name,
                         arg_show_org_unit_selection=False,
                         arg_time_range=False,
                         arg_investment_profile=False,
                         arg_investment_unit=False, ou_ids="auto", workspace_id="auto", user_type="ADMIN"):
        get_user_details = self.generic.get_user_detail()
        owner_id = get_user_details["id"]
        owner_mail = get_user_details["email"]
        if workspace_id == "auto":
            workspace_id = self.generic.env["workspace_id"]
        if ou_ids == "auto":
            ou_ids = self.generic.get_category_ou_id(workspace_id)

        get_create_dashboard_payload = self.api_payload.generate_create_dashboard_payload(
            arg_name=arg_dashboard_name,
            arg_owner_id=owner_id,
            arg_show_org_unit_selection=arg_show_org_unit_selection,
            arg_time_range=arg_time_range,
            arg_investment_profile=arg_investment_profile,
            arg_investment_unit=arg_investment_unit,
            ou_ids=ou_ids,
            owner_mail=owner_mail
        )
        default_investment_profile_id = self.investment_profile.investment_profile_retrieve_random_profile(retrieve_default=True)
        get_create_dashboard_payload["metadata"]["effort_investment_profile_filter"] = default_investment_profile_id
        # LOG.info(get_create_dashboard_payload)
        get_create_dashboard_response = self.generic.rbac_user(
            url=self.dashboard_url,
            request_type="post",
            data=get_create_dashboard_payload,
            user_type=user_type
        )

        # LOG.info(get_create_dashboard_response)
        return get_create_dashboard_response

    def delete_dashboard(self, arg_dashboard_id):
        get_delete_dashboard_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.dashboard_url + "/" + str(arg_dashboard_id),
            request_type="delete",
            arg_req_payload={},
        )
        # LOG.info(self.dashboard_url + str(arg_dashboard_id))
        # LOG.info(get_delete_dashboard_response)
        return get_delete_dashboard_response

    def update_dashboard(self, arg_dashboard_id,
                         arg_dashboard_name,
                         arg_product_id,
                         arg_integration_ids=[],
                         arg_show_org_unit_selection=False,
                         arg_time_range=False,
                         arg_investment_profile=False,
                         arg_investment_unit=False, ):
        get_user_details = self.generic.get_user_detail()
        owner_id = get_user_details["id"]
        user_name = get_user_details["email"]
        get_update_dashboard_payload = self.api_payload.generate_update_dashboard_payload(
            arg_name=arg_dashboard_name,
            arg_integration_ids=arg_integration_ids,
            arg_product_id=arg_product_id,  # no integration project
            arg_owner_id=owner_id,
            arg_show_org_unit_selection=arg_show_org_unit_selection,
            arg_time_range=arg_time_range,
            arg_investment_profile=arg_investment_profile,
            arg_investment_unit=arg_investment_unit,
            arg_req_username=user_name,
            arg_dashboard_id=arg_dashboard_id
        )
        # LOG.info(get_update_dashboard_payload)

        get_update_dashboard_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.dashboard_url + str(arg_dashboard_id),
            arg_req_payload=get_update_dashboard_payload,
            request_type="put"

        )

    def clone_dashboard(self, arg_dashboard_id,
                        arg_dashboard_name,
                        arg_product_id,
                        arg_integration_ids=[],
                        arg_show_org_unit_selection=False,
                        arg_time_range=False,
                        arg_investment_profile=False,
                        arg_investment_unit=False, ):
        get_user_details = self.generic.get_user_detail()
        owner_id = get_user_details["id"]
        user_name = get_user_details["email"]
        get_clone_dashboard_payload = self.api_payload.generate_update_dashboard_payload(
            arg_name=arg_dashboard_name,
            arg_integration_ids=arg_integration_ids,
            arg_product_id=arg_product_id,  # no integration project
            arg_owner_id=owner_id,
            arg_show_org_unit_selection=arg_show_org_unit_selection,
            arg_time_range=arg_time_range,
            arg_investment_profile=arg_investment_profile,
            arg_investment_unit=arg_investment_unit,
            arg_req_username=user_name,
            arg_dashboard_id=arg_dashboard_id
        )
        # LOG.info(get_clone_dashboard_payload)

        get_clone_dashboard_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.dashboard_url + "/" + arg_dashboard_id,
            arg_req_payload=get_clone_dashboard_payload,
            request_type="put"
        )

        return json.loads(get_clone_dashboard_response.text)

    def export_dashboard(self, arg_dashboard_id,
                         arg_dashboard_name,
                         ):
        get_user_details = self.generic.get_user_detail()
        owner_id = get_user_details["id"]
        get_export_dashboard_payload = self.api_payload.generate_export_dashboard_payload(
            arg_dashboard_id=arg_dashboard_id,
            arg_dashboard_name=arg_dashboard_name,
            arg_owner_id=owner_id)

        get_export_dashboard_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.export_dashboard_url,
            arg_req_payload=get_export_dashboard_payload,
        )

        return get_export_dashboard_response

    def ou_ids(self):
        list_ou_id = []
        worksapec_id = self.generic.env["workspace_id"]
        payload = {"filter": {"workspace_id": [worksapec_id]}}
        response = self.generic.execute_api_call(self.ou_group_list, "post", data=payload)
        if response["records"]:
            for ou_id in response["records"]:
                list_ou_id.append(ou_id["id"])
            return list_ou_id
        else:
            LOG.info("ou ids are not present the workspace")
            return None

    def get_available_dashboard(self, workspace_id):
        list_Dashboard_id = []
        base_url = self.generic.connection["base_url"] + "dashboards/list"
        payload = {"page": 0, "page_size": 10000,
                   "filter": {"workspace_id": int(workspace_id), "has_rbac_access": True}}
        response = self.generic.execute_api_call(base_url, "post", data=payload)
        if response["records"]:
            for dash_id in response["records"]:
                list_Dashboard_id.append(dash_id["id"])
            return list_Dashboard_id
        else:
            LOG.info("list_Dashboard_id are not present the workspace")
            return None

    def get_associated_dashboards(self, ou_category_id, return_type="id",user_type="ADMIN"):
        dashboard_ids = []
        baseurl = self.generic.connection["base_url"] + "ous/" + ou_category_id + self.api_info["dashboard_list"]
        payload = {"filter": {"inherited": False}, "page": 0, "page_size": 10000}

        response = self.generic.rbac_user(baseurl, "post", data=payload,user_type=user_type)
        for dash in response["records"]:
            if return_type == "response":
                return response
            else:
                if return_type == "id":
                    dashboard_ids.append(dash['dashboard_id'])
                elif return_type == "name":
                    dashboard_ids.append(dash['name'])
        return dashboard_ids

    def move_associated_dashboard(self, ou_category_id, payload):
        baseurl = self.generic.connection["base_url"] + "ous/" + ou_category_id + "/dashboards"
        response = self.generic.execute_api_call(baseurl, "put", data=payload)
