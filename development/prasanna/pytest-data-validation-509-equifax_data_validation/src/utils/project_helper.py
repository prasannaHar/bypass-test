from src.utils.widget_reusable_functions import *
from src.utils.generate_Api_payload import *
from src.utils.widget_reusable_functions import WidgetReusable
from src.utils.generate_Api_payload import GenericPayload


class project_helper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.widgetreusable = WidgetReusable(self.generic)
        self.project_details_url = self.generic.connection["base_url"] + "products"
        self.map_integration_url = self.generic.connection["base_url"] + "mappings"
        self.create_dashboard_url = self.generic.connection["base_url"] + "dashboards"
        self.api_payload = GenericPayload()
        self.api_info = self.generic.get_api_info()

    def create_project_without_integration(self, arg_project_name):
        get_user_details = self.generic.get_user_detail()
        user_id = get_user_details["id"]
        print(user_id)
        project_name = arg_project_name
        create_project_payload = self.api_payload.generate_project_payload(
            arg_project_name=project_name,
            arg_project_description="test automation",
            arg_owner_id=user_id,
        )
        create_project_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.project_details_url,
            arg_req_payload=create_project_payload
        )
        return create_project_response

    def add_integration(self, arg_integrations, arg_project_id):
        integration_id = arg_integrations
        product_id = arg_project_id

        map_integration_payload = self.api_payload.generate_map_integration_payload(
            arg_integration_id=integration_id,
            arg_product_id=product_id
        )

        map_integration_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.map_integration_url,
            arg_req_payload=map_integration_payload
        )
        # print(map_integration_response)

    def get_project_details(self, arg_project_id):
        project_details_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.project_details_url + "/" + str(arg_project_id),
            arg_req_payload="",
            request_type="get"
        )
        return project_details_response

    def update_project(self, arg_project_id, arg_updated_desc):
        ts = calendar.timegm(time.gmtime())
        get_project_details_response = self.get_project_details(arg_project_id)
        user_id = get_project_details_response['owner_id']
        project_name = get_project_details_response['name']
        project_key = get_project_details_response['key']

        update_project_payload = self.api_payload.generate_project_update_payload(
            arg_project_id=arg_project_id,
            arg_project_name=project_name,
            arg_project_description=arg_updated_desc,
            arg_owner_id=user_id,
            arg_project_key=project_key
        )
        update_project_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.project_details_url + str(arg_project_id),
            arg_req_payload=update_project_payload,
            request_type="put"
        )
        return json.loads(update_project_response.text)

    def delete_project(self, arg_project_id):
        project_details_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=self.project_details_url + str(arg_project_id),
            request_type="delete",
            arg_req_payload=""
        )
        return project_details_response

    def create_workspace(self, arg_project_name, integrationID, user_type="ADMIN"):
        get_user_details = self.generic.get_user_detail()
        user_id = get_user_details["id"]
        project_name = arg_project_name
        create_project_payload = self.api_payload.generate_workspace_payload(
            arg_project_name=project_name,
            arg_owner_id=user_id,
            integration_id=integrationID
        )
        base_url = self.generic.connection["base_url"] + self.api_info["workspace"]
        resp = self.generic.rbac_user(base_url, "post", data=create_project_payload, user_type=user_type)
        return resp

    def get_workspace_details(self, workspace_id, user_type="ADMIN"):
        base_url = self.generic.connection["base_url"] + self.api_info["workspace"] + "/" + workspace_id
        resp = self.generic.rbac_user(base_url, "get", user_type=user_type)
        return resp
