import json
from src.utils.widget_reusable_functions import WidgetReusable


class DevProdshelper():
    def __init__(self, create_generic):
        self.generic = create_generic
        self.widget = WidgetReusable(self.generic)

    def dev_prod_profile_settings_profile_id_retriever(self, arg_app_url):
        dev_prod_api_url = arg_app_url + "dev_productivity_profiles/default"
        required_payload_content = {}
        api_response = self.widget.retrieve_required_api_response(
            arg_req_api=dev_prod_api_url,
            request_type="post",
            arg_req_payload=required_payload_content)
        dev_prod_settings_id = api_response["id"]
        return dev_prod_settings_id

    def dev_prod_profile_existing_profile_settings_retriever(self, arg_app_url):
        dev_prod_api_url = arg_app_url + "dev_productivity_profiles/default"
        required_payload_content = {}
        api_response = self.widget.retrieve_required_api_response(
            arg_req_api=dev_prod_api_url,
            request_type="post",
            arg_req_payload=required_payload_content)
        dev_prod_settings_id = api_response["id"]

        return dev_prod_settings_id
