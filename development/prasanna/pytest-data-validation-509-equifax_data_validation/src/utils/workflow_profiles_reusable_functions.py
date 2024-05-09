import json
import logging

from src.utils.generate_Api_payload import GenericPayload
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class WorkprofileReusable:
    def __init__(self, create_gen):
        self.generic = create_gen
        self.api_data = self.generic.get_api_info()
        self.widgetreuse = WidgetReusable(self.generic)
        self.genericpayload = GenericPayload()

    def workflow_profile_creation(self, arg_app_url, arg_profile_type, arg_profile_name):
        required_payload_content = {}
        LOG.info("=== generating the payload based on the required workflow profile type ===")

        if arg_profile_type == "ticket_created":
            required_payload_content = self.genericpayload.generate_workflow_profile_tickets_created_payload(
                arg_required_profile_name=arg_profile_name
            )
        else:
            return False, "incorrect workflow profile type"
        LOG.info("=== generating the payload string ===")
        payload = required_payload_content
        profile_creation_api = arg_app_url + "velocity_configs"
        api_response = self.widgetreuse.retrieve_required_api_response(

            arg_req_api=profile_creation_api,
            request_type="post",
            arg_req_payload=payload)

        return api_response

    def workflow_profile_deletion(self,arg_app_url, arg_profile_id_to_be_deleted):
        required_payload_content = {}
        # payload = json.dumps(required_payload_content)
        profile_creation_api = arg_app_url + "velocity_configs/" + arg_profile_id_to_be_deleted
        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=profile_creation_api,
            request_type="delete",
            arg_req_payload=required_payload_content)
        return True

    def workflow_profile_retrieve_default_profile(self, arg_app_url):

        required_payload_content = {}
        # payload = json.dumps(required_payload_content)
        profile_creation_api = arg_app_url + self.generic.api_data["velocity_configs_list"]
        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=profile_creation_api,
            request_type="post",
            arg_req_payload=required_payload_content)

        if api_response["records"]:
            profile_ids_list = (api_response["records"])
            for each_profile in profile_ids_list:
                if each_profile["default_config"]:
                    return each_profile["id"]

        return api_response

    def workflow_profile_change_default_profile(self, arg_app_url, arg_profile_id_to_be_used):

        required_payload_content = {}
        default_profile_making_api = arg_app_url + "velocity_configs/" + arg_profile_id_to_be_used + "/set-default"
        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=default_profile_making_api,
            request_type="patch",
            arg_req_payload=required_payload_content)
        return api_response

    def workflow_profile_retrieve_profile_information(self, arg_app_url, arg_profile_id_to_be_used):
        required_payload_content = {}
        default_profile_making_api = arg_app_url + "velocity_configs/" + arg_profile_id_to_be_used
        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=default_profile_making_api,
            request_type="get",
            arg_req_payload=required_payload_content)

        return api_response

    def workflow_profile_add_dummy_stage_to_exising_profile(self, arg_app_url, arg_profile_id_to_be_used):

        LOG.info("=== retrieving the existing profile information ===")

        existing_profile_response = self.workflow_profile_retrieve_profile_information(
            arg_app_url=arg_app_url,
            arg_profile_id_to_be_used=arg_profile_id_to_be_used
        )
        edit_profile_required_payload = self.genericpayload.generate_workflow_profile_tickets_created_update_payload(
            arg_existing_profile_payload=existing_profile_response
        )
        default_profile_making_api = arg_app_url + "velocity_configs/" + arg_profile_id_to_be_used
        # print("edit_profile_required_payload", edit_profile_required_payload)
        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=default_profile_making_api,
            request_type="put",
            arg_req_payload=edit_profile_required_payload)
        return json.loads(api_response.text)



