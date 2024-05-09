import logging
from pandas.io import json

from src.utils.generate_Api_payload import GenericPayload
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class Investmentprofile:
    def __init__(self, generic):
        self.generic = generic
        self.api_payload = GenericPayload()
        self.widgetreuse = WidgetReusable(self.generic)

    def investment_profile_creation(self, arg_app_url, arg_profile_name):
        LOG.info("==== generating the payload based on the required workflow profile type ===")
        required_payload_content = self.api_payload.generate_investment_profile_creation_payload(
            arg_required_profile_name=arg_profile_name
        )

        profile_creation_api = arg_app_url + "ticket_categorization_schemes"
        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=profile_creation_api,
            request_type="post",
            arg_req_payload=required_payload_content)
        return api_response

    def investment_profile_creation_duplicate_indexes(self, arg_profile_name):
        LOG.info("==== generating the payload based on the required workflow profile type ===")
        app_url = self.generic.connection["base_url"]
        ## retrieve default profile id
        default_profile_id = self.investment_profile_retrieve_random_profile(
                                        arg_app_url=app_url, retrieve_default=True)
        default_profile_config = self.investment_profile_retrieve_profile_information(
                                        arg_app_url=app_url, 
                                        arg_profile_id_to_be_used=default_profile_id)        
        ## making the required payload with duplicate indexes
        del default_profile_config['id']
        categories = default_profile_config["config"]["categories"]
        for each_catgory in categories.keys():
            temp_dict = categories[each_catgory]
            temp_dict['index'] = 1
            categories[each_catgory] = temp_dict
        default_profile_config["config"]["categories"] = categories
        ## investment profile creation
        profile_creation_api = app_url + "ticket_categorization_schemes"
        profile_creation_response = self.generic.execute_api_call(profile_creation_api, "post", data=default_profile_config)
        return profile_creation_response

    def investment_profile_clone_default_profile(self, target_profile):
        LOG.info("==== generating the payload based on the required workflow profile type ===")
        app_url = self.generic.connection["base_url"]
        ## retrieve default profile id
        default_profile_id = self.investment_profile_retrieve_random_profile(
                                        arg_app_url=app_url, retrieve_default=True)
        default_profile_config = self.investment_profile_retrieve_profile_information(
                                        arg_app_url=app_url, 
                                        arg_profile_id_to_be_used=default_profile_id)        
        ## making the required payload with duplicate indexes
        del default_profile_config['id']
        default_profile_config["name"] = target_profile
        default_profile_config["default_scheme"] =  False
        ## investment profile creation
        profile_creation_api = app_url + "ticket_categorization_schemes"
        profile_creation_response = self.generic.execute_api_call(profile_creation_api, "post", data=default_profile_config)
        return profile_creation_response['id']

    def investment_profile_update_duplicate_indexes(self, arg_profile_name):
        LOG.info("==== generating the payload based on the required workflow profile type ===")
        app_url = self.generic.connection["base_url"]
        profile_id = self.investment_profile_clone_default_profile(target_profile=arg_profile_name)
        ## cloned profile information
        cloned_profile_config = self.investment_profile_retrieve_profile_information(
                                        arg_app_url=app_url, 
                                        arg_profile_id_to_be_used=profile_id)        
        ## making the required payload with duplicate indexes
        categories = cloned_profile_config["config"]["categories"]
        for each_catgory in categories.keys():
            temp_dict = categories[each_catgory]
            temp_dict['index'] = 1
            categories[each_catgory] = temp_dict
        cloned_profile_config["config"]["categories"] = categories
        ## investment profile update
        profile_update_api = app_url + "ticket_categorization_schemes/" + profile_id
        profile_updation_response = self.generic.execute_api_call(profile_update_api, "put", data=cloned_profile_config)
        ## clean up
        self.investment_profile_deletion(arg_app_url=app_url, arg_profile_id_to_be_deleted=profile_id)
        return profile_updation_response


    def investment_profile_retrieve_random_profile(self, arg_app_url=None, retrieve_default=False):

        if not arg_app_url:
            arg_app_url = self.generic.connection["base_url"]
        required_payload_content = {}
        profile_creation_api = arg_app_url + "ticket_categorization_schemes/list"
        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=profile_creation_api,
            request_type="post",
            arg_req_payload=required_payload_content)

        retrieve_random_investment_profile = self.widgetreuse.retrieve_three_random_records(
            arg_inp_json_response=api_response,
            number_records_required=1
        )

        required_investment_profile = retrieve_random_investment_profile[0]

        if retrieve_default:
            profile_ids_list = (api_response["records"])
            for each_profile in profile_ids_list:
                if each_profile["default_scheme"]:
                    return each_profile["id"]

        return required_investment_profile["id"]

    def investment_profile_deletion(self, arg_app_url, arg_profile_id_to_be_deleted):
        required_payload_content = {}
        profile_creation_api = arg_app_url + "ticket_categorization_schemes/" + arg_profile_id_to_be_deleted

        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=profile_creation_api,
            request_type="delete",
            arg_req_payload=required_payload_content)
        return api_response

    def investment_profile_retrieve_profile_information(self, arg_app_url, arg_profile_id_to_be_used):

        required_payload_content = {}
        default_profile_making_api = arg_app_url + "ticket_categorization_schemes/" + arg_profile_id_to_be_used

        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=default_profile_making_api,
            request_type="get",
            arg_req_payload=required_payload_content)
        return api_response

    def investment_profile_change_default_profile(self, arg_app_url, arg_profile_id_to_be_used):
        required_payload_content = self.investment_profile_retrieve_profile_information(
            arg_app_url=arg_app_url,
            arg_profile_id_to_be_used=arg_profile_id_to_be_used
        )
        del required_payload_content['updated_at']
        del required_payload_content['created_at']
        required_payload_content['default_scheme'] = True

        default_profile_making_api = arg_app_url + "ticket_categorization_schemes/" + arg_profile_id_to_be_used

        api_response = self.widgetreuse.retrieve_required_api_response(
            arg_req_api=default_profile_making_api,
            request_type="put",
            arg_req_payload=required_payload_content)
        return json.loads(api_response.text)

