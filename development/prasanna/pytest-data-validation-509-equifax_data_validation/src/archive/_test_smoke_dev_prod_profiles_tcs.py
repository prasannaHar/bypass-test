import os
from copy import deepcopy
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as tghelper
from src.utils.generate_Api_payload import GenericPayload

generic_object = tghelper()
api_payload = GenericPayload()
pytest.application_url = generic_object.connection["base_url"]


class TestSmokedev:
    def test_dev_prod_profile_settings_enable_or_disable_area(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever

        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)

        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)
        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id
        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url
        )
        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)
        ## changing the dev prod profile settings
        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_feature_values_needs_to_be_updated_dict={
                "Impact": ["enabled", False],
                "Leadership & Collaboration": ["enabled", False]
            }
        )

        # print("requried_payload", requried_payload)

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )
        ## dev prod profile changin back to existing settings
        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy
        )
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_settings_weight_changer(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever
        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)

        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)
        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id
        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)
        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)
        ## changing the dev prod profile settings
        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_feature_values_needs_to_be_updated_dict={
                "Volume": ["weight", 5],
                "Proficiency": ["weight", 2]
            }
        )

        # print("requried_payload", requried_payload)
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )
        ## dev prod profile changin back to existing settings

        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy
        )
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_quality_settings_verification(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever
        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)

        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)
        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id

        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)
        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)
        ## settings disable --- changing the dev prod profile settings

        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_sub_feature_values_needs_to_be_updated_dict={
                "Percentage of Rework": ["max_value", 50],
                "Percentage of Legacy Rework": ["enabled", False],
            }
        )
        # print("requried_payload", requried_payload)

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )

        ## dev prod profile changin back to existing settings

        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy
        )
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_impact_settings_verification(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever

        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)
        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)

        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id

        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)
        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)

        ## settings disable --- changing the dev prod profile settings

        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_sub_feature_values_needs_to_be_updated_dict={
                "Major stories resolved per month": ["max_value", 8],
                "Major bugs resolved per month": ["enabled", False],
            }
        )

        # print("requried_payload", requried_payload)
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )

        ## dev prod profile changin back to existing settings

        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy
        )
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_volume_settings_verification(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever

        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)
        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)
        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id

        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)
        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)

        ## settings disable --- changing the dev prod profile settings

        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_sub_feature_values_needs_to_be_updated_dict={
                "Number of PRs per month": ["max_value", 5],
                "Lines of Code per month": ["enabled", False],
                "Number of Story Points delivered per month": ["enabled", False],
            }
        )

        # print("requried_payload", requried_payload)

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )

        ## dev prod profile changin back to existing settings

        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy)

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_speed_settings_verification(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever

        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)

        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)
        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id
        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)

        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)
        ## settings disable --- changing the dev prod profile settings

        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_sub_feature_values_needs_to_be_updated_dict={
                "Average Issue Resolution Time": ["max_value", 3],
                "Average PR Cycle Time": ["enabled", False],
            }
        )

        # print("requried_payload", requried_payload)
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )

        ## dev prod profile changin back to existing settings
        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy
        )

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_proficiency_settings_verification(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever

        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)

        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)
        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id
        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)

        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)
        ## settings disable --- changing the dev prod profile settings

        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_sub_feature_values_needs_to_be_updated_dict={
                "Repo Breadth - Number of unique repo": ["max_value", 6],
                "Technical Breadth - Number of unique file extension": ["enabled", False],
            }
        )

        # print("requried_payload", requried_payload)

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )

        ## dev prod profile changin back to existing settings

        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy
        )

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_leadership_and_collab_settings_verification(self, widgetreusable_object, dev_prod_obj):
        ## existing dev prod settings retriever

        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)
        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)

        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id

        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)
        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)
        ## settings disable --- changing the dev prod profile settings
        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_req_sub_feature_values_needs_to_be_updated_dict={
                "Average response time for PR comments": ["max_value", 2],
                "Number of PRs commented on per month": ["enabled", False],
            }
        )

        # print("requried_payload", requried_payload)

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )
        ## dev prod profile changin back to existing settings

        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy)

        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )

    def test_dev_prod_profile_change_effort_investment_profile_verification(self, widgetreusable_object, dev_prod_obj,
                                                                            create_investmentprofile):
        ## existing dev prod settings retriever
        dev_prod_profie_settings_id = dev_prod_obj.dev_prod_profile_settings_profile_id_retriever(
            arg_app_url=pytest.application_url)

        # print("dev_prod_profie_settings_id", dev_prod_profie_settings_id)
        dev_prod_profile_settings_updater_url = pytest.application_url + "dev_productivity_profiles/" + dev_prod_profie_settings_id
        dev_prd_profile_settings = dev_prod_obj.dev_prod_profile_existing_profile_settings_retriever(
            arg_app_url=pytest.application_url)
        dev_prd_profile_settings_new_copy = deepcopy(dev_prd_profile_settings)
        ## retrive random effort investment profile id
        new_effort_investment_profile_id = create_investmentprofile.investment_profile_retrieve_random_profile(
            arg_app_url=pytest.application_url)
        ## settings disable --- changing the dev prod profile settings

        requried_payload = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings,
            arg_override_effort_investment_profile=new_effort_investment_profile_id
        )

        # print("requried_payload", requried_payload)
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload
        )
        ## dev prod profile changin back to existing settings

        requried_payload_existing = api_payload.generate_dev_prod_profile_dynamic_payload_settings_change(
            arg_dev_prod_existing_settings=dev_prd_profile_settings_new_copy
        )
        api_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=dev_prod_profile_settings_updater_url,
            arg_request_type="put",
            arg_req_payload=requried_payload_existing
        )
