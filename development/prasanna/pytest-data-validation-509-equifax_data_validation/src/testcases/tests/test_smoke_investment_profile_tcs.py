import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestInvestmentprofile:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    def test_investment_profile_creation(self, create_generic_object, widgetreusable_object, create_investmentprofile):
        workflow_profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)
        LOG.info("====  profile creation ===")
        profile_creation_api_response = create_investmentprofile.investment_profile_creation(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_name=workflow_profile_name
        )
        assert profile_creation_api_response['id'], profile_creation_api_response
        LOG.info("====  clean up ===")
        profile_id = profile_creation_api_response['id']
        profile_deletion_api_response = create_investmentprofile.investment_profile_deletion(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_id_to_be_deleted=profile_id
        )
        assert profile_deletion_api_response == ''

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    def test_investment_profile_deletion(self, create_generic_object, widgetreusable_object, create_investmentprofile):
        workflow_profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)

        LOG.info("====  profile creation ===")
        profile_creation_api_response = create_investmentprofile.investment_profile_creation(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_name=workflow_profile_name
        )

        assert profile_creation_api_response, profile_creation_api_response

        LOG.info("====  clean up ===")
        profile_id = profile_creation_api_response['id']
        profile_deletion_api_response = create_investmentprofile.investment_profile_deletion(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_id_to_be_deleted=profile_id
        )
        assert profile_creation_api_response,  profile_deletion_api_response

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_investment_profile_making_non_default_profile_to_default_profile(self,create_generic_object, widgetreusable_object,
                                                                              create_investmentprofile):
        workflow_profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)

        LOG.info("====  profile creation ===")
        profile_creation_api_response = create_investmentprofile.investment_profile_creation(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_name=workflow_profile_name
        )

        assert profile_creation_api_response, profile_creation_api_response

        LOG.info("====  retrieve default profile id ===")
        default_profile_id = create_investmentprofile.investment_profile_retrieve_random_profile(
            arg_app_url=create_generic_object.connection["base_url"],
            retrieve_default=True
        )

        assert default_profile_id, profile_creation_api_response
        LOG.info("====  changing the default profile ===")
        profile_id = profile_creation_api_response['id']
        # LOG.info("default_profile_id", default_profile_id)
        default_profile_making_response = create_investmentprofile.investment_profile_change_default_profile(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_id_to_be_used=profile_id
        )
        assert default_profile_making_response, default_profile_making_response

        LOG.info("====  reverting back the default profile ===")
        default_profile_making_response = create_investmentprofile.investment_profile_change_default_profile(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_id_to_be_used=default_profile_id
        )

        assert default_profile_making_response, default_profile_making_response

        profile_deletion_api_response = create_investmentprofile.investment_profile_deletion(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_id_to_be_deleted=profile_id
        )
        assert profile_deletion_api_response == ''
