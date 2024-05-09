import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestWorkflowProfile:
    @pytest.mark.run(order=1)
    def test_workflow_profile_creation_ticket_created(self, widgetreusable_object, create_generic_object,
                                                      create_workflow):

        workflow_profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)
        LOG.info("===  profile creation ===")
        profile_creation_api_response = create_workflow.workflow_profile_creation(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_type="ticket_created",
            arg_profile_name=workflow_profile_name
        )
        assert profile_creation_api_response, profile_creation_api_response

        LOG.info("===  clean up ===")

        if profile_creation_api_response:
            profile_id = profile_creation_api_response['id']
            profile_deletion_api_response = create_workflow.workflow_profile_deletion(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_profile_id_to_be_deleted=profile_id
            )
            assert profile_deletion_api_response, profile_deletion_api_response

    @pytest.mark.run(order=2)
    def test_workflow_profile_deletion_ticket_created(self, widgetreusable_object, create_workflow, create_generic_object):
        workflow_profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)
        LOG.info("===  profile creation ===")

        profile_creation_api_response = create_workflow.workflow_profile_creation(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_type="ticket_created",
            arg_profile_name=workflow_profile_name
        )

        assert profile_creation_api_response, profile_creation_api_response

        LOG.info("===  clean up ===")

        if profile_creation_api_response:
            profile_id = profile_creation_api_response['id']
            profile_deletion_api_response = create_workflow.workflow_profile_deletion(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_profile_id_to_be_deleted=profile_id
            )
            assert profile_deletion_api_response, profile_deletion_api_response

    @pytest.mark.run(order=3)
    def test_workflow_profile_making_non_default_profile_to_default_profile(self, create_workflow,
                                                                            widgetreusable_object, create_generic_object):

        workflow_profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)
        LOG.info("===  profile creation ===")
        profile_creation_api_response = create_workflow.workflow_profile_creation(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_type="ticket_created",
            arg_profile_name=workflow_profile_name
        )

        assert profile_creation_api_response, profile_creation_api_response

        LOG.info("===  retrieve default profile id ===")

        default_profile_id = create_workflow.workflow_profile_retrieve_default_profile(
            arg_app_url=create_generic_object.connection["base_url"],
        )

        assert profile_creation_api_response, profile_creation_api_response
        if profile_creation_api_response['id']:
            profile_id = profile_creation_api_response['id']
            LOG.info("default_profile_id {}".format(default_profile_id))
            default_profile_making_response = create_workflow.workflow_profile_change_default_profile(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_profile_id_to_be_used=profile_id
            )
            assert default_profile_making_response, default_profile_making_response
            LOG.info("===  reverting back the default profile ===")
            default_profile_making_response = create_workflow.workflow_profile_change_default_profile(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_profile_id_to_be_used=default_profile_id
            )
            assert default_profile_making_response, default_profile_making_response

            profile_deletion_api_response = create_workflow.workflow_profile_deletion(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_profile_id_to_be_deleted=profile_id
            )
            assert profile_deletion_api_response, profile_deletion_api_response

    @pytest.mark.run(order=4)
    def test_workflow_profile_edit_ticket_created(self, widgetreusable_object, create_workflow, create_generic_object):

        workflow_profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)

        LOG.info("===  profile creation ===")
        profile_creation_api_response = create_workflow.workflow_profile_creation(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_type="ticket_created",
            arg_profile_name=workflow_profile_name
        )

        newly_created_profile_id = profile_creation_api_response["id"]
        # LOG.info("profile_creation_api_response", profile_creation_api_response)
        assert profile_creation_api_response, profile_creation_api_response

        LOG.info("===  editing the exising profile ===")

        profile_edit_response = create_workflow.workflow_profile_add_dummy_stage_to_exising_profile(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_id_to_be_used=newly_created_profile_id)

        LOG.info("===  clean up ===")

        profile_deletion_api_response = create_workflow.workflow_profile_deletion(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_profile_id_to_be_deleted=newly_created_profile_id
        )

        assert profile_deletion_api_response, profile_deletion_api_response
