import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisProfilesUpdation:
    generic_object = TGhelper()
    req_changes = generic_object.api_data["trellis_profile_update_tc_config"]
    req_changes_metrics = generic_object.api_data["trellis_profile_update_metrics_tc_config"]

    @pytest.mark.trellistcs
    @pytest.mark.parametrize("req_changes", req_changes)
    @pytest.mark.run(order=1)
    def test_trellis_profiles_update_tc001_feature_settings(self, req_changes, create_widget_helper_object,
                                                        create_trellis_profile_helper_object,
                                                        create_generate_api_payload_object):
        """Validate trellis profile update functionality"""

        LOG.info("==== trellis profile setting updator ====")
        ref_profile_id = create_trellis_profile_helper_object.retrieve_trellis_profiles(
                            profile_name="Software Developer Profile")
        new_profile_id = create_widget_helper_object.trellis_profile_creator(
                            ref_profile_id=ref_profile_id)
        profile_settings = create_widget_helper_object.trellis_profiles_settings_retriever(profile_id=new_profile_id)
        requried_payload = create_generate_api_payload_object.generate_dev_prod_profile_dynamic_payload_settings_change(
                                arg_dev_prod_existing_settings=profile_settings,
                                arg_req_feature_values_needs_to_be_updated_dict=req_changes)
        updated_profile = create_widget_helper_object.trellis_profile_updater(
                        profile_id=new_profile_id,
                        payload=requried_payload)
        ## cleanup
        response = create_widget_helper_object.trellis_profile_deletor(
                            profile_id=updated_profile)
        

    @pytest.mark.trellistcs
    @pytest.mark.parametrize("req_changes_metrics", req_changes_metrics)
    @pytest.mark.run(order=2)
    def test_trellis_profiles_update_tc002_sub_feature_settings(self, req_changes_metrics, create_widget_helper_object,
                                                        create_trellis_profile_helper_object,
                                                        create_generate_api_payload_object):
        """Validate trellis profile update functionality"""

        LOG.info("==== trellis profile setting updator ====")
        ref_profile_id = create_trellis_profile_helper_object.retrieve_trellis_profiles(
                            profile_name="Software Developer Profile")
        new_profile_id = create_widget_helper_object.trellis_profile_creator(
                            ref_profile_id=ref_profile_id)
        profile_settings = create_widget_helper_object.trellis_profiles_settings_retriever(profile_id=new_profile_id)
        requried_payload = create_generate_api_payload_object.generate_dev_prod_profile_dynamic_payload_settings_change(
                                arg_dev_prod_existing_settings=profile_settings,
                                arg_req_sub_feature_values_needs_to_be_updated_dict=req_changes_metrics)
        updated_profile = create_widget_helper_object.trellis_profile_updater(
                        profile_id=new_profile_id,
                        payload=requried_payload)
        ## cleanup
        response = create_widget_helper_object.trellis_profile_deletor(
                            profile_id=updated_profile)
        
