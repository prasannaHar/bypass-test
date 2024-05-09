import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisProfiles:

    @pytest.mark.trellistcs
    @pytest.mark.run(order=1)
    def test_trellis_profiles_tc001_profile_clone(self, create_widget_helper_object,
                                                        create_trellis_profile_helper_object):
        """Validate trellis profile clone functionality"""

        LOG.info("==== trellis profile cloning ====")
        ## existing profile id retriever
        profile_id = create_trellis_profile_helper_object.retrieve_trellis_profiles(
                            profile_name="Software Developer Profile")
        ## cloning existing profile
        new_profile_id = create_widget_helper_object.trellis_profile_creator(
                            ref_profile_id=profile_id)
        ## cleanup
        response = create_widget_helper_object.trellis_profile_deletor(
                            profile_id=new_profile_id)

    @pytest.mark.trellistcs
    @pytest.mark.run(order=2)
    def test_trellis_profiles_tc002_profile_deletor(self, create_widget_helper_object,
                                                        create_trellis_profile_helper_object):
        """Validate trellis profile clone functionality"""

        LOG.info("==== trellis profile cloning ====")
        ## existing profile id retriever
        ref_profile_id = create_trellis_profile_helper_object.retrieve_trellis_profiles(
                            profile_name="Software Developer Profile")
        response = create_widget_helper_object.trellis_profile_deletor(
                            profile_id=ref_profile_id, 
                            new_profile_deletion=True)
