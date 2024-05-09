import logging

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSampleTestCase:
    
    def test_sei_on_harness_prerequisite_setup(self, harness_prequisite_helper_object):
        """Validate trellis score report functionality"""

        ## 1. create empty resource groups -- cleanup resource if it already exists
        ## 2. update the resource groups as per rbac configuration
        ## 3. create empty user groups -- cleanup user group if already exists
        ## 4. update the user group as per rbac configuration
        ## 5. create empty user- cleanup if user already exists
        ## 6. update the user with required permisssions

        ## 1. create empty resource groups -- cleanup resource if it already exists
        LOG.info("==== resource groups creation ====")
        rgs_creation_status = harness_prequisite_helper_object.resource_groups_creation()
        assert rgs_creation_status==True, "resource groups are not created correctly"
        LOG.info("==== user groups creation ====")
        ugs_creation_status = harness_prequisite_helper_object.user_groups_creation()
        assert ugs_creation_status==True, "user groups are not created correctly"
        u_creation_status = harness_prequisite_helper_object.user_creation()
        assert u_creation_status==True, "users are not created correctly"



