import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestInvestmentProfilesCreation:

    @pytest.mark.regression
    def test_investment_profile_creation_with_duplicate_indexes(self, widgetreusable_object, create_investment_profile_object):
        """Validate investment profile creation functionality"""

        LOG.info("==== investment profile creation with duplicate indexes====")

        profile_name = "py_auto_" + widgetreusable_object.random_text_generators(arg_length=10)
        profile_creation_status = create_investment_profile_object.investment_profile_creation_duplicate_indexes(
                                            arg_profile_name=profile_name)
        assert profile_creation_status.status_code == 400, "profile creation with duplicate indexes should not be allowed"

