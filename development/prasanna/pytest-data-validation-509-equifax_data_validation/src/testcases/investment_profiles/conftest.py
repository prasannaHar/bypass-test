import logging
import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.utils.investment_profiles_reusable_functions import Investmentprofile
from src.testcases.investment_profiles.effort_investment_helper import TestEffortInvestmentReportHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_investment_profile_object(create_generic_object):
    investmentprofile_obj = Investmentprofile(create_generic_object)
    return investmentprofile_obj


@pytest.fixture(scope="session", autouse=True)
def create_effort_helper_object(create_generic_object):
    investmentprofile_obj = TestEffortInvestmentReportHelper(create_generic_object)
    return investmentprofile_obj
