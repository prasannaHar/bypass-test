import pytest
from src.lib.generic_helper.generic_helper import *
from src.testcases.lead_time_single_stat.lead_time_single_stat_testcase_helper import TestLeadSingleStatHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_lead_time_single_stat_object(create_generic_object):
    testdemo_obj = TestLeadSingleStatHelper(create_generic_object)
    return testdemo_obj

