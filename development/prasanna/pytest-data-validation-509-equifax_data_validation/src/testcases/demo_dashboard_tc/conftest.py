import pytest
from src.lib.generic_helper.generic_helper import *
from src.lib.postgres_generic_helper.postgres_helper import TestPostgresHelper
from src.testcases.demo_dashboard_tc.demo_dashoard_helper import TestDemotestHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


# @pytest.fixture(scope="session", autouse=True)
# def create_postgres_object(create_generic_object):
#     postgres_obj = TestPostgresHelper(create_generic_object)
#     return postgres_obj


@pytest.fixture(scope="session", autouse=True)
def create_testdemo_object(create_generic_object):
    testdemo_obj = TestDemotestHelper(create_generic_object)
    return testdemo_obj
