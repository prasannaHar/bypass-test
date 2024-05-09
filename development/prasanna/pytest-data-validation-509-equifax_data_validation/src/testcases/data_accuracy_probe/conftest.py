import logging
import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.utils.data_accuracy_helper import DataAccuracyHelper
from src.utils.api_reusable_functions import ApiReusableFunctions

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_dataaccuracy_object(create_generic_object):
    data_object = DataAccuracyHelper(create_generic_object)
    return data_object


@pytest.fixture(scope="session", autouse=True)
def create_api_reusable_funct_object(create_generic_object):
    api_reusbale_funct = ApiReusableFunctions(create_generic_object)
    return api_reusbale_funct
