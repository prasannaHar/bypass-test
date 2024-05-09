import logging
import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.testcases.mapping_library.mapping_library_helper import Mapping_library_helper
from src.utils.widget_reusable_functions import WidgetReusable
from src.utils.api_reusable_functions import ApiReusableFunctions

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_mapping_library_object(create_generic_object):
    mapping_librarry_obj = Mapping_library_helper(create_generic_object)
    return mapping_librarry_obj


@pytest.fixture(scope="session", autouse=True)
def create_widget_reusable_funct_object(create_generic_object):
    widget_resuse_func = WidgetReusable(create_generic_object)
    return widget_resuse_func


@pytest.fixture(scope="session", autouse=True)
def create_api_reusable_funct_object(create_generic_object):
    api_reusbale_funct = ApiReusableFunctions(create_generic_object)
    return api_reusbale_funct
