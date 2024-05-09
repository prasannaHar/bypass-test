import logging
import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.testcases.es_vs_db.customer_data_validation_jira.customer_data_helper import CustomerData
from src.testcases.trellis.trellis_profiles.trellis_profiles_helper import TestTrellisProfileHelper as TCHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.generate_Api_payload import GenericPayload

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj

@pytest.fixture(scope="session", autouse=True)
def create_widget_helper_object(create_generic_object):
    testdemo_obj = TestWidgetHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session", autouse=True)
def create_customer_object():
    customer_object = CustomerData()
    return customer_object

@pytest.fixture(scope="session", autouse=True)
def create_trellis_profile_helper_object(create_generic_object):
    profile_helper_object = TCHelper(create_generic_object)
    return profile_helper_object

@pytest.fixture(scope="session", autouse=True)
def create_generate_api_payload_object():
    api_payload = GenericPayload()
    return api_payload
