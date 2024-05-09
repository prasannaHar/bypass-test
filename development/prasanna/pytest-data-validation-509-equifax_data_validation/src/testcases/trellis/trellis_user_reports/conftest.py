import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.testcases.trellis.trellis_user_reports.trellis_user_reports_helper import TestTrellisUserReportHelper
from src.testcases.es_vs_db.customer_data_validation_jira.customer_data_helper import CustomerData

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_trellis_user_report_object(create_generic_object):
    testdemo_obj = TestTrellisUserReportHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session", autouse=True)
def create_widget_helper_object(create_generic_object):
    widget_helper_obj = TestWidgetHelper(create_generic_object)
    return widget_helper_obj

@pytest.fixture(scope="session", autouse=True)
def create_customer_object():
    customer_object = CustomerData()
    return customer_object
