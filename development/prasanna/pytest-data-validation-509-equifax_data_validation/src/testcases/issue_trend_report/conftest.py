import pytest
from src.lib.generic_helper.generic_helper import *
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.testcases.issue_trend_report.issue_trend_report_helper import TestIssuesTrendReportHelper
from src.utils.widget_schemas import Schemas

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_issue_trend_object(create_generic_object):
    testdemo_obj = TestIssuesTrendReportHelper(create_generic_object)
    return testdemo_obj
