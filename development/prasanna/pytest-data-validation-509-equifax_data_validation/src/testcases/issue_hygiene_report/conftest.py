import pytest
from src.lib.generic_helper.generic_helper import *
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.testcases.issue_hygiene_report.issue_hygiene_report_testcase_helper import TestIssueHygieneHelper


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_issue_hygiene_object(create_generic_object):
    testdemo_obj = TestIssueHygieneHelper(create_generic_object)
    return testdemo_obj


@pytest.fixture(scope="session", autouse=True)
def create_widget_helper_object(create_generic_object):
    widget_helper_obj = TestWidgetHelper(create_generic_object)
    return widget_helper_obj

