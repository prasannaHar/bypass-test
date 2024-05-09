import pytest
from src.lib.generic_helper.generic_helper import *
from src.lib.postgres_generic_helper.postgres_helper import TestPostgresHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.testcases.issue_backlog_trend_report.issue_backlog_trend_report_testcase_helper import TestIssueBacklogHelper
from src.utils.widget_schemas import Schemas

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_issue_backlog_object(create_generic_object):
    testdemo_obj = TestIssueBacklogHelper(create_generic_object)
    return testdemo_obj


@pytest.fixture(scope="session", autouse=True)
def create_widget_helper_object(create_generic_object):
    widget_helper_obj = TestWidgetHelper(create_generic_object)
    return widget_helper_obj


@pytest.fixture(scope="session", autouse=True)
def widget_schema_validation():
    widget_schema_obj = Schemas()
    return widget_schema_obj
