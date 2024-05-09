import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.lib.postgres_generic_helper.postgres_helper import TestPostgresHelper
from src.testcases.scm_widgets.scm_prs_report.scm_prs_report_testcase_helper import TestScmPrsHelper
from src.utils.OU_helper import Ouhelper
from src.utils.dev_prod_profiles_reusable_functions import DevProdshelper
from src.utils.generate_widget_drilldown_payloads import WidgetDrillDown
from src.utils.project_helper import project_helper
from src.utils.retrieve_filter_values_reusable_functions import Filterresuable
from src.utils.investment_profiles_reusable_functions import Investmentprofile
from src.utils.workflow_profiles_reusable_functions import WorkprofileReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session")
def create_postgres_object(request, create_generic_object):
    postgres_obj = TestPostgresHelper(create_generic_object)
    request.addfinalizer(lambda: postgres_obj.close_postgres_cconnection())
    return postgres_obj


@pytest.fixture(scope="session", autouse=True)
def drilldown_object(create_generic_object):
    drill_object = WidgetDrillDown(create_generic_object)
    return drill_object


@pytest.fixture(scope="session", autouse=True)
def create_ou_object(create_generic_object):
    ou_object = Ouhelper(create_generic_object)
    return ou_object


@pytest.fixture(scope="session", autouse=True)
def create_project_object(create_generic_object):
    project_obj = project_helper(create_generic_object)
    return project_obj


@pytest.fixture(scope="session", autouse=True)
def create_filterresuable_object(create_generic_object):
    filterresuable_obj = Filterresuable(create_generic_object)
    return filterresuable_obj


@pytest.fixture(scope="session", autouse=True)
def create_investmentprofile(create_generic_object):
    investmentprofile_obj = Investmentprofile(create_generic_object)
    return investmentprofile_obj


@pytest.fixture(scope="session", autouse=True)
def create_workflow(create_generic_object):
    workflow_obj = WorkprofileReusable(create_generic_object)
    return workflow_obj

@pytest.fixture(scope="session", autouse=True)
def dev_prod_obj(create_generic_object):
    dev_pro_obj = DevProdshelper(create_generic_object)
    return dev_pro_obj


@pytest.fixture(scope="session", autouse=True)
def create_scm_prs_report_object_public(create_generic_object):
    testdemo_obj = TestScmPrsHelper(create_generic_object)
    return testdemo_obj