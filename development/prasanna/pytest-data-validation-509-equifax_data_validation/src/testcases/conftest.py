import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.testcases.es_vs_db.customer_data_validation_jira.customer_data_helper import CustomerData
from src.utils.OU_helper import Ouhelper
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve
from src.utils.datetime_reusable_functions import DateTimeReusable
from src.utils.api_reusable_functions import ApiReusableFunctions
from src.utils.widget_reusable_functions import WidgetReusable
from src.utils.dashboard_helper import DashboardHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.testcases.ado_widgets.ado_widgets_helper import ADOWidgetsHelper
from src.utils.ado_custom_fields_helper import ADOCustomFieldsHelper
from src.testcases.ado_widgets.ado_sprint_reports_helper import ADOSprintReportsHelper
from src.utils.trellis_helper import TrellisHelper
from src.utils.trellis_scores_helper import TrellisScoresHelper
from src.utils.trellis_validations_helper import TrellisValidationsHelper
from src.testcases.trellis.trellis_user_reports.trellis_user_reports_helper_v2 import TrellisUserReportHelperV2
from src.utils.trellis_profile_helper import TrellisProfileHelper
from src.utils.trellis_profile_mapping_helper import TrellisProfileMappingHelper

@pytest.fixture(autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(autouse=True)
def get_integration_obj(create_generic_object):
    # generic_obj = TestGenericHelper()
    integration_obj = create_generic_object.integration_ids_basedon_workspace()
    return integration_obj


@pytest.fixture(autouse=True)
def base_url(create_generic_object):
    # generic_obj = TestGenericHelper()
    base_obj = create_generic_object.connection["base_url"]
    return base_obj


@pytest.fixture(scope="session", autouse=True)
def create_customer_object():
    customer_object = CustomerData()
    return customer_object

@pytest.fixture(autouse=True)
def ou_helper_object(create_generic_object):
    testdemo_obj = Ouhelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def reports_test_params_object():
    report_obj = ReportTestParametersRetrieve()
    return report_obj


@pytest.fixture(autouse=True)
def reports_datetime_utils_object():
    datetime_report_obj = DateTimeReusable()
    return datetime_report_obj


@pytest.fixture(autouse=True)
def api_reusable_functions_object(create_generic_object):
    api_reusbale_funct = ApiReusableFunctions(create_generic_object)
    return api_reusbale_funct

@pytest.fixture(autouse=True)
def widgetreusable_object(create_generic_object):
    resuable_obj = WidgetReusable(create_generic_object)
    return resuable_obj

@pytest.fixture(autouse=True)
def create_dashboard_object(create_generic_object):
    dashboard_obj = DashboardHelper(create_generic_object)
    return dashboard_obj

@pytest.fixture(autouse=True)
def create_widget_helper_object(create_generic_object):
    widget_helper_obj = TestWidgetHelper(create_generic_object)
    return widget_helper_obj

@pytest.fixture(autouse=True)
def ado_widget_helper_object(create_generic_object):
    testdemo_obj = ADOWidgetsHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def ado_custom_field_helper_object(create_generic_object):
    testdemo_obj = ADOCustomFieldsHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def ado_sprint_report_helper_object(create_generic_object):
    testdemo_obj = ADOSprintReportsHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def trellis_helper_object(create_generic_object):
    testdemo_obj = TrellisHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def trellis_scores_helper_object():
    testdemo_obj = TrellisScoresHelper()
    return testdemo_obj

@pytest.fixture(autouse=True)
def trellis_validations_helper_object(create_generic_object):
    testdemo_obj = TrellisValidationsHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def trellis_user_report_v2_helper_object(create_generic_object):
    testdemo_obj = TrellisUserReportHelperV2(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def trellis_profile_helper_object(create_generic_object):
    testdemo_obj = TrellisProfileHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def trellis_profile_mapping_helper_object(create_generic_object):
    testdemo_obj = TrellisProfileMappingHelper(create_generic_object)
    return testdemo_obj

