import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.testcases.es_vs_db.customer_data_validation_jira.customer_data_helper import CustomerData
from src.utils.OU_helper import Ouhelper
from src.testcases.trellis.trellis_widgets.sql_queries import TrellisUserReportSqlQuery
from src.lib.postgres_generic_helper.postgres_helper import TestPostgresHelper
from src.testcases.trellis.trellis_widgets.trellis_raw_stats_helper import TrellisRawStatsHelper

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
def ou_helper_object(create_generic_object):
    testdemo_obj = Ouhelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session")
def create_sql_query_object():
    sql_object = TrellisUserReportSqlQuery()
    return sql_object

@pytest.fixture(scope="session")
def create_postgres_object(request, create_generic_object):
    postgres_obj = TestPostgresHelper(create_generic_object)
    request.addfinalizer(lambda: postgres_obj.close_postgres_cconnection())
    return postgres_obj

@pytest.fixture(scope="session")
def raw_stats_helper_object(create_generic_object):
    postgres_obj = TrellisRawStatsHelper(create_generic_object)
    return postgres_obj
