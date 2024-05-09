import logging
import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.testcases.es_vs_db.customer_data_validation_jira.customer_data_helper import CustomerData
from src.lib.postgres_generic_helper.postgres_helper import TestPostgresHelper
from src.testcases.es_vs_db.trellis_optimisation_data_validation.sql_queries import TrellisOptSqlQuery

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj

@pytest.fixture(scope="session", autouse=True)
def create_customer_object():
    customer_object = CustomerData()
    return customer_object

@pytest.fixture(scope="session")
def create_postgres_object(request, create_generic_object):
    postgres_obj = TestPostgresHelper(create_generic_object)
    request.addfinalizer(lambda: postgres_obj.close_postgres_cconnection())
    return postgres_obj

@pytest.fixture(scope="session")
def create_sql_query_object():
    sql_object = TrellisOptSqlQuery()
    return sql_object
