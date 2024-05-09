import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.archive.bullseye_code_coverage_report.bullseye_code_coverage_helper import TestBullseyeCodeCoverage
from src.utils.widget_schemas import Schemas


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_bullseye_code_coverage_object(create_generic_object):
    testdemo_obj = TestBullseyeCodeCoverage(create_generic_object)
    return testdemo_obj


@pytest.fixture(scope="session", autouse=True)
def create_widget_helper_object(create_generic_object):
    widget_helper_obj = TestWidgetHelper(create_generic_object)
    return widget_helper_obj


@pytest.fixture(scope="session", autouse=True)
def widget_schema_validation():
    widget_schema_obj = Schemas()
    return widget_schema_obj
