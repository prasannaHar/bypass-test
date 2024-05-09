import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.equifax.agile_raw_data import AgileData
from src.utils.widget_reusable_functions import WidgetReusable


@pytest.fixture(autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(autouse=True)
def base_url(create_generic_object):
    # generic_obj = TestGenericHelper()
    base_obj = create_generic_object.connection["base_url"]
    return base_obj


@pytest.fixture(autouse=True)
def agile_raw_data_obj(create_generic_object):
    agile_obj = AgileData(create_generic_object)
    return agile_obj


@pytest.fixture(autouse=True)
def widgetreusable_object(create_generic_object):
    resuable_obj = WidgetReusable(create_generic_object)
    return resuable_obj