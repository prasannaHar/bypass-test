import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper


@pytest.fixture(autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(autouse=True)
def base_url(create_generic_object):
    # generic_obj = TestGenericHelper()
    base_obj = create_generic_object.connection["base_url"]
    return base_obj
