import pytest
from src.lib.generic_helper.generic_helper import *
from src.testcases.github_api_validation.pygithub import GithubApi


# @pytest.fixture(scope="session", autouse=True)
# def create_generic_object():
#     generic_obj = TestGenericHelper()
#     return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_pygithub_object():
    py_object = GithubApi()
    return py_object
