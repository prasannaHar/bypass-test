import pytest
import logging
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.harness_rbac_setup.rbac_prerequiste_setup import HarnessRBACPrerequisiteSetup

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj

@pytest.fixture(scope="session", autouse=True)
def harness_prequisite_helper_object(create_generic_object):
    testdemo_obj = HarnessRBACPrerequisiteSetup(create_generic_object)
    return testdemo_obj
