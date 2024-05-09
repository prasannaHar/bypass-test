import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.utils.OU_helper import Ouhelper
from src.utils.generate_widget_drilldown_payloads import WidgetDrillDown
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj


@pytest.fixture(scope="session", autouse=True)
def create_ou_object(create_generic_object):
    ou_object = Ouhelper(create_generic_object)
    return ou_object


@pytest.fixture(scope="session", autouse=True)
def widgetreusable_object(create_generic_object):
    resuable_obj = WidgetReusable(create_generic_object)
    return resuable_obj


@pytest.fixture(scope="session", autouse=True)
def drilldown_object(create_generic_object):
    drill_object = WidgetDrillDown(create_generic_object)
    return drill_object
