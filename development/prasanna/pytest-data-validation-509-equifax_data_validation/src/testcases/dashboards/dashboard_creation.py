import calendar
import time
import pytest
import logging

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestDashboardCreationFlow:

    @pytest.mark.regression
    def test_create_Dashboard_investment_profile_and_units_enabled(self, create_dashboard_object):
        """ dashboard creation functionality """
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "py_auto_" + str(ts)
        ## dashboard creation
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_time_range=True,
            arg_investment_unit=True,
            arg_investment_profile=True)
        dashboard_id = get_create_dashboard_response["id"]
        ## cleanup -- dashboard deletion
        create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.regression
    def test_create_Dashboard_investment_profile_enabled_and_units_disbled(self, create_dashboard_object):
        """ dashboard creation functionality """
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "py_auto_" + str(ts)
        ## dashboard creation
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_time_range=True,
            arg_investment_profile=True)
        dashboard_id = get_create_dashboard_response["id"]
        ## cleanup -- dashboard deletion
        create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.regression
    def test_create_Dashboard_investment_profile_and_units_disabled(self, create_dashboard_object):
        """ dashboard creation functionality """
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "py_auto_" + str(ts)
        ## dashboard creation
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_time_range=True)
        dashboard_id = get_create_dashboard_response["id"]
        ## cleanup -- dashboard deletion
        create_dashboard_object.delete_dashboard(dashboard_id)
