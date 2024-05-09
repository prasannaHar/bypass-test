import logging
import pytest
import itertools

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.testcases.trellis.trellis_user_reports.trellis_user_reports_helper import TestTrellisUserReportHelper as Trellishelper
from src.utils.dev_prod_reusable_functions import dev_prod_results_generation as trellisresult

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TestTrellisUserReports:
    generic_object = TGhelper()
    tchelper_object = Trellishelper(generic_object)
    ou_user_time_periods, ou_user_ids_list = \
            tchelper_object.trellis_tcs_prerequisite_setup(dps_config_data=generic_object.env["dps_test_config"])
    required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))

    @pytest.mark.parametrize("ou_user_id, time_period", required_tests)
    def test_trellis_user_data_consistency_widget_drilldown_on_the_fly_calculation_es(self,create_generic_object,ou_user_id, time_period):
        """Trellis user reports data validation widget v/s drill-down"""

        LOG.info("==== Trellis User Reports data validation across widget and drill-down ====")

        failure_details_if_any = trellisresult(
            arg_ou_user_ids_list=[ou_user_id],
            arg_ou_user_time_periods=[time_period],
            arg_application_url=create_generic_object.connection["base_url"],
            arg_dp_temp_db_file_name="../result/dp_temp_db.db",
            arg_temp_db_file_name_tcresults="tcresults",
            arg_temp_db_file_name_tcresultdata="tcresultdata",
            on_fly_calc=True, force_source="es")

        assert failure_details_if_any.shape[0] == 0, failure_details_if_any

