import logging
import os
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.dev_prod_reusable_functions import dev_prod_fetch_input_data
from src.lib.file_operations import text_file_creation_fun
from src.lib.sqlite3_db_operations import *


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisUserReportHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)

    def trellis_tcs_prerequisite_setup(self, dps_config_data):

        ou_user_time_periods, ou_user_ids_list = \
            dev_prod_fetch_input_data(arg_config_file_data=dps_config_data)
        pytest.dpstccounter = 0
        dp_temp_db_file_name = "../result/dp_temp_db.db"
        if os.path.exists(dp_temp_db_file_name):
            os.remove(dp_temp_db_file_name)

        sqlite3_database_creation(arg_db_name=dp_temp_db_file_name)
        sqlite3_database_create_table(arg_db_file_name=dp_temp_db_file_name,
                                    arg_table_name="tcresults",
                                    arg_column_names=["Feature_TestScenario", "Result"])
        sqlite3_database_create_table(arg_db_file_name=dp_temp_db_file_name,
                                    arg_table_name="tcresultdata",
                                    arg_column_names=["OU_UserId", "TimePeriod", "FeatureName", 
                                        "WidgetResult", "DrilldownResult", "Status"])
        text_file_creation_fun(
            arg_file_name="../result/result_analysis_req_params.txt",
            arg_file_content= self.generic.connection['env_name'] + "," + self.generic.connection['tenant_name']\
                + "," + str(len(ou_user_ids_list)) + "," + dp_temp_db_file_name + ",tcresults,tcresultdata")

        return ou_user_time_periods, ou_user_ids_list

