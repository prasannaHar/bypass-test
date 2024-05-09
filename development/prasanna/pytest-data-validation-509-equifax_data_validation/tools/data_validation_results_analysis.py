import sys
sys.path.append('../')

from src.utils.dev_prod_reusable_functions import *
from src.lib.file_operations import *

# Access command-line arguments
arguments = sys.argv
env = arguments[1]
tenant = arguments[2]
sender_email = arguments[3]
target_emails = arguments[4]
accesscode = arguments[5]

result_analysis_parameters = text_file_retrieve_file_content_fun(arg_req_file="../result/result_analysis_req_params.txt")
env_name, tenant_name, ou_users_considered, \
dp_temp_db_file_name, dp_temp_db_file_name_tbl_tcresults, \
dp_temp_db_file_name_tbl_tcresultdata = result_analysis_parameters.split(",")

dps_tc_results = sqlite3_database_retrieve_table_data(arg_db_file_name=dp_temp_db_file_name,
                        arg_table_name=dp_temp_db_file_name_tbl_tcresults)

dps_tc_results_DF = pd.DataFrame(dps_tc_results)
dps_tc_results_DF = dps_tc_results_DF.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
dps_tc_results_DF_list = dps_tc_results_DF.values.tolist()

dps_tc_result_data = sqlite3_database_retrieve_table_data(arg_db_file_name=dp_temp_db_file_name,
                        arg_table_name=dp_temp_db_file_name_tbl_tcresultdata)

dps_tc_result_data_DF = pd.DataFrame(dps_tc_result_data, columns =["OU user-id", "Time period",  'Feature Name',\
                                                         'Widget Result', "Drill-down Result", "Status"])

dev_prod_result_analysis_and_mailer(
    arg_dps_metrics_result_DF=dps_tc_result_data_DF, 
    arg_dps_test_cases=dps_tc_results_DF_list,
    arg_ou_users_considered=ou_users_considered, 
    arg_env_name=env_name, 
    arg_tenant_name=tenant_name,
    sender_email=sender_email,
    target_emails=target_emails,
    accesscode = accesscode)