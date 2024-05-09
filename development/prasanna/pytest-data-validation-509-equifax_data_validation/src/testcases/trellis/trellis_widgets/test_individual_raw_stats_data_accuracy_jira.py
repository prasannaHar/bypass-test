import logging
import pytest
import pandas as pd
import json
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.lib.core_reusable_functions import *

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIndividualRawStatsReport:
    generic_object = TGhelper()
    rbacusertype = generic_object.api_data["trellis_rbac_user_types"]
    req_interval = generic_object.env["trellis_intervals"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_individual_raw_stats_report_vs_jira_issues_report(self, create_widget_helper_object,req_interval,ou_helper_object,
                            create_sql_query_object,create_postgres_object,create_generic_object, create_customer_object):
        """Validate individual raw stats widget functionality - negative values check"""
        tenant_name = create_generic_object.connection['tenant_name']
        org_ids = create_generic_object.env["set_trellis_ous"]
        all_users_org_id = create_generic_object.env["all_users_ou"]
        no_of_months, gt, lt = epoch_timeStampsGenerationForRequiredTimePeriods(req_interval)
        not_executed_list = []
        invalid_ou_data_list = []
        inaacuracte_ou_data_list = []
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": req_interval}
                response_data = create_widget_helper_object.create_individual_raw_stats(filters=filters)
                executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(response=response_data,
                                                                                               inside_records=True,
                                                                                               invalid_values_check=True,
                                                                                               raw_stats=True)
                if valid_data_flag:
                    response_data = (response_data["records"][0])["records"]
                    response_df = pd.json_normalize(response_data)
                    temp_data_validation_flg = False
                    if 'raw_stats.Number of bugs worked on per month' in response_df.columns:
                        for index, row in response_df.iterrows():
                            ## retrieve jira display names
                            integration_user_ids = ou_helper_object.retrieve_valid_users(full_name=row["full_name"], )
                            jira_display_names_query= create_sql_query_object.retrieve_jira_display_names(
                                                                tenant=tenant_name, jira_ids=integration_user_ids)
                            jira_display_namesDF = create_postgres_object.execute_query(query = jira_display_names_query, df_flag=True)
                            jira_display_names = jira_display_namesDF["display_name"].tolist()
                            jira_display_names = set(jira_display_names)
                            jira_display_names = list(jira_display_names)
                            ## retrieve user associated integration ids
                            integration_ids = ou_helper_object.retrieve_valid_users(full_name=row["full_name"], integration_ids=True)
                            ## number of bugs worked on per month
                            number_of_prs_trellis = row["raw_stats.Number of bugs worked on per month"]
                            if number_of_prs_trellis > 0:
                                temp_data_validation_flg = True
                                ## retrieve trellis feature list drill-down response -- ticket count
                                trellis_payload = {"sort":[],"filter":{"user_id_type":"ou_user_ids",
                                            "user_id_list":[row["org_user_id"]],"feature_name":"Number of bugs worked on per month",
                                            "time_range":{"$gt": gt,"$lt": lt},"partial":{}},"across":""}
                                trellis_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["trellis_feature_list"]
                                trellis_resp = create_generic_object.execute_api_call(trellis_base_url, "post", data=trellis_payload)
                                number_of_tickets_trellis = (trellis_resp['_metadata'])["total_count"]
                                ## retrieve jira assigne options
                                creator_filter_options = create_generic_object.get_filter_options(
                                    arg_filter_type=["assignee"], 
                                    arg_integration_ids = integration_ids,
                                    req_additional_keys=jira_display_names)
                                ## jira issues report -- tickets count
                                report_payload = {"filter":{"metric":"ticket","sort_xaxis":"value_high-low","issue_types":["BUG"],
                                            "visualization":"bar_chart","assignees":creator_filter_options,"integration_ids":integration_ids,
                                            "issue_resolved_at": {"$gt": gt,"$lt": lt}},"across":"assignee",
                                            "sort":[{"id":"ticket_count","desc":True}],"ou_ids":all_users_org_id,
                                            "ou_user_filter_designation":create_generic_object.env["ou_user_filter_designation"]}
                                base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["jira_tickets_report"]
                                prs_report_resp = create_generic_object.execute_api_call(base_url, "post", data=report_payload)
                                prs_report_resp_df = pd.json_normalize(prs_report_resp['records'], max_level=1)
                                number_of_tickets_report = prs_report_resp_df['total_tickets'].sum()                                
                                if not (-1 <= (number_of_tickets_report - number_of_tickets_trellis) <=1):
                                    inaacuracte_ou_data_list.append(org_id)
                                    break

                    if not temp_data_validation_flg:
                        not_executed_list.append(org_id)
                    
                if not executed_flag:
                    not_executed_list.append(org_id)
                if not valid_data_flag:
                    invalid_ou_data_list.append(org_id)
                if not valid_data_flag:
                    inaacuracte_ou_data_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("Invalid Raw stats values OUs List {}".format(set(invalid_ou_data_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(invalid_ou_data_list) == 0, "invalid data OUs- list is {}".format(set(invalid_ou_data_list))
        assert len(inaacuracte_ou_data_list) == 0, "OUs with inaccuracate data - list is {}".format(set(invalid_ou_data_list))
        


