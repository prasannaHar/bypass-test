import logging
import pytest
import pandas as pd
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.lib.core_reusable_functions import *
import numpy

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIndividualRawStatsReport:
    generic_object = TGhelper()
    rbacusertype = generic_object.api_data["trellis_rbac_user_types"]
    req_interval = generic_object.env["trellis_intervals"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_individual_raw_stats_report_vs_jira_issues(self, create_widget_helper_object,req_interval,ou_helper_object,
                    create_sql_query_object, create_generic_object, create_customer_object,create_postgres_object):
        """Validate individual raw stats widget functionality - negative values check"""
        failure_data = []
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
                    if 'raw_stats.Number of bugs worked on per month' in response_df.columns:
                        for index, row in response_df.iterrows():
                            print(row["full_name"])                            
                            number_of_prs_trellis = row["raw_stats.Number of bugs worked on per month"]
                            if number_of_prs_trellis != 0 and (not numpy.isnan(number_of_prs_trellis)):
                                ## retrieve jira display names
                                integration_user_ids = ou_helper_object.retrieve_valid_users(full_name=row["full_name"])
                                ## retrieve user associated integration ids
                                integration_ids = ou_helper_object.retrieve_valid_users(full_name=row["full_name"], integration_ids=True)
                                jira_display_names_query= create_sql_query_object.retrieve_jira_display_names(
                                                                    tenant=tenant_name, jira_ids=integration_user_ids)
                                jira_display_namesDF = create_postgres_object.execute_query(query = jira_display_names_query, df_flag=True)

                                if "display_name" not in jira_display_namesDF.columns:
                                    continue

                                jira_display_names = jira_display_namesDF["display_name"].tolist()
                                jira_display_names = set(jira_display_names)
                                jira_display_names = list(jira_display_names)

                                jira_in_progress_status_ids_query= create_sql_query_object.retrieve_jira_in_progress_status_ids(
                                                                    tenant=tenant_name, integration_ids=integration_ids)
                                jira_in_progress_status_idsDF = create_postgres_object.execute_query(query = jira_in_progress_status_ids_query, df_flag=True)
                                jira_in_progress_status_ids = jira_in_progress_status_idsDF["status_id"].tolist()

                                ## retrieve trellis feature list drill-down response
                                trellis_payload = {"sort":[],"filter":{"user_id_type":"ou_user_ids",
                                            "user_id_list":[row["org_user_id"]],"feature_name":"Number of bugs worked on per month",
                                            "time_range":{"$gt": gt,"$lt": lt},"partial":{}},"across":""}
                                trellis_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["trellis_feature_list"]
                                trellis_resp = create_generic_object.execute_api_call(trellis_base_url, "post", data=trellis_payload)
                                number_of_tickets_trellis = (trellis_resp['_metadata'])["total_count"]
                                ## Jira Issues Report drill-down
                                report_payload = {"sort":[{"id":"bounces","desc":True}],
                                            "filter":{"metric":"ticket","issue_types":["BUG"],"visualization":"bar_chart",
                                            "integration_ids":integration_ids,"issue_resolved_at":{"$gt": gt,"$lt": lt}},
                                            "across":"assignee","ou_ids":all_users_org_id,"ou_exclusions":["assignees"],
                                            "ou_user_filter_designation":create_generic_object.env["ou_user_filter_designation"]}
                                base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["drill_down_api_url"]
                                jira_issues_resp = create_generic_object.execute_api_call(base_url, "post", data=report_payload)
                                jira_issues_resp_df = pd.json_normalize(jira_issues_resp['records'])
                                jira_issues_resp_df["assignee_list"] = jira_issues_resp_df["assignee_list"].astype(str)
                                filtered_df = jira_issues_resp_df[jira_issues_resp_df['assignee_list'].str.contains('|'.join(jira_display_names))]
                                filtered_df_req_jira_keys = []
                                for index1, row1 in filtered_df.iterrows():
                                    issue_response = create_widget_helper_object.retrieve_issue_details(
                                        integration_id=integration_ids, issue_keys=[row1["key"]])
                                    story_points, ticket_portion_calc, storypoints_portion_calc, assignee_time_calc = create_customer_object.\
                                        trellis_calculate_ado_ticket_portion_v2(workitem_response=issue_response, 
                                                                            assignee_name=jira_display_names,
                                                                            status_ids=jira_in_progress_status_ids)
                                    if assignee_time_calc>0:
                                        filtered_df_req_jira_keys.append(row1["key"])
                                number_of_tickets_report = len(filtered_df_req_jira_keys)
                                ticket_difference = number_of_tickets_report - number_of_tickets_trellis
                                if not (-1.5 <= ticket_difference <=1.5):
                                    failure_data.append([org_id, row["org_user_id"], row["full_name"], number_of_tickets_trellis, number_of_tickets_report, ticket_difference])
                                    inaacuracte_ou_data_list.append(org_id)
                    
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
        if len(failure_data)>=1:
            failure_data_df = pd.DataFrame(failure_data, columns = ["ou_name", "ou_user_id", "ou_full_name", 
                    "number_of_bugs-trellis", "number_of_bugs-report", "difference"])
            failure_data_df.to_csv("log_updates/raw_stats_vs_jira_issues_data_accuracy.csv")

        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(invalid_ou_data_list) == 0, "invalid data OUs- list is {}".format(set(invalid_ou_data_list))
        assert len(inaacuracte_ou_data_list) == 0, "OUs with inaccuracate data - list is {}".format(set(invalid_ou_data_list))

