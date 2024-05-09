import logging
import pytest
import pandas as pd
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.lib.core_reusable_functions import *

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIndividualRawStatsReport:
    generic_object = TGhelper()
    rbacusertype = generic_object.api_data["trellis_rbac_user_types"]
    req_interval = generic_object.env["trellis_intervals"]

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_individual_raw_stats_report_vs_scm_rework(self, create_widget_helper_object,req_interval,ou_helper_object,
                                                                    create_generic_object, create_customer_object, 
                                                                    raw_stats_helper_object):
        """Validate individual raw stats widget functionality - negative values check"""
        org_ids = create_generic_object.env["set_trellis_ous"]
        all_users_org_id = create_generic_object.env["all_users_ou"]
        ## fetching the required integration ids
        ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(all_users_org_id)
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
                    response_df = response_df.fillna(0)
                    if 'raw_stats' in response_df.columns:
                        for index, row in response_df.iterrows():
                            integration_user_ids = ou_helper_object.retrieve_valid_users(full_name=row["full_name"])
                            number_of_prs_trellis = raw_stats_helper_object.retreive_metric_raw_stats(
                                                                raw_stats=row['raw_stats'],
                                                                req_metric="Percentage of Rework")
                            if number_of_prs_trellis != 0:
                                ## trellis scm rework drilldown 
                                trellis_payload = {"sort":[],"filter":{"user_id_type":"ou_user_ids",
                                            "user_id_list":[row["org_user_id"]],"feature_name":"Percentage of Rework",
                                            "time_range":{"$gt": gt,"$lt": lt},"partial":{}},"across":""}
                                trellis_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["trellis_feature_list"]
                                trellis_resp = create_generic_object.execute_api_call(trellis_base_url, "post", data=trellis_payload)
                                number_of_prs_trellis_drilldown = (trellis_resp['_metadata'])["total_count"]
                                author_information = ((trellis_resp['records'][0])["records"][0])["author_info"]
                                integration_user_ids.append(author_information["cloudId"])
                                integration_user_ids = list(set(integration_user_ids))
                                display_name = author_information["display_name"]
                                if display_name != "null":
                                    integration_user_ids.append(display_name)
                                creator_filter_options = create_generic_object.get_filter_options_scm(
                                    arg_filter_type="author", 
                                    report_type="commits",
                                    req_additional_keys=integration_user_ids)
                                if display_name == "null":
                                    creator_filter_options.append(author_information["id"])
                                report_payload = {"filter":{"authors":creator_filter_options,"integration_ids":ou_based_integration_ids,
                                                "include_metrics":True,"committed_at":{"$gt": gt,"$lt": lt}},
                                                "across":"author","ou_ids":all_users_org_id,"ou_exclusions":["authors"]}
                                base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_commits_list_drilldown"]
                                prs_report_resp = create_generic_object.execute_api_call(base_url, "post", data=report_payload)
                                number_of_prs_report = (prs_report_resp['_metadata'])["total_count"]
                                if not (-1.5 <= (number_of_prs_report - number_of_prs_trellis_drilldown) <=1.5):
                                    inaacuracte_ou_data_list.append(org_id +" " + str(integration_user_ids))
                    
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

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_individual_raw_stats_report_vs_scm_legacy_rework(self, create_widget_helper_object,req_interval,ou_helper_object,
                                                                    create_generic_object, create_customer_object,
                                                                    raw_stats_helper_object):
        """Validate individual raw stats widget functionality - negative values check"""
        org_ids = create_generic_object.env["set_trellis_ous"]
        all_users_org_id = create_generic_object.env["all_users_ou"]
        ## fetching the required integration ids
        ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(all_users_org_id)
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
                    response_df = response_df.fillna(0)
                    if 'raw_stats' in response_df.columns:
                        for index, row in response_df.iterrows():
                            integration_user_ids = ou_helper_object.retrieve_valid_users(full_name=row["full_name"])
                            number_of_prs_trellis = raw_stats_helper_object.retreive_metric_raw_stats(
                                                                raw_stats=row['raw_stats'],
                                                                req_metric="Percentage of Legacy Rework")
                            if number_of_prs_trellis != 0:
                                ## trellis scm rework drilldown 
                                trellis_payload = {"sort":[],"filter":{"user_id_type":"ou_user_ids",
                                            "user_id_list":[row["org_user_id"]],"feature_name":"Percentage of Legacy Rework",
                                            "time_range":{"$gt": gt,"$lt": lt},"partial":{}},"across":""}
                                trellis_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["trellis_feature_list"]
                                trellis_resp = create_generic_object.execute_api_call(trellis_base_url, "post", data=trellis_payload)
                                number_of_prs_trellis = (trellis_resp['_metadata'])["total_count"]
                                ## retrieve scm commit author options
                                trellis_payload = {"sort":[],"filter":{"user_id_type":"ou_user_ids",
                                            "user_id_list":[row["org_user_id"]],"feature_name":"Number of Commits per month",
                                            "time_range":{"$gt": gt,"$lt": lt},"partial":{}},"across":""}
                                trellis_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["trellis_feature_list"]
                                trellis_resp = create_generic_object.execute_api_call(trellis_base_url, "post", data=trellis_payload)
                                author_information = ((trellis_resp['records'][0])["records"][0])["author_info"]
                                # creator_filter_options = [author_information["id"]]
                                integration_user_ids.append(author_information["cloudId"])
                                integration_user_ids = list(set(integration_user_ids))
                                display_name = author_information["display_name"]
                                if display_name != "null":
                                    integration_user_ids.append(display_name)
                                creator_filter_options = create_generic_object.get_filter_options_scm(
                                    arg_filter_type="author", 
                                    report_type="commits",
                                    req_additional_keys=integration_user_ids)
                                if display_name == "null":
                                    creator_filter_options.append(author_information["id"])
                                report_payload = {"filter":{"authors":creator_filter_options,"integration_ids":ou_based_integration_ids,
                                                "include_metrics":True,"committed_at":{"$gt": gt,"$lt": lt}},
                                                "across":"author","ou_ids":all_users_org_id,"ou_exclusions":["authors"]}
                                base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_commits_list_drilldown"]
                                prs_report_resp = create_generic_object.execute_api_call(base_url, "post", data=report_payload)
                                number_of_prs_report = (prs_report_resp['_metadata'])["total_count"]
                                if not (-1.5 <= (number_of_prs_report - number_of_prs_trellis) <=1.5):
                                    inaacuracte_ou_data_list.append(org_id +" " + str(integration_user_ids))
                    
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
