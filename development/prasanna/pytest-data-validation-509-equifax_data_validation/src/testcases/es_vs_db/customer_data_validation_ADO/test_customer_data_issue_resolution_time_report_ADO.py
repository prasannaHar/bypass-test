import json
import logging
import pandas as pd
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["issue-resolution-across_type_ADO"]
    across_type_ss = generic_object.api_data["issue-resolution-across_type_ss_ADO"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    def test_issue_reolution_time_report_ADO_es(self, across_type,  create_generic_object,
                                                create_customer_object, get_integration_obj):
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint.index:
            try:
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]

                filters = {"metric": ["median_resolution_time", "number_of_tickets_closed"],
                           "workitem_statuses": ["Done", "Closed", "In Progress"],
                           "workitem_resolved_at": {"$gt": gt, "$lt": lt},
                           "sort_xaxis": "value_high-low", "product_id": create_generic_object.env["project_id"],
                           "integration_ids": get_integration_obj}

                payload = {"filter": filters, "across": across_type,
                           "ou_ids": [str(df_sprint["ou_id"][val])],
                           "ou_user_filter_designation": {"sprint": ["sprint_report "]}}
                if len(interval_val) != 0:
                    payload["interval"] = interval_val

                LOG.info("payload {} ".format(payload))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "resolution_time_report_ADO"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "resolution_time_report_ADO"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_ADO(es_response, db_response, across_type,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max", "mean",
                                                                                         "p90", 'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(across_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(str(df_sprint["ou_id"][val]))
                if not zero_flag:
                    zero_list.append(str(df_sprint["ou_id"][val]))
            except Exception as ex:
                not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across : " + across_type + " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across :" + across_type + "  {}".format(
            set(list_not_match))

    @pytest.mark.run(order=2)
    def test_issue_reolution_time_report_trends_ADO_es(self,  create_generic_object,
                                                       create_customer_object, get_integration_obj):

        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint.index:
            try:
                filters = {"metric": ["median_resolution_time", "number_of_tickets_closed"],
                           "workitem_resolved_at": {"$gt": gt, "$lt": lt},
                           "workitem_statuses": ["Done", "Closed", "In Progress"],
                           "sort_xaxis": "value_high-low", "product_id": create_generic_object.env["project_id"],
                           "integration_ids": get_integration_obj}

                payload = {"filter": filters, "across": "trend",
                           "ou_ids": [str(df_sprint["ou_id"][val])],
                           "ou_user_filter_designation": {"sprint": ["sprint_report "]}}
                LOG.info("payload {} ".format(payload))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "resolution_time_report_ADO"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "resolution_time_report_ADO"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_ADO(es_response, db_response,
                                                                                across_type="trend",
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max", "mean",
                                                                                         "p90", 'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(str(df_sprint["ou_id"][val]))
                if not zero_flag:
                    zero_list.append(str(df_sprint["ou_id"][val]))
            except Exception as ex:
                not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across : " + "trend" + " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across :" + "trend" + "  {}".format(
            set(list_not_match))

    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("across_type_ss", across_type_ss)
    def test_issue_reolution_time_report_single_stat_ADO_es(self, across_type_ss,  create_generic_object,
                                                create_customer_object, get_integration_obj):

        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint.index:
            for each_agg_type in agg_type:
                try:
                    filters = {"agg_type": each_agg_type, across_type_ss + "_at": {"$gt": gt, "$lt": lt},
                               "product_id": create_generic_object.env["project_id"], "integration_ids": get_integration_obj}

                    payload = {"filter": filters, "across": across_type_ss,
                               "ou_ids": [str(df_sprint["ou_id"][val])],
                               "ou_user_filter_designation": {"sprint": ["sprint_report "]}}
                    LOG.info("payload {} ".format(payload))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "resolution_time_report_ADO"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "resolution_time_report_ADO"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db_ADO(es_response, db_response,
                                                                                    across_type_ss,
                                                                                    columns=['key', 'additional_key',
                                                                                             'median', "min", "max",
                                                                                             "mean",
                                                                                             "p90", 'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(across_type_ss)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(str(df_sprint["ou_id"][val]))
                    if not zero_flag:
                        zero_list.append(str(df_sprint["ou_id"][val]))
                except Exception as ex:
                    not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across : " + across_type_ss + " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across :" + across_type_ss + "  {}".format(
            set(list_not_match))
