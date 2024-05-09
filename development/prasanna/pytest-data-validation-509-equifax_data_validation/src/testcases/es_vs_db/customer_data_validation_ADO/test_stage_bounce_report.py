import json
import logging
import pandas as pd
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestStageBounceCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["issue-resolution-across_type_ADO"]
    metric_type = generic_object.api_data["metric_type"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    def test_stage_bounce_report(self, across_type, create_generic_object,
                                 create_customer_object, get_integration_obj):
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        stacks = create_generic_object.api_data["issues-report-stacks_ADO"]
        metric = create_generic_object.api_data["metric_type"]
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint.index:
            for stacks_val in stacks:
                for metx in metric:
                    try:
                        filters = {"sort_xaxis": "value_high-low",
                                   "workitem_statuses": ["Done", "Closed", "In Progress"],
                                   "metric": metx,
                                   "product_id": create_generic_object.env["project_id"],
                                   "integration_ids": get_integration_obj}

                        payload = {"filter": filters, "across": across_type, "stacks": [stacks_val],
                                   "ou_ids": [str(df_sprint["ou_id"][val])],
                                   "ou_user_filter_designation": {"sprint": ["sprint_report "]}}
                        LOG.info("payload {} ".format(json.dumps(payload)))
                        es_base_url = create_generic_object.connection[
                                          "base_url"] + create_generic_object.api_data[
                                          "backlog_report"] + "?there_is_no_cache=true&force_source=es"
                        es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                        db_base_url = create_generic_object.connection[
                                          "base_url"] + create_generic_object.api_data[
                                          "backlog_report"] + "?there_is_no_cache=true&force_source=db"
                        db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                        flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                    columns=['key', 'additional_key',
                                                                                             'median', "mean",
                                                                                             'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(across_type)+" "+ str(val)))
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
    @pytest.mark.parametrize("metric_type", metric_type)
    def test_stage_bounce_trend_report(self, metric_type, create_generic_object,
                                       create_customer_object, get_integration_obj):
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint.index:
            try:
                filters = {"agg_type": "total",
                           "workitem_statuses": ["Done", "Closed", "In Progress"],
                           "metric": metric_type,
                           "product_id": create_generic_object.env["project_id"],
                           "integration_ids": get_integration_obj}
                payload = {"filter": filters, "across": "trend",
                           "ou_ids": [str(df_sprint["ou_id"][val])],
                           "ou_user_filter_designation": {"sprint": ["sprint_report "]}}

                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "backlog_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "backlog_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'stage', "mean",
                                                                                     'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(across_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(str(df_sprint["ou_id"][val]))
                if not zero_flag:
                    zero_list.append(str(df_sprint["ou_id"][val]))
            except Exception as ex:
                not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across : " + metric_type + " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across :" + metric_type + "  {}".format(
            set(list_not_match))
