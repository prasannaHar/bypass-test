import logging
import pandas as pd
import pytest
import json

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestBounceCustomer:
    generic_object = TGhelper()
    time_period = generic_object.api_data["time_period"]
    across_type = generic_object.api_data["issue-resolution-across_type_ADO"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("time_period", time_period)
    def test_bounce_single_stat(self, time_period,  create_generic_object,
                                create_customer_object, get_integration_obj):
        aggr_types = ["average", "total"]
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint.index:
            for aggr_type in aggr_types:
                try:
                    filters = {"agg_type": aggr_type, "time_period": time_period,
                               "product_id": create_generic_object.env["project_id"],
                               "integration_ids": get_integration_obj}

                    payload = {"filter": filters, "across": "trends",
                               "ou_ids": [str(df_sprint["ou_id"][val])], "sort": [{"id": "bounces", "desc": "True"}],
                               "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "bounce_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "bounce_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', 'min', 'max',
                                                                                         'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(time_period)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(str(df_sprint["ou_id"][val]))
                    if not zero_flag:
                        zero_list.append(str(df_sprint["ou_id"][val]))
                except Exception as ex:
                    not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across : " + str(time_period) + " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across :" + str(time_period) + "  {}".format(
            set(list_not_match))

    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("across_type", across_type)
    def test_bounce_report_without_filter(self, across_type,  create_generic_object,
                                          create_customer_object, get_integration_obj ):
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint.index:
            try:
                filters = {"sort_xaxis": "value_high-low",
                           "product_id": create_generic_object.env["project_id"],
                           "integration_ids": get_integration_obj}

                payload = {"filter": filters, "across": across_type,
                           "ou_ids": [str(df_sprint["ou_id"][val])], "sort": [{"id": "bounces", "desc": "True"}],
                           "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "bounce_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "bounce_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', 'min', 'max',
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

    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("across_type", across_type)
    def test_bounce_report_with_filter(self, across_type,  create_generic_object,
                                       create_customer_object, get_integration_obj ):

        filter = {"workitem_statuses": ["Closed", "Done", "Removed"], "workitem_types": ["Bug", "Epic", "Issue"]}
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint.index:
            for k, v in filter.items():
                try:
                    filters = {"sort_xaxis": "value_high-low", k: v,
                               "product_id": create_generic_object.env["project_id"],
                               "integration_ids": get_integration_obj}

                    payload = {"filter": filters, "across": across_type,
                               "ou_ids": [str(df_sprint["ou_id"][val])], "sort": [{"id": "bounces", "desc": "True"}],
                               "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "bounce_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "bounce_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', 'min', 'max',
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
