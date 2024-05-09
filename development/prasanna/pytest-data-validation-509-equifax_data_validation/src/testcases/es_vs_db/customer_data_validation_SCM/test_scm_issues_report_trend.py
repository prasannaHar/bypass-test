import json
import logging
import pandas as pd
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmReportsCustomer:
    generic_object = TGhelper()
    time_period = generic_object.api_data["time_period"]
    filter_type = generic_object.api_data["scm_single_stat_filter_types"]

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("time_period", sorted(time_period))
    def test_scm_issues_single_stat_report(self, time_period, create_generic_object,
                                           create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for arrg in agg_type:
                try:
                    filters = {"time_period": time_period, "agg_type": arrg,
                               "integration_ids": get_integration_obj}

                    payload = {"filter": filters, "across": "issue_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'count'], unique_id="time_period with ou id: {}".format(str(time_period)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(val)
                    if not zero_flag:
                        zero_list.append(val)
                except Exception as ex:
                    not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.esdbfilterscm
    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_issues_single_stat_report_es_filter(self, filter_type, create_generic_object,
                                                     create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for arrg in agg_type:
                try:
                    filters = {"time_period": 30, "agg_type": arrg,
                               "integration_ids": get_integration_obj}
                    filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                    filters[filter_mapping[filter_type]] = create_generic_object.get_filter_options_scm(
                        arg_filter_type=filter_type, report_type="issues")
                    payload = {"filter": filters, "across": "issue_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'count'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(val)
                    if not zero_flag:
                        zero_list.append(val)
                except Exception as ex:
                    not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.esdbfilterscm
    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_issues_single_stat_report_es_excludefilter(self, filter_type, create_generic_object,
                                                            create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for arrg in agg_type:
                try:
                    filters = {"time_period": 30, "agg_type": arrg,
                               "integration_ids": get_integration_obj}
                    filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                    filters["exclude"] = {filter_mapping[filter_type]:
                        create_generic_object.get_filter_options_scm(
                            arg_filter_type=filter_type, report_type="issues")}
                    payload = {"filter": filters, "across": "issue_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'count'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(val)
                    if not zero_flag:
                        zero_list.append(val)
                except Exception as ex:
                    not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.esdbfilterscm
    @pytest.mark.run(order=4)
    def test_scm_issues_single_stat_report_es_partialmatchfilter(self, create_generic_object,
                                                                 create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for arrg in agg_type:
                try:
                    filters = {"time_period": 30, "agg_type": arrg, "integration_ids": get_integration_obj,
                               "partial_match": {"repo_ids": {"$begins": "levelops"}}}
                    payload = {"filter": filters, "across": "issue_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issues-report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'count'], unique_id="ou_id is :"+val)
                    if not flag:
                        list_not_match.append(val)
                    if not zero_flag:
                        zero_list.append(val)
                except Exception as ex:
                    not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))
