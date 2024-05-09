import json
import logging
import pandas as pd
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmResponseTimeReportsCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["scm-prs-response-time_across"]
    filter_type = generic_object.api_data["scm_prs_report_filter_types"]

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", sorted(across_type))
    def test_scm_prs_response_time_author(self, across_type, drilldown_object, create_generic_object,
                                          create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        for val in df_sprint:
            try:
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]

                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"}}

                payload = {"filter": filters, "across": across_type, "ou_ids": [val]}
                if len(interval_val) != 0:
                    payload["interval"] = interval_val
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(across_type)+" "+ str(val)))
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

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("across_type", across_type)
    def test_scm_prs_response_time_reviewer(self, across_type, drilldown_object, create_generic_object,
                                            create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        for val in df_sprint:
            try:
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]

                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"}}

                payload = {"filter": filters, "across": across_type, "ou_ids": [val]}

                if len(interval_val) != 0:
                    payload["interval"] = interval_val
                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(across_type)+" "+ str(val)))
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

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=3)
    def test_scm_prs_response_time_single_stat_author(self, drilldown_object, create_generic_object,
                                                      create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"}}

                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="ou_id is :"+val)
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

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=4)
    def test_scm_prs_response_time_single_stat_reviewer(self, drilldown_object, create_generic_object,
                                                        create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"}}

                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="ou_id is :"+val)
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
    @pytest.mark.run(order=5)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_author_es_filter(self, filter_type, create_generic_object,
                                                    create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"}}
                filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                filters[filter_mapping[filter_type]] = create_generic_object.get_filter_options_scm(
                    arg_filter_type=filter_type, report_type="prs")
                payload = {"filter": filters, "across": "repo_id", "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=6)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_author_es_excludefilter(self, filter_type, create_generic_object,
                                                           create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"}}
                filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                filters["exclude"] = {filter_mapping[filter_type]:
                    create_generic_object.get_filter_options_scm(
                        arg_filter_type=filter_type, report_type="prs")}
                payload = {"filter": filters, "across": "repo_id", "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=7)
    def test_scm_prs_response_time_author_es_partialmatchfilter(self, create_generic_object,
                                                                create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"},
                           "partial_match": {"repo_ids": {"$begins": "levelops"}}}
                payload = {"filter": filters, "across": "repo_id", "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="ou_id is :"+val)
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
    @pytest.mark.run(order=8)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_reviewer_es_filter(self, filter_type, create_generic_object,
                                                      create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"}}
                filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                filters[filter_mapping[filter_type]] = create_generic_object.get_filter_options_scm(
                    arg_filter_type=filter_type, report_type="prs")
                payload = {"filter": filters, "across": "repo_id", "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=9)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_reviewer_es_excludefilter(self, filter_type, create_generic_object,
                                                             create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"}}
                filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                filters["exclude"] = {filter_mapping[filter_type]:
                    create_generic_object.get_filter_options_scm(
                        arg_filter_type=filter_type, report_type="prs")}
                payload = {"filter": filters, "across": "repo_id", "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=10)
    def test_scm_prs_response_time_reviewer_es_partialmatchfilter(self, create_generic_object,
                                                                  create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"metrics": "average_author_response_time", "integration_ids": get_integration_obj,
                           "code_change_size_unit": "files",
                           "code_change_size_config": {"small": "50", "medium": "150"},
                           "comment_density_size_config": {"shallow": "1", "good": "5"},
                           "partial_match": {"repo_ids": {"$begins": "levelops"}}}
                payload = {"filter": filters, "across": "repo_id", "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max",
                                                                                     "count", "sum", 'mean'], unique_id="ou_id is :"+val)
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
    @pytest.mark.run(order=11)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_single_stat_author_es_filter(self, filter_type, create_generic_object,
                                                                create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"}}
                    filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                    filters[filter_mapping[filter_type]] = create_generic_object.get_filter_options_scm(
                        arg_filter_type=filter_type, report_type="prs")
                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=12)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_single_stat_author_es_excludefilter(self, filter_type, create_generic_object,
                                                                       create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"}}
                    filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                    filters["exclude"] = {filter_mapping[filter_type]:
                        create_generic_object.get_filter_options_scm(
                            arg_filter_type=filter_type, report_type="prs")}
                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=13)
    def test_scm_prs_response_time_single_stat_author_es_partialmatchfilter(self, create_generic_object,
                                                                            create_customer_object,
                                                                            get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"}}
                    filters["partial_match"] = {"repo_ids": {"$begins": "levelops"}}
                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_author_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="ou_id is :"+val)
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
    @pytest.mark.run(order=14)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_single_stat_reviewer_es_filter(self, filter_type, create_generic_object,
                                                                  create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"}}
                    filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                    filters[filter_mapping[filter_type]] = create_generic_object.get_filter_options_scm(
                        arg_filter_type=filter_type, report_type="prs")
                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=15)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_prs_response_time_single_stat_reviewer_es_excludefilter(self, filter_type, create_generic_object,
                                                                         create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"}}
                    filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                    filters["exclude"] = {filter_mapping[filter_type]:
                        create_generic_object.get_filter_options_scm(
                            arg_filter_type=filter_type, report_type="prs")}
                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
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
    @pytest.mark.run(order=16)
    def test_scm_prs_response_time_single_stat_reviewer_es_partialmatchfilter(self, create_generic_object,
                                                                              create_customer_object,
                                                                              get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            for each_agg in agg_type:

                try:
                    filters = {"time_period": 30, "agg_type": each_agg, "integration_ids": get_integration_obj,
                               "code_change_size_unit": "files",
                               "code_change_size_config": {"small": "50", "medium": "150"},
                               "comment_density_size_config": {"shallow": "1", "good": "5"},
                               "partial_match": {"repo_ids": {"$begins": "levelops"}}}
                    payload = {"filter": filters, "across": "pr_created", "ou_ids": [val]}
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_reviewer_response_time_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max",
                                                                                         "count", "sum", 'mean'], unique_id="ou_id is :"+val)
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

