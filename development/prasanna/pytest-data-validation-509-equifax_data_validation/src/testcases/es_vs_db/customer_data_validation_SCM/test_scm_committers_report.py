import logging
import pandas as pd
import pytest
import json

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCMCommittersreport:
    generic_object = TGhelper()
    agg_type = generic_object.api_data["SCM_commit_types"]
    trends_type = generic_object.api_data["SCM_commit_trends"]
    filter_type = generic_object.api_data["scm_repos_report_filter_types"]

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=1)
    def test_scm_committers_report(self, create_generic_object,
                                   create_customer_object, get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"product_id": create_generic_object.env['project_id'],
                           "integration_ids": get_integration_obj,
                           }

                payload = {"filter": filters, "ou_user_filter_designation": {"github": ["committer"]},
                           "ou_ids": [val]}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name='name',
                                                                                           columns=['id',
                                                                                                    'name',
                                                                                                    'tech_breadth',
                                                                                                    'repo_breadth',
                                                                                                    'file_types',
                                                                                                    'num_repos',
                                                                                                    'num_commits',
                                                                                                    'num_prs',
                                                                                                    'num_additions',
                                                                                                    'num_deletions',
                                                                                                    'num_changes',
                                                                                                    'num_workitems'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.esdbfilterscm
    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_committers_report_filter(self, filter_type, create_generic_object,
                                          create_customer_object, get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"product_id": create_generic_object.env['project_id'],
                           "integration_ids": get_integration_obj,
                           }
                filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                filters[filter_mapping[filter_type]] = create_generic_object.get_filter_options_scm(
                    arg_filter_type=filter_type, report_type="commits")
                payload = {"filter": filters, "ou_user_filter_designation": {"github": ["committer"]},
                           "ou_ids": [val]}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name='name',
                                                                                           columns=['id',
                                                                                                    'name',
                                                                                                    'tech_breadth',
                                                                                                    'repo_breadth',
                                                                                                    'file_types',
                                                                                                    'num_repos',
                                                                                                    'num_commits',
                                                                                                    'num_prs',
                                                                                                    'num_additions',
                                                                                                    'num_deletions',
                                                                                                    'num_changes',
                                                                                                    'num_workitems'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.esdbfilterscm
    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_scm_committers_report_excludefilter(self, filter_type, create_generic_object,
                                                 create_customer_object, get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"product_id": create_generic_object.env['project_id'],
                           "integration_ids": get_integration_obj,
                           }
                filter_mapping = create_generic_object.api_data["scm_values_vs_report_filter_maping"]
                filters["exclude"] = {filter_mapping[filter_type]:
                    create_generic_object.get_filter_options_scm(
                        arg_filter_type=filter_type, report_type="commits")}
                payload = {"filter": filters, "ou_user_filter_designation": {"github": ["committer"]},
                           "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name='name',
                                                                                           columns=['id',
                                                                                                    'name',
                                                                                                    'tech_breadth',
                                                                                                    'repo_breadth',
                                                                                                    'file_types',
                                                                                                    'num_repos',
                                                                                                    'num_commits',
                                                                                                    'num_prs',
                                                                                                    'num_additions',
                                                                                                    'num_deletions',
                                                                                                    'num_changes',
                                                                                                    'num_workitems'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.esdbfilterscm
    @pytest.mark.run(order=4)
    def test_scm_committers_report_partialmatchfilter(self, create_generic_object,
                                                      create_customer_object, get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                filters = {"product_id": create_generic_object.env['project_id'],
                           "integration_ids": get_integration_obj,
                           }
                filters["partial_match"] = {"repo_ids": {"$begins": "levelops"}}
                payload = {"filter": filters, "ou_user_filter_designation": {"github": ["committer"]},
                           "ou_ids": [val]}
                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "scm_committer_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name='name',
                                                                                           columns=['id',
                                                                                                    'name',
                                                                                                    'tech_breadth',
                                                                                                    'repo_breadth',
                                                                                                    'file_types',
                                                                                                    'num_repos',
                                                                                                    'num_commits',
                                                                                                    'num_prs',
                                                                                                    'num_additions',
                                                                                                    'num_deletions',
                                                                                                    'num_changes',
                                                                                                    'num_workitems'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))
