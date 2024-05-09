import logging
import pandas as pd
import pytest
import json
import random
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
import random

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()

    # filter_type = generic_object.api_data["constant_filters_jira"]
    # filter_type_custom_field = generic_object.get_aggregration_fields(only_custom=True)
    # filter_type = filter_type + list(filter_type_custom_field)
    # dashboard_partial = ["or", "partial_match"]

    @pytest.mark.run(order=1)
    def test_broadridge_sprint_metrics_single_stat_report(self, create_generic_object, create_customer_object,
                                               get_integration_obj):

        df_sprint = ['241', '243',
                     '242', '244', '247', '248', '267', '252', '255', '254', '257', '260', '262', '269',
                     '253', '258', '263', '274',
                     '275', '276', '266', '249', '250', '251', '264', '268', '265', '261', '256', '259', '270', '271',
                     '272', '219']

        zero_list = []
        list_not_match = []
        not_executed_list = []
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                # filters = {"include_issue_keys": True, "agg_type": "average",
                #            "issue_types": ["BUG", "BUG SUB-TASK", "STORY", "SPIKE", "TASK"],
                #            "completed_at": {"$gt": "1667779200", "$lt": "1669075199"}, "creep_buffer": 86400,
                #            "additional_done_statuses": ["QA COMPLETE", "UAT COMPLETE", "PENDING PROD VERIFICATION"],
                #            "integration_ids": ["12", "3", "4", "5"]}

                payload = {"filter": {"include_issue_keys": True, "agg_type": "average",
                                      "completed_at": {"$gt": "1648771200", "$lt": "1653825479"},
                                      "integration_ids": ["3", "4"]}, "ou_ids": [val],
                           "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_values_inside_columns_str=[
                                                                                               "commit_delivered_keys",
                                                                                               "committed_keys",
                                                                                               "delivered_keys",
                                                                                               "delivered_creep_keys",
                                                                                               "creep_keys"],
                                                                                           sort_column_name='sprint_id',
                                                                                           columns=['key',
                                                                                                    'additional_key',
                                                                                                    'sprint_id',
                                                                                                    "committed_story_points",
                                                                                                    "commit_delivered_story_points",
                                                                                                    "delivered_story_points",
                                                                                                    "creep_story_points",
                                                                                                    "delivered_creep_story_points",
                                                                                                    "committed_keys",
                                                                                                    "commit_delivered_keys",
                                                                                                    "delivered_keys",
                                                                                                    "creep_keys",
                                                                                                    "delivered_creep_keys",
                                                                                                    "total_issues",
                                                                                                    "total_unestimated_issues"])
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                LOG.info("Exception {}".format(ex))
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.run(order=2)
    def test_broadridge_sprint_metrics_single_stat_report_jira_es_sprint_trends(self, create_generic_object,
                                                                     create_customer_object, get_integration_obj):

        df_sprint = ['241', '243',
                     '242', '244', '247', '248', '267', '252', '255', '254', '257', '260', '262', '269',
                     '253', '258', '263', '274',
                     '275', '276', '266', '249', '250', '251', '264', '268', '265', '261', '256', '259', '270', '271',
                     '272', '219']
        zero_list = []
        list_not_match = []
        not_executed_list = []
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                # filters = {"metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                #                       "creep_not_done_points"],
                #            "agg_type": "average", "completed_at": {"$gt": "1648771200", "$lt": "1664582399"},
                #            "integration_ids": ["3", "4", "5"]}

                payload = {"filter": {"metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                                                 "creep_not_done_points"],
                                      "agg_type": "average", "completed_at": {"$gt": "1648771200", "$lt": "1653825479"},
                                      "integration_ids": ["3", "4"]}, "across": "sprint", "interval": "week",
                           "ou_ids": [val],
                           "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name='sprint_id',
                                                                                           sort_values_inside_columns_str=[
                                                                                               "commit_delivered_keys",
                                                                                               "committed_keys",
                                                                                               "delivered_keys",
                                                                                               "delivered_creep_keys",
                                                                                               "creep_keys"],
                                                                                           columns=['key',
                                                                                                    'additional_key',
                                                                                                    'sprint_id',
                                                                                                    "committed_story_points",
                                                                                                    "commit_delivered_story_points",
                                                                                                    "delivered_story_points",
                                                                                                    "creep_story_points",
                                                                                                    "delivered_creep_story_points",
                                                                                                    "committed_keys",
                                                                                                    "commit_delivered_keys",
                                                                                                    "delivered_keys",
                                                                                                    "creep_keys",
                                                                                                    "delivered_creep_keys",
                                                                                                    "total_issues",
                                                                                                    "total_unestimated_issues",
                                                                                                    ])
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

    @pytest.mark.run(order=3)
    def test_broadridge_sprint_metrics_single_stat_report_drilldown(self, create_generic_object,
                                                         create_customer_object, get_integration_obj):

        df_sprint = ['241', '243',
                     '242', '244', '247', '248', '267', '252', '255', '254', '257', '260', '262', '269',
                     '253', '258', '263', '274',
                     '275', '276', '266', '249', '250', '251', '264', '268', '265', '261', '256', '259', '270', '271',
                     '272', '219']
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                filters = {"include_issue_keys": True, "agg_type": "average",
                           "issue_types": ["BUG", "BUG SUB-TASK", "STORY", "SPIKE", "TASK"],
                           "completed_at": {"$gt": "1667779200", "$lt": "1669075199"}, "creep_buffer": 86400,
                           "additional_done_statuses": ["QA COMPLETE", "UAT COMPLETE", "PENDING PROD VERIFICATION"],
                           "integration_ids": ["12", "3", "4", "5"]}

                payload = {"filter": {"metric": "avg_commit_to_done", "agg_type": "average",
                                      "completed_at": {"$gt": "1648771200", "$lt": "1653825479"},
                                      "include_issue_keys": True,
                                      "integration_ids": ["3", "4"], "include_total_count": True},
                           "ou_ids": [val],
                           "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)

                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_values_inside_columns_str=[
                                                                                               "commit_delivered_keys",
                                                                                               "committed_keys",
                                                                                               "delivered_keys",
                                                                                               "delivered_creep_keys",
                                                                                               "creep_keys"],
                                                                                           sort_column_name='sprint_id',
                                                                                           columns=['key',
                                                                                                    'additional_key',
                                                                                                    'sprint_id',
                                                                                                    "committed_story_points",
                                                                                                    "commit_delivered_story_points",
                                                                                                    "delivered_story_points",
                                                                                                    "creep_story_points",
                                                                                                    "delivered_creep_story_points",
                                                                                                    "committed_keys",
                                                                                                    "commit_delivered_keys",
                                                                                                    "delivered_keys",
                                                                                                    "creep_keys",
                                                                                                    "delivered_creep_keys",
                                                                                                    "total_issues",
                                                                                                    "total_unestimated_issues"])
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

    @pytest.mark.run(order=4)
    def test_broadridge_sprint_metrics_single_stat_report_jira_es_avg_issue_size(self, create_generic_object,
                                                                      create_customer_object,
                                                                      get_integration_obj):

        df_sprint = ['241', '243',
                     '242', '244', '247', '248', '267', '252', '255', '254', '257', '260', '262', '269',
                     '253', '258', '263', '274',
                     '275', '276', '266', '249', '250', '251', '264', '268', '265', '261', '256', '259', '270', '271',
                     '272', '219']
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                filters = {"include_issue_keys": True, "agg_type": "average",
                           "issue_types": ["BUG", "BUG SUB-TASK", "STORY", "SPIKE", "TASK"],
                           "completed_at": {"$gt": "1667779200", "$lt": "1669075199"}, "story_points": {"$gt": "0"},
                           "integration_ids": ["12", "3", "4", "5"]}

                payload = {"filter": {"metric": "avg_creep", "agg_type": "average",
                                      "completed_at": {"$gt": "1648771200",
                                                       "$lt": "1653825479"},
                                      "include_issue_keys": True,
                                      "integration_ids": ["3", "4"],
                                      "include_total_count": True}, "ou_ids": [val],
                           "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_values_inside_columns_str=[
                                                                                               "commit_delivered_keys",
                                                                                               "committed_keys",
                                                                                               "delivered_keys",
                                                                                               "delivered_creep_keys",
                                                                                               "creep_keys"],
                                                                                           sort_column_name='sprint_id',
                                                                                           columns=['key',
                                                                                                    'additional_key',
                                                                                                    'sprint_id',
                                                                                                    "committed_story_points",
                                                                                                    "commit_delivered_story_points",
                                                                                                    "delivered_story_points",
                                                                                                    "creep_story_points",
                                                                                                    "delivered_creep_story_points",
                                                                                                    "committed_keys",
                                                                                                    "commit_delivered_keys",
                                                                                                    "delivered_keys",
                                                                                                    "creep_keys",
                                                                                                    "delivered_creep_keys",
                                                                                                    "total_issues",
                                                                                                    "total_unestimated_issues"])
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

    @pytest.mark.run(order=5)
    def test_broadridge_sprint_metrics_single_stat_report_jira_es_sprint_trends_drilldown(self, create_generic_object,
                                                                               create_customer_object,
                                                                               get_integration_obj):

        df_sprint = ['241', '243',
                     '242', '244', '247', '248', '267', '252', '255', '254', '257', '260', '262', '269',
                     '253', '258', '263', '274',
                     '275', '276', '266', '249', '250', '251', '264', '268', '265', '261', '256', '259', '270', '271',
                     '272', '219']
        zero_list = []
        list_not_match = []
        not_executed_list = []
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                # filters = {"metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                #                       "creep_not_done_points"],
                #            "agg_type": "average", "completed_at": {"$gt": "1648771200", "$lt": "1664582399"},
                #            "integration_ids": ["3", "4", "5"]}

                payload = {"filter": {"metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                                                 "creep_not_done_points"],
                                      "agg_type": "average", "completed_at": {"$gt": "1648771200", "$lt": "1653825479"},
                                      "integration_ids": ["3", "4"]}, "across": "sprint", "interval": "week",
                           "ou_ids": [val],
                           "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                LOG.info("payload {} ".format(json.dumps(payload)))
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                allSprints = []
                for eachSprint in db_response['records']:
                    allSprints.append(eachSprint['additional_key'])

                payload_drilldown = {
                    "filter": {"agg_type": "average", "completed_at": {"$gt": "1648771200", "$lt": "1653825479"},
                               "integration_ids": ["3", "4"], "include_issue_keys": True,
                               "include_workitem_ids": True, "sprint_report": [random.choice(allSprints)]},
                    "ou_ids": [val], "ou_exclusions": ["sprint"],
                    "ou_user_filter_designation": {"sprint": ["sprint_report"]}, "page": 0, "page_size": 10000}

                es_base_url_drilldown = create_generic_object.connection[
                                            "base_url"] + create_generic_object.api_data[
                                            "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                es_response_drilldown = create_generic_object.execute_api_call(es_base_url_drilldown, "post",
                                                                               data=payload_drilldown)

                db_base_url_drilldown = create_generic_object.connection[
                                            "base_url"] + create_generic_object.api_data[
                                            "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=db"
                db_response_drilldown = create_generic_object.execute_api_call(db_base_url_drilldown, "post",
                                                                               data=payload_drilldown)

                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response_drilldown,
                                                                                           db_response_drilldown,
                                                                                           sort_column_name='sprint_id',
                                                                                           sort_values_inside_columns_str=[
                                                                                               "commit_delivered_keys",
                                                                                               "committed_keys",
                                                                                               "delivered_keys",
                                                                                               "delivered_creep_keys",
                                                                                               "creep_keys"],
                                                                                           columns=['key',
                                                                                                    'additional_key',
                                                                                                    'sprint_id',
                                                                                                    "committed_story_points",
                                                                                                    "commit_delivered_story_points",
                                                                                                    "delivered_story_points",
                                                                                                    "creep_story_points",
                                                                                                    "delivered_creep_story_points",
                                                                                                    "committed_keys",
                                                                                                    "commit_delivered_keys",
                                                                                                    "delivered_keys",
                                                                                                    "creep_keys",
                                                                                                    "delivered_creep_keys",
                                                                                                    "total_issues",
                                                                                                    "total_unestimated_issues",
                                                                                                    ])
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

    @pytest.mark.run(order=6)
    def test_broadridge_sprint_metrics_single_stat_report_filters(self, create_generic_object, create_customer_object,
                                                       get_integration_obj):

        df_sprint = ['241', '243',
                     '242', '244', '247', '248', '267', '252', '255', '254', '257', '260', '262', '269',
                     '253', '258', '263', '274',
                     '275', '276', '266', '249', '250', '251', '264', '268', '265', '261', '256', '259', '270', '271',
                     '272', '219']
        zero_list = []
        list_not_match = []
        not_executed_list = []
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                # filters = {"include_issue_keys": True, "agg_type": "average",
                #            "issue_types": ["BUG", "BUG SUB-TASK", "STORY", "SPIKE", "TASK"],
                #            "completed_at": {"$gt": "1667779200", "$lt": "1669075199"}, "creep_buffer": 86400,
                #            "additional_done_statuses": ["QA COMPLETE", "UAT COMPLETE", "PENDING PROD VERIFICATION"],
                #            "integration_ids": ["12", "3", "4", "5"]}

                payload = {"filter": {"agg_type": "average", "completed_at": {"$gt": "1654214400", "$lt": "1669766399"},
                                      "integration_ids": ["3", "4"], "product_id": "1", "sprint_report": ["Sprint 47"],
                                      "include_issue_keys": True, "include_workitem_ids": True, "creep_buffer": 86400,
                                      "additional_done_statuses": ["QA COMPLETE", "UAT COMPLETE",
                                                                   "PENDING PROD VERIFICATION"]}, "ou_ids": [val],
                           "ou_user_filter_designation": {"sprint": ["sprint_report"]}, "ou_exclusions": ["sprint"]}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_values_inside_columns_str=[
                                                                                               "commit_delivered_keys",
                                                                                               "committed_keys",
                                                                                               "delivered_keys",
                                                                                               "delivered_creep_keys",
                                                                                               "creep_keys"],
                                                                                           sort_column_name='sprint_id',
                                                                                           columns=['key',
                                                                                                    'additional_key',
                                                                                                    'sprint_id',
                                                                                                    "committed_story_points",
                                                                                                    "commit_delivered_story_points",
                                                                                                    "delivered_story_points",
                                                                                                    "creep_story_points",
                                                                                                    "delivered_creep_story_points",
                                                                                                    "committed_keys",
                                                                                                    "commit_delivered_keys",
                                                                                                    "delivered_keys",
                                                                                                    "creep_keys",
                                                                                                    "delivered_creep_keys",
                                                                                                    "total_issues",
                                                                                                    "total_unestimated_issues"])
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                LOG.info("Exception {}".format(ex))
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))
