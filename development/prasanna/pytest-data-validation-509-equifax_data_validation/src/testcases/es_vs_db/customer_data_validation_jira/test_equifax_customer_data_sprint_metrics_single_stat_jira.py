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


class TestEquifaxCustomer:
    generic_object = TGhelper()

    # filter_type = generic_object.api_data["constant_filters_jira"]
    # filter_type_custom_field = generic_object.get_aggregration_fields(only_custom=True)
    # filter_type = filter_type + list(filter_type_custom_field)
    # dashboard_partial = ["or", "partial_match"]

    @pytest.mark.run(order=1)
    def test_equifax_sprint_metrics_single_stat_report1(self, create_generic_object, create_customer_object,
                                                get_integration_obj):

        df_sprint = ["133","131","129"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = {"filter": {"include_issue_keys": True, "agg_type": "average",
                                      "completed_at": {"$gt": "1667260800", "$lt": "1669852799"},
                                      "integration_ids": ["8"]}, "ou_ids": [val],
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
                                                                                                    "total_unestimated_issues"], unique_id="ou_id is :"+val)
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
    def test_equifax_sprint_metrics_single_stat_report2(self, create_generic_object, create_customer_object,
                                                get_integration_obj):

        df_sprint = ["133","131","129"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:

                payload = {"filter": {"include_issue_keys": True, "agg_type": "average", "issue_types": ["STORY"],
                                      "completed_at": {"$gt": "1667260800", "$lt": "1669852799"},
                                      "integration_ids": ["8"]}, "ou_ids": [val],
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
                                                                                                    "total_unestimated_issues"], unique_id="ou_id is :"+val)
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

    @pytest.mark.run(order=3)
    def test_equifax_sprint_metrics_single_stat_report_drilldown(self, create_generic_object,
                                                          create_customer_object, get_integration_obj):

        df_sprint = ["133","131","129"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:

                payload = {"filter": {"metric": "avg_commit_to_done", "agg_type": "average",
                                      "completed_at": {"$gt": "1667260800",
                                                       "$lt": "1669852799"},
                                      "include_issue_keys": True, "integration_ids": ["8"],
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
                                                                                                    "total_unestimated_issues"], unique_id="ou_id is :"+val)
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
    def test_equifax_sprint_metrics_single_stat_report_drilldown2(self, create_generic_object,
                                                          create_customer_object, get_integration_obj):

        df_sprint = ["133","131","129"]
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:

                payload = {"filter": {"metric": "avg_creep", "agg_type": "average",
                                      "completed_at": {"$gt": "1667260800",
                                                       "$lt": "1669852799"},
                                      "include_issue_keys": True, "integration_ids": ["8"],
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
                                                                                                    "total_unestimated_issues"], unique_id="ou_id is :"+val)
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
    def test_equifax_sprint_metrics_single_stat_report_drilldown3(self, create_generic_object,
                                                          create_customer_object, get_integration_obj):

        df_sprint = ["133","131","129"]
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:

                payload = {"filter": {"metric": "velocity_points", "agg_type": "average",
                                      "completed_at": {"$gt": "1667260800",
                                                       "$lt": "1669852799"},
                                      "include_issue_keys": True, "integration_ids": ["8"],
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
                                                                                                    "total_unestimated_issues"], unique_id="ou_id is :"+val)
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
    def test_equifax_sprint_metrics_single_stat_report_drilldown4(self, create_generic_object,
                                                          create_customer_object, get_integration_obj):

        df_sprint = ["133","131","129"]
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:

                payload = {"filter": {"metric": "velocity_points_std", "agg_type": "average",
                                      "completed_at": {"$gt": "1667260800", "$lt": "1669852799"},
                                      "include_issue_keys": True, "integration_ids": ["8"],
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
                                                                                                    "total_unestimated_issues"], unique_id="ou_id is :"+val)
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

    @pytest.mark.run(order=7)
    def test_equifax_sprint_metrics_single_stat_report_drilldown5(self, create_generic_object,
                                                          create_customer_object, get_integration_obj):

        df_sprint = ["133","131","129"]
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:
                payload = {"filter": {"metric": "average_ticket_size_per_sprint", "agg_type": "average",
                                      "issue_types": ["STORY"],
                                      "completed_at": {"$gt": "1667260800", "$lt": "1669852799"},
                                      "include_issue_keys": True, "integration_ids": ["8"],
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
                                                                                                    "total_unestimated_issues"], unique_id="ou_id is :"+val)
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

    @pytest.mark.run(order=8)
    def test_equifax_sprint_metrics_single_stat_report_jira_es_sprint_trends(self, create_generic_object,
                                                                     create_customer_object, get_integration_obj):

        df_sprint = ["133","131","129"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = {"filter": {"metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                                                 "creep_not_done_points"], "agg_type": "average",
                                      "completed_at": {"$gt": "1667260800", "$lt": "1669852799"},
                                      "integration_ids": ["8"]}, "across": "sprint", "interval": "week",
                           "ou_ids": [val], "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

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
                                                                                                    ], unique_id="ou_id is :"+val)
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


    @pytest.mark.run(order=9)
    def test_equifax_sprint_metrics_single_stat_report_jira_es_sprint_trends_drilldown(self, create_generic_object,
                                                                               create_customer_object,
                                                                               get_integration_obj):

        df_sprint = ["133","131","129"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = {"filter": {"metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                                                 "creep_not_done_points"], "agg_type": "average",
                                      "completed_at": {"$gt": "1667260800", "$lt": "1669852799"},
                                      "integration_ids": ["8"]}, "across": "sprint", "interval": "week",
                           "ou_ids": [val], "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                LOG.info("payload {} ".format(json.dumps(payload)))
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                allSprints=[]
                for eachSprint in db_response['records']:
                    allSprints.append(eachSprint['additional_key'])

                payload_drilldown = {
                    "filter": {"agg_type": "average", "completed_at": {"$gt": "1667260800", "$lt": "1669852799"},
                               "integration_ids": ["8"], "include_issue_keys": True,
                               "include_workitem_ids": True, "sprint_report": [random.choice(allSprints)]},
                    "ou_ids": [val], "ou_exclusions": ["sprint"],
                    "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

                es_base_url_drilldown = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=es"
                es_response_drilldown = create_generic_object.execute_api_call(es_base_url_drilldown, "post", data=payload_drilldown)

                db_base_url_drilldown = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_jira"] + "?there_is_no_cache=true&force_source=db"
                db_response_drilldown = create_generic_object.execute_api_call(db_base_url_drilldown, "post", data=payload_drilldown)

                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response_drilldown, db_response_drilldown,
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
                                                                                                    ], unique_id="ou_id is :"+val)
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
