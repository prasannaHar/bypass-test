import logging
import pandas as pd
import pytest
import json
import random
from copy import deepcopy

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["week_across"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    @pytest.mark.aggstcsunit
    def test_sprint_metrics_trend_report(self, across_type, create_generic_object,
                                         create_customer_object):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                filters = {"metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                                      "creep_not_done_points"],
                           "completed_at": {"$gt": gt, "$lt": lt}}
                filters2 = deepcopy(filters)
                existing_flow_id, new_flow_id = (create_generic_object.env["widget_aggs_test_flag"]).split(":")
                filters["integration_ids"] = [existing_flow_id]
                filters2["integration_ids"] = [new_flow_id]
                ou1, ou2 = val.split(":")
                payload1 = {"filter": filters, "across": across_type, "interval": "week",
                            "ou_ids": [ou1], "ou_user_filter_designation": {"sprint": ["sprint_report"]}}
                payload2 = {"filter": filters2, "across": across_type, "interval": "week",
                            "ou_ids": [ou2], "ou_user_filter_designation": {"sprint": ["sprint_report"]}}
                LOG.info("payload {} ".format(json.dumps(payload1)))
                LOG.info("payload {} ".format(json.dumps(payload2)))

                url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                    "sprint_metrics_report_jira"]
                es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
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
