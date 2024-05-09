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

    ou_ids = ['241', '243',"725","726","727","728","730","729"
              '242', '244', '247', '248', '267', '252', '255', '254', '257', '260', '262', '269',
              '253', '258', '263', '274',
              '275', '276', '266', '249', '250', '251', '264', '268', '265', '261', '256', '259', '270', '271',
              '272', '219']

    # ou_ids = ['267']

    def metric_payload(self, metric, ou_id, gt, lt):
        payload = {"filter": {"metric": metric, "agg_type": "average",
                              "completed_at": {"$gt": gt,
                                               "$lt": lt},
                              "include_issue_keys": True, "include_total_count": True},
                   "ou_ids": [ou_id],
                   "ou_user_filter_designation": {"sprint": ["sprint_report"]}}
        return payload

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("ou_id", ou_ids)
    def test_broadridge_sprint_metrics_single_stat_report_with_trend_report(self, create_generic_object,
                                                                            create_customer_object, ou_id):
        # breakpoint()
        """The test case compares the single stat sprint metrics drilldown with the sprint metrics report as wel check with sprint metrics trend report all 3
        return the same sprints for the same time period"""
        # gt, lt = create_generic_object.get_epoc_time(value=1)
        gt,lt="1664586001","1672491599"

        LOG.info("gt,lt---{},{}".format(gt, lt))
        # gt, lt = TGhelper.get_epoc_time(value=1)
        # gt,lt= generic_object.get_epoc_info(value=1)

        payload = {"filter":
                       {"include_issue_keys": True, "agg_type": "average", "completed_at": {"$gt": gt, "$lt": lt}},
                   "ou_ids": [ou_id], "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

        url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
            "sprint_metrics_report_jira"]
        LOG.info("url used---{}".format(url))
        res = create_generic_object.execute_api_call(url, "post", data=payload)
        sprints_returned = []
        for i in range(0, len(res['records'])):
            sprints_returned.append(res['records'][i]['additional_key'])

        """comapre with sprint metrics trend report"""

        payload_1 = {"filter": {
            "metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                       "creep_not_done_points"],
            "agg_type": "average",
            "completed_at": {"$gt": gt, "$lt": lt}}, "across": "sprint", "interval": "week", "ou_ids": [ou_id],
            "ou_user_filter_designation": {"sprint": ["sprint_report"]}}
        trend_sprint_returned = []
        trend_res = create_generic_object.execute_api_call(url, "post", data=payload_1)
        for i in range(0, len(trend_res['records'])):
            trend_sprint_returned.append(trend_res['records'][i]['additional_key'])

        assert set(sprints_returned) == set(
            trend_sprint_returned), "The sprints returned by sprint metrics trend report and sprint metrics single stat do not match"

        """check the drilldown for the sprint metrics single stat"""
        metric = ["avg_commit_to_done", "avg_creep", "velocity_points", "velocity_points_std",
                  "average_ticket_size_per_sprint"]
        result = []
        for i in metric:
            metric_res_sprint = []
            # breakpoint()
            metric_res = create_generic_object.execute_api_call(url, "post",
                                                                data=self.metric_payload(metric=i, ou_id=ou_id, gt=gt,
                                                                                         lt=lt))
            for i in range(0, len(metric_res['records'])):
                metric_res_sprint.append(metric_res['records'][i]['additional_key'])

            if set(metric_res_sprint) != set(sprints_returned):
                result.append({metric: len(metric_res_sprint), "sprints_metric": metric_res_sprint})

        assert len(
            result) == 0, "the sprint metric single stat drilldown doesnot contain the same sprint from trend report ----{}".format(
            sprints_returned)
