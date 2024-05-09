import logging
import random

import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestScmPrResponseTimeReport:
    generic_object = TGhelper()
    metrics = generic_object.api_data["metrics"]
    sort_list = generic_object.api_data["sort_list"]
    interval = generic_object.api_data["interval"]
    aggs=generic_object.api_data["scm_pr_response_time_report_aggs"]

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_pr_response_time_report_001(self, create_scm_pr_response_time_object, create_generic_object,
                                             get_integration_obj):
        """Validate alignment of scm scm_pr_response_time_report """

        LOG.info("==== create widget with available filter ====")
        create_scm_rework_report = create_scm_pr_response_time_object.scm_pr_response_time_report(
            integration_id=get_integration_obj)

        assert create_scm_rework_report, "widget is not created"

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_pr_response_time_report_002(self, create_scm_pr_response_time_object, create_generic_object,
                                             get_integration_obj):
        """Validate alignment of scm scm_pr_response_time_report """

        LOG.info("==== create widget with available filter ====")
        create_scm_rework_report = create_scm_pr_response_time_object.scm_pr_response_time_report(
            integration_id=get_integration_obj)

        assert create_scm_rework_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of SCM files report with drilldown ====")
        key_value = create_scm_pr_response_time_object.scm_pr_response_time_report(integration_id=get_integration_obj,
                                                                                   keys=True)
        for key, count in key_value.items():
            widget_count = count
            drilldown = create_scm_pr_response_time_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj,
                key=key)
            avg_author_response_time = []
            for eachDrilldownRecord in drilldown['records']:
                if key in eachDrilldownRecord["assignee_ids"]:
                    avg_author_response_time.append(
                        eachDrilldownRecord["avg_author_response_time"])
            drilldown_count = len(avg_author_response_time)
            assert widget_count == drilldown_count, "count is not matching for " + key

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_scm_pr_response_time_report_003(self, create_scm_pr_response_time_object, create_generic_object,
                                             get_integration_obj):
        """Validate alignment of scm scm_pr_response_time_report """

        LOG.info("==== create widget with available filter ====")
        create_scm_rework_report = create_scm_pr_response_time_object.scm_pr_response_time_report(
            integration_id=get_integration_obj, metrics="average_reviewer_response_time")

        assert create_scm_rework_report, "widget is not created"

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_scm_pr_response_time_report_004(self, create_scm_pr_response_time_object, create_generic_object,
                                             get_integration_obj):
        """Validate alignment of scm scm_pr_response_time_report """

        LOG.info("==== create widget with available filter ====")
        create_scm_rework_report = create_scm_pr_response_time_object.scm_pr_response_time_report(
            integration_id=get_integration_obj, metrics="average_reviewer_response_time")

        assert create_scm_rework_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of SCM files report with drilldown ====")
        key_value = create_scm_pr_response_time_object.scm_pr_response_time_report(integration_id=get_integration_obj,
                                                                                   metrics="average_reviewer_response_time",
                                                                                   keys=True)
        for key, count in key_value.items():
            widget_count = count
            drilldown = create_scm_pr_response_time_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj,
                key=key)
            avg_author_response_time = []
            for eachDrilldownRecord in drilldown['records']:
                if key in eachDrilldownRecord["assignee_ids"]:
                    avg_author_response_time.append(
                        eachDrilldownRecord["avg_author_response_time"])
            drilldown_count = len(avg_author_response_time)
            assert widget_count == drilldown_count, "count is not matching for " + key

    @pytest.mark.parametrize("metrics", metrics)
    @pytest.mark.parametrize("sort_list", sort_list)
    @pytest.mark.parametrize("aggs", aggs)
    # @pytest.mark.parametrize("interval", interval)
    def test_scm_pr_response_time_sorting_functionality(self, create_scm_pr_response_time_object, create_generic_object,
                                                        get_integration_obj, metrics, sort_list, aggs):
        """Verify sorting x-axis functionality for all aggs """
        if "low-high" in sort_list:
            desc = False
        else:
            desc = True
        # breakpoint()
        if "label" in sort_list:
            sort_value = aggs
        else:
            if metrics in ["average_author_response_time", "average_reviewer_response_time"]:
                sort_value = "average_author_response_time".replace('average', "mean")
            else:
                sort_value = "median_author_response_time"

        create_scm_pr_response_report = create_scm_pr_response_time_object.scm_pr_response_time_report(
            integration_id=get_integration_obj, metrics=metrics, sort=sort_value, sort_list=True, desc=desc,
            across=aggs, sort_value=sort_list)

        assert create_scm_pr_response_report, "widget is not created"
        if "label" not in sort_list:
            sorting = create_scm_pr_response_time_object.check_response_sorting_by_value(metrics, sort_value=sort_list,
                                                                                         response=create_scm_pr_response_report)
            assert len(sorting) == 0, "sorting isnt working for---{}".format(sorting)

        elif "label" in sort_list:
            sorting = create_scm_pr_response_time_object.check_response_sorting_by_label(metrics, sort_value=sort_list,
                                                                                         response=create_scm_pr_response_report)
            assert len(sorting) == 0, "sorting isnt working for---{}".format(sorting)

    @pytest.mark.parametrize("metrics", metrics)
    @pytest.mark.parametrize("sort_list", sort_list)
    @pytest.mark.parametrize("interval", interval)
    def test_scm_pr_response_time_sorting_functionality_pr_closed(self, create_scm_pr_response_time_object,
                                                                  create_generic_object,
                                                                  get_integration_obj, metrics, sort_list, interval):
        aggs = "pr_closed"
        """Verify sorting x-axis functionality for all aggs """
        if "low-high" in sort_list:
            desc = False
        else:
            desc = True
        # breakpoint()
        if "label" in sort_list:
            sort_value = aggs
        else:
            if metrics in ["average_author_response_time", "average_reviewer_response_time"]:
                sort_value = "average_author_response_time".replace('average', "mean")
            else:
                sort_value = "median_author_response_time"

        create_scm_pr_response_report = create_scm_pr_response_time_object.scm_pr_response_time_report(
            integration_id=get_integration_obj, metrics=metrics, sort=sort_value, sort_list=True, desc=desc,
            interval=interval, across=aggs, sort_value=sort_list)

        assert create_scm_pr_response_report, "widget is not created"

        if "label" not in sort_list:
            sorting = create_scm_pr_response_time_object.check_response_sorting_by_value(metrics, sort_value=sort_list,
                                                                                         response=create_scm_pr_response_report)
            assert len(sorting) == 0, "sorting isnt working for---{}".format(sorting)

        elif "label" in sort_list:
            sorting = create_scm_pr_response_time_object.check_response_sorting_by_label(metrics, sort_value=sort_list,
                                                                                         response=create_scm_pr_response_report)
            assert len(sorting) == 0, "sorting isnt working for---{}".format(sorting)
