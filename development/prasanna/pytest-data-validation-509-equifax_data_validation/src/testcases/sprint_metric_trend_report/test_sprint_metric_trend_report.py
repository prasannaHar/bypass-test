import inspect
import logging
import random
import pandas as pd

import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSprintmetric:
    generic_object = TGhelper()
    filter = reportparam()
    jira_filter = filter.retrieve_widget_test_parameters(report_name="issue_sprint_metrics")
    jira_filter_single_stat = filter.retrieve_widget_test_parameters(report_name="issue_sprint_metrics_single_stat")

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_sprint_metric_trend_001(self, create_sprint_metric_object, create_generic_object):
        """Validate alignment of sprint metric trend Report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=15)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        completed_at = {"$gt": gt, "$lt": lt}
        create_sprint_metric_trend = create_sprint_metric_object.sprint_metric_trend_report(completed_at=completed_at)
        LOG.info("==== Validate Widget Schema ====")

        assert create_sprint_metric_trend, "widget is not created"

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_sprint_metric_trend_002(self, create_sprint_metric_object, create_generic_object):

        """Validate alignment of sprint metric trend Report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=15)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        completed_at = {"$gt": gt, "$lt": lt}
        create_sprint_metric_trend = create_sprint_metric_object.sprint_metric_trend_report(completed_at=completed_at)
        LOG.info("==== Validate Widget Schema ====")

        assert create_sprint_metric_trend, "widget is not created"

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_sprint_metric_object.sprint_metric_trend_report(completed_at=completed_at, keys=True)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_sprint_metric_object.sprint_metric_trend_drilldown(key=eachRecord["additional_key"],
                                                                                  completed_at=completed_at)
            assert eachRecord["total_issues"] == drilldown['records'][0][
                "total_issues"], "Mismatch data in the drill down"
        #
        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_sprint_metric_trend_003(self, create_sprint_metric_object, create_generic_object):

        """Validate alignment of sprint metric trend Report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=15)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        completed_at = {"$gt": gt, "$lt": lt}
        filters = {"sprint_count": 8, "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}}
        create_sprint_metric_trend = create_sprint_metric_object.sprint_metric_trend_report(completed_at=completed_at,
                                                                                            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_sprint_metric_trend, "widget is not created"

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_sprint_metric_object.sprint_metric_trend_report(completed_at=completed_at,
                                                                           var_filters=filters, keys=True)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_sprint_metric_object.sprint_metric_trend_drilldown(key=eachRecord["additional_key"],
                                                                                  var_filters=filters,
                                                                                  completed_at=completed_at)
            assert eachRecord["total_issues"] == drilldown['records'][0][
                "total_issues"], "Mismatch data in the drill down"
        #
        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_sprint_metric_trend_004(self, create_sprint_metric_object, create_generic_object):

        """Validate alignment of sprint metric trend Report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=15)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        completed_at = {"$gt": gt, "$lt": lt}
        filters = {"sprint_count": 8, "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}}
        create_sprint_metric_trend = create_sprint_metric_object.sprint_metric_trend_report(completed_at=completed_at,
                                                                                            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_sprint_metric_trend, "widget is not created"

    @pytest.mark.parametrize("param", jira_filter)
    @pytest.mark.regression
    def test_sprint_metric_widget_drilldown_comparison_jira(self, create_sprint_metric_object, create_generic_object,
                                                            param):

        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]
        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=5)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = self.generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys()))) )
            LOG.info("param---".format(param))

            if "custom" in param[0]:
                filter = random_keys[count]
                count = count + 1

            else:
                filter = param[0]
            exclude = param[1]
            aggregation = param[2]
            creep_buffer = param[3]
            filter2 = param[4]
            additional_done_status = param[5]
            sprint_mandate_date = param[6]
            LOG.info("filter---{},exclude---{},aggregation---{},creep_buffer--{},"
                     "creep_buffer--{},filter2--{},additional_done_status---{},sprint_mandate_date---{}".format(filter,
                                                                                                                exclude,
                                                                                                                aggregation,
                                                                                                                creep_buffer,
                                                                                                                creep_buffer,
                                                                                                                filter2,
                                                                                                                additional_done_status,
                                                                                                                sprint_mandate_date))

            additional_done_statuses = create_sprint_metric_object.get_additional_statuses(
                int_ids=int_ids)
            if additional_done_status is not None:
                get_3_additional_done_statuses_values = random.sample(additional_done_statuses, min(3, len(additional_done_statuses)))
            else:
                get_3_additional_done_statuses_values = None
            custom_value = (custom[random_keys[count]])
            # breakpoint()
            flag_list, payload, widget_response = create_sprint_metric_object.widget_creation_sprint_metrics_trend_report(
                filter,
                creep_buffer,
                filter2,
                int_ids,
                gt, lt,
                custom_value,
                exclude,
                get_3_additional_done_statuses_values,
                i,
                aggregation,
                sprint_mandate_date=sprint_mandate_date)
            df = pd.DataFrame(
                {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)]})
            if len(flag_list) != 0:
                flag.append(flag_list)
            df.to_csv(
                "log_updates/" + str(inspect.stack()[0][3])
                + '.csv', header=True,
                index=False, mode='a')

        assert len(flag) == 0, "The flag list is not empty-----{}".format(flag_list)

    @pytest.mark.parametrize("param", jira_filter_single_stat)
    @pytest.mark.regression
    def test_sprint_metric_widget_single_stat_drilldown_comparison_jira(self, create_sprint_metric_object,
                                                                        create_generic_object, param):
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=5)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = self.generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            random_keys = random.sample(list(custom.keys()) , min(3, len(list(custom.keys()))) )
            LOG.info("param---".format(param))

            if "custom" in param[0]:
                filter = random_keys[count]
                count = count + 1

            else:
                filter = param[0]
            exclude = param[1]
            aggregation = param[2]
            creep_buffer = param[3]
            filter2 = param[4]
            additional_done_status = param[5]
            metric = param[6]
            sprint_mandate_date = param[7]
            LOG.info("filter---{},exclude---{},aggregation---{},creep_buffer--{},"
                     "creep_buffer--{},filter2--{},additional_done_status---{}".format(filter, exclude, aggregation,
                                                                                       creep_buffer, creep_buffer,
                                                                                       filter2,
                                                                                       additional_done_status))

            additional_done_statuses = create_sprint_metric_object.get_additional_statuses(
                int_ids=int_ids)
            if additional_done_status is not None:
                get_3_additional_done_statuses_values = random.sample(additional_done_statuses, min(3,len(additional_done_statuses)) )
            else:
                get_3_additional_done_statuses_values = None
            custom_value = (custom[random_keys[count]])

            flag_list, payload, widget_response = create_sprint_metric_object.widget_creation_sprint_metrics_trend_report(
                filter,
                creep_buffer,
                filter2,
                int_ids,
                gt, lt,
                custom_value,
                exclude,
                get_3_additional_done_statuses_values,
                i,
                aggregation,
                metric=metric, sprint_mandate_date=sprint_mandate_date)
            df = pd.DataFrame(
                {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)]})
            if len(flag_list) != 0:
                flag.append(flag_list)
            df.to_csv(
                "log_updates/" + str(inspect.stack()[0][3])
                + '.csv', header=True,
                index=False, mode='a')

        assert len(flag) == 0, "The flag list is not empty-----{}".format(flag_list)
