import inspect
import json
import logging
import pytest
import random
import pandas as pd
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReport:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="issue_resolution_time_report")

    @pytest.mark.run(order=1)
    @pytest.mark.sanity
    def test_issue_resolution_time_001(self, create_generic_object, create_issue_resolution_time_report_object,
                                       create_widget_helper_object, widget_schema_validation):
        """Validate alignment of issue resolution time report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolve = {"$gt": gt, "$lt": lt}
        create_issue_resolution_time = create_issue_resolution_time_report_object.issue_resolution_time_report(
            issue_resolve=issue_resolve)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time,
            "Schema validation failed for reate issue resolution time report widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.sanity
    def test_issue_resolution_time_002(self, create_generic_object, create_issue_resolution_time_report_object,
                                       create_widget_helper_object, widget_schema_validation):
        """Validate alignment of issue resolution time report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolve = {"$gt": gt, "$lt": lt}
        create_issue_resolution_time = create_issue_resolution_time_report_object.issue_resolution_time_report(
            issue_resolve=issue_resolve)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time,
            "Schema validation failed for reate issue resolution time report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_resolution_time_report_object.issue_resolution_time_report(
            issue_resolve=issue_resolve, keys=True)
        for key, count in key_value.items():
            drilldown = create_issue_resolution_time_report_object.issue_resolution_time_report_drilldown(
                issue_resolve=issue_resolve, key=key)
            assert count == drilldown["_metadata"]["total_count"], "Mismatch data in the drill down for key " + key

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=3)
    def test_issue_resolution_time_003(self, create_generic_object, create_issue_resolution_time_report_object,
                                       create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of issue resolution time report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolve = {"$gt": gt, "$lt": lt}

        get_filter_response_status = create_generic_object.get_filter_options(arg_filter_type=["status"],
                                                                              arg_integration_ids=get_integration_obj)
        temp_records_status = [get_filter_response_status['records'][0]['status']]
        status_value = []
        for eachassignee in temp_records_status[0]:
            status_value.append(eachassignee['key'])
        filters = {"exclude": {"stages": random.sample(status_value, min(3, len(status_value)))}}
        create_issue_resolution_time = create_issue_resolution_time_report_object.issue_resolution_time_report(
            issue_resolve=issue_resolve, var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time,
            "Schema validation failed for reate issue resolution time report widget endpoint")

    @pytest.mark.run(order=4)
    def test_issue_resolution_time_004(self, create_generic_object, create_issue_resolution_time_report_object,
                                       create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of issue resolution time report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolve = {"$gt": gt, "$lt": lt}

        get_filter_response_status = create_generic_object.get_filter_options(arg_filter_type=["status"],
                                                                              arg_integration_ids=get_integration_obj)
        temp_records_status = [get_filter_response_status['records'][0]['status']]
        status_value = []
        for eachassignee in temp_records_status[0]:
            status_value.append(eachassignee['key'])
        filters = {"exclude": {"stages": random.sample(status_value, min(3, len(status_value)))}}
        create_issue_resolution_time = create_issue_resolution_time_report_object.issue_resolution_time_report(
            issue_resolve=issue_resolve, var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time,
            "Schema validation failed for reate issue resolution time report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_resolution_time_report_object.issue_resolution_time_report(
            issue_resolve=issue_resolve, keys=True, var_filters=filters)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for key, count in key_value.items():
            drilldown = create_issue_resolution_time_report_object.issue_resolution_time_report_drilldown(
                issue_resolve=issue_resolve, key=key, var_filters=filters)
            assert count == drilldown["_metadata"]["total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.parametrize("param", jira_filter_ticket)
    @pytest.mark.regression
    def test_issues_resolution_time_report_widget_drilldown_verification(self, create_generic_object, param,
                                                                         create_issue_resolution_time_report_object):
        custom_value = None

        LOG.info("Param----,{}".format(param))
        flag = []

        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt, lt = create_generic_object.get_epoc_time(value=5)
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys()))))
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            across = param[1]
            interval = param[2]
            datetime_filters = param[3]
            filter2 = param[4]
            if param[5] is None:
                exclude = None
            elif "custom" in param[5]:
                exclude = "custom-" + param[5]
            else:
                exclude = param[5]
            sprint = param[6]
            sort_x_axis = param[7]

            metric = param[8]
            # breakpoint()
            flag_list, payload = create_issue_resolution_time_report_object.widget_creation_issues_resolution_time_report(
                filter=filter,
                filter2=filter2,
                int_ids=int_ids,
                gt=gt,
                lt=lt,
                custom_values=custom_value,
                interval=interval,
                exclude=exclude,
                ou_id=i,
                metric=metric,
                datetime_filters=datetime_filters,
                sprint=sprint,
                sort_x_axis=sort_x_axis,
                sprint_field=sprint_field,
                across=across,
                across_limit=20
            )

            if len(flag_list) != 0:
                flag.append(flag_list)
                df = pd.DataFrame(
                    {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)]})
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')

        assert len(flag) == 0, "Flag list is not empty---{}".format(flag_list)

    @pytest.mark.parametrize("param", jira_filter_ticket)
    def test_issue_resolution_time_report_sorting(self, create_generic_object,
                                                  create_issue_resolution_time_report_object, param):

        custom_value = None
        # param=('custom_field', 'ticket_category', 'None', 'issue_resolved_at', 'projects', 'projects', 'None', 'value_low-high', 'median_resolution_time')
        LOG.info("Param----,{}".format(param))
        flag = []

        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt, lt = create_generic_object.get_epoc_time(value=5)
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys()))))
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            across = param[1]
            interval = param[2]
            datetime_filters = param[3]
            filter2 = param[4]
            if param[5] is None:
                exclude = None
            elif "custom" in param[5]:
                exclude = "custom-" + param[5]
            else:
                exclude = param[5]
            sprint = param[6]
            sort_x_axis = param[7]

            metric = param[8]

            filter = create_issue_resolution_time_report_object.jira_helper.jira_filter_creation(filter_type=None,
                                                                                                 filter2=filter2,
                                                                                                 integration_id=int_ids,
                                                                                                 gt=gt,
                                                                                                 lt=lt,
                                                                                                 custom_values=custom_value,
                                                                                                 sort_x_axis=sort_x_axis,
                                                                                                 exclude=None,
                                                                                                 datetime_filters=datetime_filters,
                                                                                                 sprint=sprint,
                                                                                                 metric=metric,
                                                                                                 report_type="resolution_time_report")

            payload = create_issue_resolution_time_report_object.issue_resolution_time_report_payload_generation(
                filter=filter,
                ou_id=i,
                sprint_field=sprint_field,
                across=across,
                across_limit=30,
                interval=interval, sort_xaxis=sort_x_axis)

            LOG.info(f"Payload--{json.dumps(payload)}")

            resp = create_issue_resolution_time_report_object.get_resolution_time_report_response(payload=payload)
            if len(resp['records']) == 0:
                pytest.skip("No records found")
            resp_df = pd.json_normalize(resp['records'])
            column_list = resp_df.columns.to_list()
            if "tickets" in metric:
                column = "total_tickets"
            elif "90th" in metric:
                column = "p90"
            elif "average" in metric:
                column = "mean"
            else:
                column = "median"

            if sort_x_axis == "label_high-low":
                if "additional_key" in column_list:
                    is_sorted_desc = resp_df['additional_key'].is_monotonic_decreasing
                    sort_list = resp_df['additional_key'].to_list()


                else:
                    is_sorted_desc = resp_df['key'].is_monotonic_decreasing
                    sort_list = resp_df['key'].to_list()

            elif sort_x_axis == "label_low-high":
                if "additional_key" in column_list:
                    is_sorted_desc = resp_df['additional_key'].is_monotonic_increasing
                    sort_list = resp_df['additional_key'].to_list()

                else:
                    is_sorted_desc = resp_df['key'].is_monotonic_increasing
                    sort_list = resp_df['key'].to_list()

            elif sort_x_axis == "value_high-low":
                is_sorted_desc = resp_df[column].is_monotonic_decreasing
                sort_list = resp_df[column].to_list()

            elif sort_x_axis == "value_low-high":
                is_sorted_desc = resp_df[column].is_monotonic_increasing
                sort_list = resp_df[column].to_list()

            elif sort_x_axis == "default_old-latest":
                is_sorted_desc = resp_df['key'].is_monotonic_increasing
                sort_list = resp_df['key'].to_list()
            else:
                is_sorted_desc = resp_df['key'].is_monotonic_decreasing
                sort_list = resp_df['key'].to_list()
            if not is_sorted_desc:
                flag.append({"sorting is not as expected": sort_list, "sort_xis": sort_x_axis, "metric": metric})

        assert len(flag) == 0, f"Flag is not empty----{flag}"
