import logging
import pytest
import random
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReport:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="issue_single_stat")

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issues_single_stat_001(self, create_generic_object, create_issues_single_stat_object,
                                    create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues single stat """

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(type="days", value=30)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issues_single_start = create_issues_single_stat_object.issues_single_stat(
            time_range_value=time_range_value)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_single_start, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_single_start,
                                                       "Schema validation failed for issues Single Stat widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issues_single_stat_002(self, create_generic_object, create_issues_single_stat_object,
                                    create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues single stat """

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(type="days", value=30)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issues_single_start = create_issues_single_stat_object.issues_single_stat(
            time_range_value=time_range_value,
            across="issue_updated")
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_single_start, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_single_start,
                                                       "Schema validation failed for issues Single Stat widget endpoint")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_issues_single_stat_003(self, create_generic_object, create_issues_single_stat_object,
                                    create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues single stat """

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(type="days", value=30)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issues_single_start = create_issues_single_stat_object.issues_single_stat(
            time_range_value=time_range_value,
            across="issue_resolved")
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_single_start, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_single_start,
                                                       "Schema validation failed for issues Single Stat widget endpoint")

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_issues_single_stat_004(self, create_generic_object, create_issues_single_stat_object,
                                    create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues single stat """

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issues_single_start = create_issues_single_stat_object.issues_single_stat(
            time_range_value=time_range_value)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_single_start, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_single_start,
                                                       "Schema validation failed for issues Single Stat widget endpoint")

    @pytest.mark.run(order=5)
    def test_issues_single_stat_005(self, create_generic_object, create_issues_single_stat_object,
                                    create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Issues single stat """

        project_names = create_generic_object.env["project_names"]
        LOG.info("==== create widget with available filter ====")
        get_filter_response_label = create_generic_object.get_filter_options(
            arg_filter_type=["label"],
            arg_integration_ids=get_integration_obj)

        temp_records_label = [get_filter_response_label['records'][0]['label']]
        label_value = []
        for eachlabel in temp_records_label[0]:
            label_value.append(eachlabel['key'])

        get_filter_response_priority = create_generic_object.get_filter_options(arg_filter_type=["priority"],
                                                                                arg_integration_ids=get_integration_obj)
        temp_records_priority = [get_filter_response_priority['records'][0]['priority']]
        priority_value = []
        for eachpriority in temp_records_priority[0]:
            priority_value.append(eachpriority['key'])
        priority_values = random.sample(priority_value, min(4, len(priority_value)))

        filters = {"labels": label_value, "projects": project_names, "priorities": priority_values}
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issues_single_start = create_issues_single_stat_object.issues_single_stat(
            time_range_value=time_range_value,
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_single_start, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_single_start,
                                                       "Schema validation failed for issues Single Stat widget endpoint")

    @pytest.mark.parametrize("param", jira_filter_ticket)
    @pytest.mark.regression
    def test_issues_single_stat_with_all_1x_jira_filters(self, create_generic_object, create_issues_single_stat_object,
                                                         param):
        custom_value = None
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
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
            random_keys = random.sample(list(custom.keys()) , min(3, len(list(custom.keys()))) )
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
            datetime_filters = param[2]
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:
                exclude = "custom-" + param[4]
            else:
                exclude = param[4]
            sprint = param[5]

            flag_list = create_issues_single_stat_object.widget_creation_and_comparing__issues_single_stat_vs_issues_report(
                filter, filter2, int_ids, gt, lt, custom_value, exclude, i, datetime_filters, sprint, sprint_field,
                across=across)
            if len(flag_list) != 0:
                flag.append(flag_list)

        assert len(flag) == 0, f"Flag list is not empty ----{flag}"
