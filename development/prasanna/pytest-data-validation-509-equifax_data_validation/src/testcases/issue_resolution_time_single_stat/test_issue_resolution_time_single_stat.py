import logging
import pytest
import random
import pandas as pd
import inspect
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReport:
    filter = reportparam()
    jira_filter = filter.retrieve_widget_test_parameters(report_name="issue_resolution_time_single_stat")

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_resolution_time_single_stat_001(self, create_generic_object,
                                                   create_issue_resolution_time_single_stat_object,
                                                   create_widget_helper_object, widget_schema_validation):
        """Validate alignment of issue resolution time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issue_resolution_time_single_stat = create_issue_resolution_time_single_stat_object.issue_resolution_time_single_stat(
            time_range_value=time_range_value)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time_single_stat,
            "Schema validation failed for create issues resolution time single stat widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_resolution_time_single_stat_002(self, create_generic_object,
                                                   create_issue_resolution_time_single_stat_object,
                                                   create_widget_helper_object, widget_schema_validation):
        """Validate alignment of issue resolution time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issue_resolution_time_single_stat = create_issue_resolution_time_single_stat_object.issue_resolution_time_single_stat(
            time_range_value=time_range_value)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time_single_stat,
            "Schema validation failed for create issues resolution time single stat widget endpoint")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_issue_resolution_time_single_stat_003(self, create_generic_object,
                                                   create_issue_resolution_time_single_stat_object,
                                                   create_widget_helper_object, widget_schema_validation):
        """Validate alignment of issue resolution time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issue_resolution_time_single_stat = create_issue_resolution_time_single_stat_object.issue_resolution_time_single_stat(
            time_range_value=time_range_value, across="issue_resolved")
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time_single_stat,
            "Schema validation failed for create issues resolution time single stat widget endpoint")

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_issue_resolution_time_single_stat_004(self, create_generic_object,
                                                   create_issue_resolution_time_single_stat_object,
                                                   create_widget_helper_object, widget_schema_validation):
        """Validate alignment of issue resolution time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issue_resolution_time_single_stat = create_issue_resolution_time_single_stat_object.issue_resolution_time_single_stat(
            time_range_value=time_range_value, across="issue_updated")
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time_single_stat,
            "Schema validation failed for create issues resolution time single stat widget endpoint")

    @pytest.mark.run(order=5)
    @pytest.mark.regression
    def test_issue_resolution_time_single_stat_005(self, create_generic_object,
                                                   create_issue_resolution_time_single_stat_object,
                                                   create_widget_helper_object, widget_schema_validation):
        """Validate alignment of issue resolution time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        create_issue_resolution_time_single_stat = create_issue_resolution_time_single_stat_object.issue_resolution_time_single_stat(
            time_range_value=time_range_value, agg_type="total")
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time_single_stat,
            "Schema validation failed for create issues resolution time single stat widget endpoint")

    @pytest.mark.run(order=6)
    @pytest.mark.regression
    def test_issue_resolution_time_single_stat_006(self, create_generic_object,
                                                   create_issue_resolution_time_single_stat_object,
                                                   create_widget_helper_object, widget_schema_validation,
                                                   get_integration_obj):
        """Validate alignment of issue resolution time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        time_range_value = {"$gt": gt, "$lt": lt}
        get_filter_response_priority = create_generic_object.get_filter_options(arg_filter_type=["priority"],
                                                                                arg_integration_ids=get_integration_obj)
        temp_records_priority = [get_filter_response_priority['records'][0]['priority']]
        priority_value = []
        for eachpriority in temp_records_priority[0]:
            priority_value.append(eachpriority['key'])
        priority_values = random.sample(priority_value, min(4, len(priority_value)))

        get_filter_response_issueType = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                                 arg_integration_ids=get_integration_obj)
        temp_records_issueType = [get_filter_response_issueType['records'][0]['issue_type']]
        issueType_value = []
        for eachissueType in temp_records_issueType[0]:
            issueType_value.append(eachissueType['key'])
        issue_types = random.sample(issueType_value, min(4, len(issueType_value)))

        filters = {"issue_types": issue_types, "priorities": priority_values}

        create_issue_resolution_time_single_stat = create_issue_resolution_time_single_stat_object.issue_resolution_time_single_stat(
            time_range_value=time_range_value, var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_resolution_time_single_stat(),
            create_issue_resolution_time_single_stat,
            "Schema validation failed for create issues resolution time single stat widget endpoint")

    @pytest.mark.parametrize("param", jira_filter)
    def test_issue_resolutions_single_stat_with_issues_report(self,param,
                                                              create_issue_resolution_time_single_stat_object,
                                                              create_generic_object):
        custom_value = None
        LOG.info("Param----,{}".format(param))
        # param = ('resolutions', 'issue_updated', 'projects', 'reporters', 'last_sprint', 'average', 'issue_updated_at')
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=45, type="days")
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
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys()))) )
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            stacks = None
            across = param[1]
            interval = None
            datetime_filters = param[6]
            filter2 = param[2]
            if param[3] is None:
                exclude = None
            elif "custom" in param[3]:
                exclude = "custom-" + param[3]
            else:
                exclude = param[3]
            sprint = param[4]
            sort_x_axis = None

            metric = None
            dependency_analysis = None
            agg_type = param[5]
            flag_list = create_issue_resolution_time_single_stat_object.create_resolution_single_stat_widget(
                filter=filter,
                filter2=filter2,
                across=across,
                exclude=exclude,
                sprint=sprint,
                agg_type=agg_type,
                datetime_filter=datetime_filters,
                gt=gt, lt=lt,
                custom_values=custom_value,
                int_ids=int_ids,
                ou_id=i,
                sprint_field=sprint_field)
            if len(flag) != 0:
                df = pd.DataFrame(
                    {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)]})
                flag.append(flag_list)
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')
                flag.append(flag_list)

        assert len(flag) == 0, f"The length of the flag list is not empty---{len(flag)},flag_list= {flag}"
