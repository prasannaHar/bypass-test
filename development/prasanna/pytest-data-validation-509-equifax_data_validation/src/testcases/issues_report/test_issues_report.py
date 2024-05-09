import inspect
import logging
import random
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam
import pytest
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReport:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="issues_report_ticket")
    jira_filter_story_point = filter.retrieve_widget_test_parameters(report_name="issues_report_story_point")
    dependency_analysis = filter.retrieve_widget_test_parameters(report_name="dependency_analysis")

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issues_Report_001(self, create_issues_report_object, create_generic_object,
                               create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""

        LOG.info("==== create widget with available filter ====")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        var_filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}
        create_issues_report = create_issues_report_object.issues_report(var_filters=var_filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.sanity
    def test_issues_Report_002(self, create_issues_report_object,
                               create_widget_helper_object, widget_schema_validation, create_generic_object):
        """Validate alignment of Issues Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        var_filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}
        LOG.info("==== create widget with available filter ====")
        create_issues_report = create_issues_report_object.issues_report(var_filters=var_filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=var_filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=var_filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=3)
    def test_issues_Report_003(self, create_issues_report_object,
                               create_widget_helper_object, widget_schema_validation,create_generic_object):
        """Validate alignment of Issues Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        var_filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}
        LOG.info("==== create widget with available filter ====")
        create_issues_report = create_issues_report_object.issues_report(across="issue_type", metric="story_point",
                                                                         sort_id="story_points", stacks="issue_type",
                                                                         story=True, var_filters=var_filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

    @pytest.mark.run(order=4)
    def test_issues_Report_004(self, create_issues_report_object,
                               create_widget_helper_object, widget_schema_validation, create_generic_object):
        """Validate alignment of Issues Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        var_filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}

        LOG.info("==== create widget with available filter ====")
        create_issues_report = create_issues_report_object.issues_report(across="issue_type", metric="story_point",
                                                                         sort_id="story_points", stacks="issue_type",
                                                                         story=True, var_filters=var_filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(across="issue_type", metric="story_point",
                                                              sort_id="story_points", stacks="issue_type",
                                                              story=True, keys=True, var_filters=var_filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key_option="issue_types", key=key,
                                                                            across="issue_type",
                                                                            metric="story_point", stacks="issue_type",
                                                                            var_filters=var_filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=5)
    def test_issues_Report_filter_assignee_005(self, create_issues_report_object, create_generic_object,
                                               get_integration_obj,
                                               create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])

        LOG.info("==== create widget with available filter ====")
        get_filter_response_assignee = create_generic_object.get_filter_options(arg_filter_type=["assignee"],
                                                                                arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_assignee['records'][0]['assignee']]
        assignee_values = []
        for eachassignee in temp_records_project[0]:
            if eachassignee['additional_key'] != "_UNASSIGNED_":
                assignee_values.append(eachassignee['key'])
        filter = {"assignees": random.sample(assignee_values, min(5, len(assignee_values))), "issue_resolved_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=6)
    def test_issues_Report_filter_created_at_006(self, create_issues_report_object, create_generic_object,
                                                 create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=8)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filter = {"issue_created_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=7)
    def test_issues_Report_filter_resolved_at_007(self, create_issues_report_object, create_generic_object,
                                                  create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=8)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=8)
    def test_issues_Report_filter_updated_at_008(self, create_issues_report_object, create_generic_object,
                                                 create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=8)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filter = {"issue_updated_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=9)
    def test_issues_Report_filter_issue_type_009(self, create_issues_report_object, create_generic_object,
                                                 get_integration_obj,
                                                 create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        LOG.info("==== create widget with available filter ====")
        get_filter_response_issueType = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                                 arg_integration_ids=get_integration_obj)

        temp_records_issueType = [get_filter_response_issueType['records'][0]['issue_type']]
        issueType_value = []
        for eachissueType in temp_records_issueType[0]:
            issueType_value.append(eachissueType['key'])
        filter = {"issue_types": random.sample(issueType_value, min(3, len(issueType_value))), "issue_resolved_at": {"$gt": gt, "$lt": lt}}
        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=10)
    def test_issues_Report_filter_project_010(self, create_issues_report_object, create_generic_object,
                                              get_integration_obj,
                                              create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])

        LOG.info("==== create widget with available filter ====")
        get_filter_response_project = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                               arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_project['records'][0]['project_name']]
        project_value = []
        for eachproject in temp_records_project[0]:
            project_value.append(eachproject['key'])
        filter = {"jira_projects": random.sample(project_value, min(3, len(project_value))),"issue_resolved_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=11)
    def test_issues_Report_filter_priority_011(self, create_issues_report_object, create_generic_object,
                                               get_integration_obj,
                                               create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""

        LOG.info("==== create widget with available filter ====")
        get_filter_response_priority = create_generic_object.get_filter_options(arg_filter_type=["priority"],
                                                                                arg_integration_ids=get_integration_obj)
        temp_records_priority = [get_filter_response_priority['records'][0]['priority']]
        priority_value = []
        for eachpriority in temp_records_priority[0]:
            priority_value.append(eachpriority['key'])
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filter = {"priorities": random.sample(priority_value, min(2, len(priority_value))),"issue_resolved_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=12)
    @pytest.mark.regression
    def test_issues_Report_filter_status_012(self, create_issues_report_object, create_generic_object,
                                             get_integration_obj,
                                             create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""

        LOG.info("==== create widget with available filter ====")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        get_filter_response_status = create_generic_object.get_filter_options(arg_filter_type=["status"],
                                                                              arg_integration_ids=get_integration_obj)
        temp_records_status = [get_filter_response_status['records'][0]['status']]
        status_value = []
        for eachstatus in temp_records_status[0]:
            status_value.append(eachstatus['key'])
        filter = {"statuses": random.sample(status_value, min(15, len(temp_records_status[0]))),
                  "issue_resolved_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=13)
    def test_issues_Report_filter_story_points_013(self, create_issues_report_object, create_generic_object,
                                                   create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issues Report"""

        LOG.info("==== create widget with available filter ====")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filter = {"story_points": {"$gt": "1", "$lt": "23"}, "issue_resolved_at": {"$gt": gt, "$lt": lt}}

        create_issues_report = create_issues_report_object.issues_report(var_filters=filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issues_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issues_report(),
                                                       create_issues_report,
                                                       "Schema validation failed for isuues Report widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issues_report_object.issues_report(keys=True, var_filters=filter)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for key, total_tickets in key_value.items():
            drilldown = create_issues_report_object.issues_report_drilldown(key=key, var_filters=filter)
            assert total_tickets == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.parametrize("param", jira_filter_ticket)
    @pytest.mark.regression
    def test_issues_repor_widget_drilldown_verification_ticket(self, create_generic_object,
                                                               create_issues_report_object, param):
        custom_value = None
        # param=('projects', 'resolution', 'issue_due', 'week', 'issue_resolved_at', 'None', 'None', 'last_sprint', 'default_old-latest', 'ticket', 'None')
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=7, type="days")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        gt = str(int(gt) - 19800)
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
            stacks = param[1]
            across = param[2]
            interval = param[3]
            datetime_filters = param[4]
            filter2 = param[5]
            if param[6] is None:
                exclude = None
            elif "custom" in param[6]:
                exclude = "custom-" + param[6]
            else:
                exclude = param[6]
            sprint = param[7]
            sort_x_axis = param[8]

            metric = param[9]
            dependency_analysis = param[10]
            if across in ["issue_due", "issue_created", "issue_resolved", "issue_updated"]:
                gt, lt = None, None
            # breakpoint()
            flag_list, payload = create_issues_report_object.widget_creation_issues_report(filter=filter,
                                                                                           filter2=filter2,
                                                                                           int_ids=int_ids,
                                                                                           gt=gt,
                                                                                           lt=lt,
                                                                                           custom_values=custom_value,
                                                                                           stacks=stacks,
                                                                                           interval=interval,
                                                                                           exclude=exclude,
                                                                                           ou_id=i,
                                                                                           metric=metric,
                                                                                           datetime_filters=datetime_filters,
                                                                                           sprint=sprint,
                                                                                           sort_x_axis=sort_x_axis,
                                                                                           sprint_field=sprint_field,
                                                                                           across=across,
                                                                                           dependency_analysis=dependency_analysis,
                                                                                           across_limit=20
                                                                                           )
            if len(flag_list) != 0:
                df = pd.DataFrame(
                    {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)]})
                flag.append(flag_list)
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')
                flag.append(flag_list)

        assert len(flag) == 0, "Flag list is not empty---{}".format(flag_list)

    @pytest.mark.parametrize("param", jira_filter_story_point)
    @pytest.mark.regression
    def test_issues_repor_widget_drilldown_verification_story_point(self, param, create_generic_object,
                                                                    create_issues_report_object):
        custom_value = None
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=7, type="days")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        gt = str(int(gt) - 19800)
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
            stacks = param[1]
            across = param[2]
            interval = param[3]
            datetime_filters = param[4]
            filter2 = param[5]
            if param[6] is None:
                exclude = None
            elif "custom" in param[6]:
                exclude = "custom-" + param[6]
            else:
                exclude = param[6]
            sprint = param[7]
            sort_x_axis = param[8]

            metric = param[9]
            dependency_analysis = param[10]
            # breakpoint()
            flag_list, payload = create_issues_report_object.widget_creation_issues_report(filter=filter,
                                                                                           filter2=filter2,
                                                                                           int_ids=int_ids,
                                                                                           gt=gt,
                                                                                           lt=lt,
                                                                                           custom_values=custom_value,
                                                                                           stacks=stacks,
                                                                                           interval=interval,
                                                                                           exclude=exclude,
                                                                                           ou_id=i,
                                                                                           metric=metric,
                                                                                           datetime_filters=datetime_filters,
                                                                                           sprint=sprint,
                                                                                           sort_x_axis=sort_x_axis,
                                                                                           sprint_field=sprint_field,
                                                                                           across=across,
                                                                                           across_limit=20,
                                                                                           dependency_analysis=dependency_analysis
                                                                                           )

            if len(flag_list) != 0:
                df = pd.DataFrame(
                    {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)]})
                flag.append(flag_list)
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')
                flag.append(flag_list)

        assert len(flag) == 0, "Flag list is not empty---{}".format(flag_list)

    @pytest.mark.parametrize("param", dependency_analysis)
    def test_dependency_analysis_stack_vs_no_stack_issues_report(self, param, create_generic_object,
                                                                 create_issues_report_object):
        custom_value = None
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=15, type="days")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        gt = str(int(gt) - 19800)
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
            stacks = param[1]
            across = param[2]
            interval = param[3]
            datetime_filters = param[4]
            filter2 = param[5]
            if param[6] is None:
                exclude = None
            elif "custom" in param[6]:
                exclude = "custom-" + param[6]
            else:
                exclude = param[6]
            sprint = param[7]
            sort_x_axis = param[8]

            metric = param[9]
            dependency_analysis = param[10]
            res = create_issues_report_object.issues_report_creation(filter=filter,
                                                                     filter2=filter2,
                                                                     int_ids=int_ids,
                                                                     gt=gt,
                                                                     lt=lt,
                                                                     custom_values=custom_value,
                                                                     stacks=stacks,
                                                                     interval=interval,
                                                                     exclude=exclude,
                                                                     ou_id=i,
                                                                     metric=metric,
                                                                     datetime_filters=datetime_filters,
                                                                     sprint=sprint,
                                                                     sort_x_axis=sort_x_axis,
                                                                     sprint_field=sprint_field,
                                                                     across=across,
                                                                     across_limit=20,
                                                                     dependency_analysis=dependency_analysis
                                                                     )

            res_without_stacks = create_issues_report_object.issues_report_creation(filter=filter,
                                                                                    filter2=filter2,
                                                                                    int_ids=int_ids,
                                                                                    gt=gt,
                                                                                    lt=lt,
                                                                                    custom_values=custom_value,
                                                                                    stacks=None,
                                                                                    interval=interval,
                                                                                    exclude=exclude,
                                                                                    ou_id=i,
                                                                                    metric=metric,
                                                                                    datetime_filters=datetime_filters,
                                                                                    sprint=sprint,
                                                                                    sort_x_axis=sort_x_axis,
                                                                                    sprint_field=sprint_field,
                                                                                    across=across,
                                                                                    across_limit=20,
                                                                                    dependency_analysis=dependency_analysis
                                                                                    )

            df_stacks = pd.json_normalize(res['records'], max_level=1)
            df_without_stacks = pd.json_normalize(res_without_stacks['records'], max_level=1)
            diff_df = df_stacks[df_stacks['total_tickets'] != df_without_stacks['total_tickets']]
            assert diff_df.empty, "There is difference between the response with and without stacks"

    @pytest.mark.parametrize("param", dependency_analysis)
    # @pytest.mark.regression
    def test_issues_repor_widget_drilldown_verification_ticket_dependency_analysis(self, create_generic_object,
                                                                                   create_issues_report_object
                                                                                   , param
                                                                                   ):
        custom_value = None
        # param = (
        # 'None', 'resolution', 'issue_due', 'week', 'issue_resolved_at', 'None', 'None', 'None', 'default_old-latest',
        # 'ticket', 'relates to')
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=7, type="days")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        gt = str(int(gt) - 19800)
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
            random_keys =  random.sample(list(custom.keys()), min(3, len(list(custom.keys()))) )
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            stacks = param[1]
            across = param[2]
            interval = param[3]
            datetime_filters = param[4]
            filter2 = param[5]
            if param[6] is None:
                exclude = None
            elif "custom" in param[6]:
                exclude = "custom-" + param[6]
            else:
                exclude = param[6]
            sprint = param[7]
            sort_x_axis = param[8]

            metric = param[9]
            dependency_analysis = param[10]
            # if across in ["issue_due", "issue_created", "issue_resolved", "issue_updated"]:
            #     gt, lt = None, None
            # breakpoint()
            flag_list, payload = create_issues_report_object.widget_creation_issues_report(filter=filter,
                                                                                           filter2=filter2,
                                                                                           int_ids=int_ids,
                                                                                           gt=gt,
                                                                                           lt=lt,
                                                                                           custom_values=custom_value,
                                                                                           stacks=stacks,
                                                                                           interval=interval,
                                                                                           exclude=exclude,
                                                                                           ou_id=i,
                                                                                           metric=metric,
                                                                                           datetime_filters=datetime_filters,
                                                                                           sprint=sprint,
                                                                                           sort_x_axis=sort_x_axis,
                                                                                           sprint_field=sprint_field,
                                                                                           across=across,
                                                                                           dependency_analysis=dependency_analysis,
                                                                                           across_limit=20
                                                                                           )
            if len(flag_list) != 0:
                df = pd.DataFrame(
                    {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)]})
                flag.append(flag_list)
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')
                flag.append(flag_list)

        assert len(flag) == 0, "Flag list is not empty---{}".format(flag_list)
        LOG.info("Test executed Successfully")
