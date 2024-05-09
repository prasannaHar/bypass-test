import itertools
import logging
import random

import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReport:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="issue_lead_time_single_stat")

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issues_lead_time_by_stage_001(self, create_issue_lead_time_by_stage_report_object,
                                           create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time by stage"""

        LOG.info("==== create widget with available filter ====")
        create_issue_Lead_time_by_stage = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage()
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_by_stage, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_resolution_time_by_stage(),
                                                       create_issue_Lead_time_by_stage,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issues_lead_time_by_stage_002(self, create_issue_lead_time_by_stage_report_object,
                                           create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time by stage"""

        LOG.info("==== create widget with available filter ====")
        create_issue_Lead_time_by_stage = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage()
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_by_stage, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_resolution_time_by_stage(),
                                                       create_issue_Lead_time_by_stage,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage(keys=True)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for key, mean in key_value.items():
            drilldown = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage_drilldown(key=key,
                                                                                                          mean=True)
            assert -3600 <= (mean - drilldown) <= 3600, "Mismatch data in the drill down for key :" + key

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_issues_lead_time_by_stage_003(self, create_issue_lead_time_by_stage_report_object,
                                           create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time by stage"""

        LOG.info("==== create widget with available filter ====")
        filters = {"story_points": {"$gt": "1", "$lt": "100"}}
        create_issue_Lead_time_by_stage = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_by_stage, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_resolution_time_by_stage(),
                                                       create_issue_Lead_time_by_stage,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage(keys=True,
                                                                                            var_filters=filters)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for key, mean in key_value.items():
            drilldown = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage_drilldown(key=key,
                                                                                                          mean=True,
                                                                                                          var_filters=filters)
            assert -3600 <= (mean - drilldown) <= 3600, "Mismatch data in the drill down for key :" + key

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_issues_lead_time_by_stage_004(self, create_issue_lead_time_by_stage_report_object,
                                           create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time by stage"""

        LOG.info("==== create widget with available filter ====")
        filters = {"story_points": {"$gt": "1", "$lt": "100"}}
        create_issue_Lead_time_by_stage = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_by_stage, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_resolution_time_by_stage(),
                                                       create_issue_Lead_time_by_stage,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

    @pytest.mark.run(order=5)
    @pytest.mark.regression
    def test_issues_lead_time_by_stage_005(self, create_generic_object, create_issue_lead_time_by_stage_report_object,
                                           create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Issue lead time by stage"""
        LOG.info("==== create widget with available filter ====")
        get_filter_response_issueType = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                                 arg_integration_ids=get_integration_obj)

        temp_records_issueType = [get_filter_response_issueType['records'][0]['issue_type']]
        issueType_value = []
        for eachissueType in temp_records_issueType[0]:
            issueType_value.append(eachissueType['key'])
        if "STORY" in issueType_value:
            issueType = ["STORY"]
        else:
            issueType = [issueType_value[0]]
        get_filter_response_project = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                               arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_project['records'][0]['project_name']]
        gt, lt = create_generic_object.get_epoc_time(value=15, type="days")
        jira_issue_resolved_at = {"$gt": gt, "$lt": lt}
        filters = {"jira_issue_types": issueType, "jira_projects": create_generic_object.env["project_names"],
                   "jira_issue_resolved_at": jira_issue_resolved_at}
        create_issue_Lead_time_by_stage = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_by_stage, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_resolution_time_by_stage(),
                                                       create_issue_Lead_time_by_stage,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage(keys=True,
                                                                                            var_filters=filters)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for key, mean in key_value.items():
            drilldown = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage_drilldown(key=key,
                                                                                                          mean=True,
                                                                                                          var_filters=filters)
            assert -3600 <= (mean - drilldown) <= 3600, "Mismatch data in the drill down for key :" + key

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=6)
    @pytest.mark.regression
    def test_issues_lead_time_by_stage_006(self, create_generic_object, create_issue_lead_time_by_stage_report_object,
                                           create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Issue lead time by stage"""

        LOG.info("==== create widget with available filter ====")
        get_filter_response_issueType = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                                 arg_integration_ids=get_integration_obj)

        temp_records_issueType = [get_filter_response_issueType['records'][0]['issue_type']]
        issueType_value = []
        for eachissueType in temp_records_issueType[0]:
            issueType_value.append(eachissueType['key'])
        if "STORY" in issueType_value:
            issueType = ["STORY"]
        else:
            issueType = [issueType_value[0]]
        get_filter_response_project = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                               arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_project['records'][0]['project_name']]
        gt, lt = create_generic_object.get_epoc_time(value=15, type="days")
        jira_issue_resolved_at = {"$gt": gt, "$lt": lt}

        filters = {"jira_issue_types": issueType, "jira_projects": create_generic_object.env["project_names"],
                   "jira_issue_resolved_at": jira_issue_resolved_at}
        create_issue_Lead_time_by_stage = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_by_stage, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_resolution_time_by_stage(),
                                                       create_issue_Lead_time_by_stage,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

    @pytest.mark.parametrize("param", jira_filter_ticket)
    def test_lead_time_single_stat_1x_filters(self, create_issue_lead_time_by_stage_report_object,
                                              create_generic_object, create_widget_helper_object,
                                              widget_schema_validation, param):

        custom_value = None
        jira_velocity_ids = [create_generic_object.env['env_jira_velocity_config_id']]
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        # param=('fix_versions', 'jira_issue_resolved_at', 'projects', 'versions', 'None', 'ticket_velocity')
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        gt, lt = create_generic_object.get_epoc_time(value=20, type="days")
        count = 0
        for i, j in itertools.zip_longest(ou_id, jira_velocity_ids, fillvalue=jira_velocity_ids[0]):
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys())))  )
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            datetime_filters = param[1]
            filter2 = param[2]
            if param[3] is None:
                exclude = None
            elif "custom" in param[3]:
                exclude = "custom-" + param[3]
            else:
                exclude = param[3]
            sprint = param[4]
            metric = param[5]

            # breakpoint()
            create_issue_Lead_time_by_stage = create_issue_lead_time_by_stage_report_object.lead_time_single_stat(
                filter_type=filter,
                integration_id=int_ids,
                gt=gt,
                lt=lt,
                custom_values=custom_value,
                exclude=exclude,
                ou_id=i,
                datetime_filters=datetime_filters,
                sprint=sprint,
                sprint_field=sprint_field,
                velocity_config_id=j
            )

        create_issue_Lead_time = create_issue_lead_time_by_stage_report_object.issues_lead_time_by_stage()
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_by_stage, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_resolution_time_by_stage(),
                                                       create_issue_Lead_time,
                                                       "Schema validation failed for lead time single stat")
