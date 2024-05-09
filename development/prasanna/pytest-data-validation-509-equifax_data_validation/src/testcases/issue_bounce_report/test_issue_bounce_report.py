import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueBounce:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_bounce_001(self, create_issue_bounce_object, create_generic_object,
                              create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue Bounce Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        var_filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}

        LOG.info("==== create widget with available filter ====")
        create_issue_bounce = create_issue_bounce_object.issue_bounce_report(var_filters=var_filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_bounce, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_bounce_report(),
            create_issue_bounce,
            "Schema validation failed for create issues bounce widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_bounce_002(self, create_issue_bounce_object, create_generic_object,
                              create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue Bounce Report"""

        LOG.info("==== create widget with available filter ====")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        var_filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}
        create_issue_bounce = create_issue_bounce_object.issue_bounce_report(var_filters=var_filter)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_bounce, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_issue_bounce_report(),
            create_issue_bounce,
            "Schema validation failed for reate issues bounce widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_bounce_object.issue_bounce_report(keys=True, var_filters=var_filter)
        for eachRecord in key_value:
            drilldown = create_issue_bounce_object.issue_bounce_drilldown(key=eachRecord["key"], var_filters=var_filter)
            bounce_value = create_issue_bounce_object.issue_bounce_drilldown(key=eachRecord["key"], bounces=True,
                                                                             var_filters=var_filter)
            drilldown_min = min(bounce_value)
            drilldown_max = max(bounce_value)

            assert eachRecord["total_tickets"] == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"
            assert eachRecord["min"] == drilldown_min, "Mismatch data in the drill down"
            assert eachRecord["max"] == drilldown_max, "Mismatch data in the drill down"
        #
        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_issue_bounce_003(self, create_generic_object, create_issue_bounce_object,
                              create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Issue Bounce Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        get_filter_response_assignee = create_generic_object.get_filter_options(arg_filter_type=["assignee"],
                                                                                arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_assignee['records'][0]['assignee']]
        assignee_value_exclude = []
        for eachassignee in temp_records_project[0]:
            if eachassignee['additional_key'] == "Former user":
                assignee_value_exclude.append(eachassignee['key'])
        filters = {"exclude": {"assignees": assignee_value_exclude}, "issue_resolved_at": {"$gt": gt, "$lt": lt},
                   "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}}
        LOG.info("==== create widget with available filter ====")
        create_issue_bounce = create_issue_bounce_object.issue_bounce_report(var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_bounce, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_bounce_report(),
                                                       create_issue_bounce,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_issue_bounce_004(self, create_generic_object, create_issue_bounce_object,
                              create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Issue Bounce Report"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        get_filter_response_assignee = create_generic_object.get_filter_options(arg_filter_type=["assignee"],
                                                                                arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_assignee['records'][0]['assignee']]
        assignee_value_exclude = []
        for eachassignee in temp_records_project[0]:
            if eachassignee['additional_key'] == "Former user":
                assignee_value_exclude.append(eachassignee['key'])
        filters = {"exclude": {"assignees": assignee_value_exclude},"issue_resolved_at": {"$gt": gt, "$lt": lt}, "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}}

        LOG.info("==== create widget with available filter ====")
        create_issue_bounce = create_issue_bounce_object.issue_bounce_report(var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_bounce, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_bounce_report(),
                                                       create_issue_bounce,
                                                       "Schema validation failed for reate issues bounce widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_bounce_object.issue_bounce_report(keys=True, var_filters=filters)
        for eachRecord in key_value:
            drilldown = create_issue_bounce_object.issue_bounce_drilldown(key=eachRecord["key"], var_filters=filters)
            bounce_value = create_issue_bounce_object.issue_bounce_drilldown(key=eachRecord["key"], bounces=True,
                                                                             var_filters=filters)
            drilldown_min = min(bounce_value)
            drilldown_max = max(bounce_value)

            assert eachRecord["total_tickets"] == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down"
            assert eachRecord["min"] == drilldown_min, "Mismatch data in the drill down"
            assert eachRecord["max"] == drilldown_max, "Mismatch data in the drill down"
        #
        LOG.info("==== Validate the data in the widget with different filters ====")
