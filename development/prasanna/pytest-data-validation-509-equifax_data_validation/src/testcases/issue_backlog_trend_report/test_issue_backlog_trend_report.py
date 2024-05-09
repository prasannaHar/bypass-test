import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestIssueBacklogTrend:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_backlog_trend_001(self, create_issue_backlog_object, create_generic_object,
                                               create_widget_helper_object, widget_schema_validation):

        """Validate alignment of Issue Progress Report"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        snapshot_range = {"$gt": gt, "$lt": lt}
        create_issue_backlog_trend = create_issue_backlog_object.issue_backlog_trend_report(product_id=project_id,
                                                                                            snapshot_range=snapshot_range,
                                                                                            )
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_backlog_trend, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_issue_backlog_trend(),
                                                       create_issue_backlog_trend,
                                                       "Schema validation failed for reate issues backlog widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_backlog_trend_002(self, create_issue_backlog_object, create_generic_object,create_widget_helper_object, widget_schema_validation):

        """Validate alignment of Issue Progress Report"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        var_filter = {"issue_resolved_at": {"$gt": gt, "$lt": lt}}
        snapshot_range = {"$gt": gt, "$lt": lt}
        create_issue_backlog_trend = create_issue_backlog_object.issue_backlog_trend_report(product_id=project_id,
                                                                                            snapshot_range=snapshot_range,
                                                                                            var_filters=var_filter
                                                                                            )
        assert create_issue_backlog_trend, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_issue_backlog_trend(),
                                                       create_issue_backlog_trend,
                                                       "Schema validation failed for reate issues backlog widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_backlog_object.issue_backlog_trend_report(product_id=project_id,
                                                                           snapshot_range=snapshot_range, keys=True,var_filters=var_filter
                                                                           )
        for key, total_tickets in key_value.items():
            drilldown = create_issue_backlog_object.issue_backlog_trend_drilldown(project_id,
                                                                                  snapshot_range=snapshot_range,var_filters=var_filter,
                                                                                  key=key)
            assert total_tickets == drilldown['_metadata']["total_count"], "Mismatch data in the drill down"
        #
        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_issue_backlog_trend_report_003(self, create_issue_backlog_object, create_generic_object,create_widget_helper_object, widget_schema_validation):

        """Validate alignment of Issue Progress Report"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        snapshot_range = {"$gt": gt, "$lt": lt}
        filters = {"leftYAxis": "total_tickets", "rightYAxis": "median","issue_resolved_at": {"$gt": gt, "$lt": lt},
                   "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}, "priorities": ["HIGHEST", "HIGH"]}
        create_issue_backlog_trend = create_issue_backlog_object.issue_backlog_trend_report(product_id=project_id,
                                                                                            snapshot_range=snapshot_range,
                                                                                            var_filters=filters
                                                                                            )
        assert create_issue_backlog_trend, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_issue_backlog_trend(),
                                                       create_issue_backlog_trend,
                                                       "Schema validation failed for reate issues backlog widget endpoint")

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_issue_backlog_trend_report_004(self, create_issue_backlog_object, create_generic_object,create_widget_helper_object, widget_schema_validation):

        """Validate alignment of Issue Progress Report"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        snapshot_range = {"$gt": gt, "$lt": lt}
        filters = {"leftYAxis": "total_tickets", "rightYAxis": "median",
                   "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}, "priorities": ["HIGHEST", "HIGH"]}
        create_issue_backlog_trend = create_issue_backlog_object.issue_backlog_trend_report(product_id=project_id,
                                                                                            snapshot_range=snapshot_range,
                                                                                            var_filters=filters
                                                                                            )
        assert create_issue_backlog_trend, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_issue_backlog_trend(),
                                                       create_issue_backlog_trend,
                                                       "Schema validation failed for reate issues backlog widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_backlog_object.issue_backlog_trend_report(product_id=project_id,
                                                                           snapshot_range=snapshot_range, keys=True,
                                                                           var_filters=filters
                                                                           )
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for key, total_tickets in key_value.items():
            drilldown = create_issue_backlog_object.issue_backlog_trend_drilldown(project_id,
                                                                                  snapshot_range=snapshot_range,
                                                                                  key=key, var_filters=filters)
            assert total_tickets == drilldown['_metadata']["total_count"], "Mismatch data in the drill down"
