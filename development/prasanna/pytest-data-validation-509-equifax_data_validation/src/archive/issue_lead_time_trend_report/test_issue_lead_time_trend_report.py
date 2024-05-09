import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReport:
    @pytest.mark.run(order=1)
    def test_issues_lead_time_trend_001(self, create_issue_lead_time_trend_report_object,
                                        create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time TREND REPORT"""

        LOG.info("==== create widget with available filter ====")
        create_issue_Lead_time_trend = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report()
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_trend, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_lead_time_trend_report(),
                                                       create_issue_Lead_time_trend,
                                                       "Schema validation failed for Issue lead time TREND REPORT  widget endpoint")

    @pytest.mark.run(order=2)
    def test_issues_lead_time_trend_002(self, create_issue_lead_time_trend_report_object,
                                        create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time TREND REPORT """

        LOG.info("==== create widget with available filter ====")
        create_issue_Lead_time_trend = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report()
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_trend, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_lead_time_trend_report(),
                                                       create_issue_Lead_time_trend,
                                                       "Schema validation failed for Issue lead time TREND REPORT  widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report(keys=True)
        for key, count in key_value.items():
            drilldown = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report_drilldown(key=key)
            assert count == drilldown["count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")

    @pytest.mark.run(order=3)
    def test_issues_lead_time_trend_003(self, create_issue_lead_time_trend_report_object,
                                        create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time TREND REPORT """

        LOG.info("==== create widget with available filter ====")
        filters = {"story_points": {"$gt": "1", "$lt": "100"}}
        create_issue_Lead_time_trend = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_trend, "widget is not created"

    @pytest.mark.run(order=4)
    def test_issues_lead_time_trend_004(self, create_issue_lead_time_trend_report_object,
                                        create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Issue lead time TREND REPORT """

        LOG.info("==== create widget with available filter ====")
        filters = {"story_points": {"$gt": "1", "$lt": "100"}}
        create_issue_Lead_time_trend = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")

        assert create_issue_Lead_time_trend, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_issue_lead_time_trend_report(),
                                                       create_issue_Lead_time_trend,
                                                       "Schema validation failed for Issue lead time TREND REPORT  widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report(var_filters=filters,
                                                                                             keys=True)
        for key, count in key_value.items():
            drilldown = create_issue_lead_time_trend_report_object.issues_lead_time_trend_report_drilldown(
                var_filters=filters, key=key)
            assert count == drilldown["count"], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")
