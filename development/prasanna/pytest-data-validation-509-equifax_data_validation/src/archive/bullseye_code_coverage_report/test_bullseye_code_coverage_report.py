import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestBullseyeCodeCoverageReport:
    @pytest.mark.run(order=1)
    @pytest.mark.need_maintenance
    def test_bullseye_code_coverage_001(self, create_bullseye_code_coverage_object,
                                           create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Bullseye code coverage report"""

        LOG.info("==== create widget with available filter ====")
        create_bullseye_code_coverage_report = create_bullseye_code_coverage_object.bullseye_code_coverage()

        assert create_bullseye_code_coverage_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_bullseye_code_coverage(),
                                                       create_bullseye_code_coverage_report,
                                                       "Schema validation failed for Bullseye Code Coverage Report  widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.need_maintenance
    def test_bullseye_code_coverage_002(self, create_bullseye_code_coverage_object,
                                              create_widget_helper_object, widget_schema_validation):
        """Validate alignment of Bullseye code coverage report"""

        LOG.info("==== create widget with available filter ====")
        create_bullseye_code_coverage_report = create_bullseye_code_coverage_object.bullseye_code_coverage()

        assert create_bullseye_code_coverage_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_bullseye_code_coverage(),
                                                       create_bullseye_code_coverage_report,
                                                       "Schema validation failed for Bullseye Code Coverage Report  widget endpoint")

        LOG.info("==== Validate the data in the widget of Bullseye Code Coverage Report with drilldown ====")
        key_value = create_bullseye_code_coverage_object.bullseye_code_coverage(keys=True)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_bullseye_code_coverage_object.bullseye_code_coverage_drilldown(key=eachRecord['key'])
            assert drilldown, "No Data In Drilldown"
