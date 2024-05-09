import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestSonarqubeCodeComplexity:
    @pytest.mark.run(order=1)
    @pytest.mark.need_maintenance
    def test_sonarqube_code_complexity_001(self, create_sonarqube_code_complexity_object,
                                           create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of sonarQube code complexity """

        LOG.info("==== create widget with available filter ====")
        sonarqube_code_complexity = create_sonarqube_code_complexity_object.sonarqube_code_complexity_report(
            integration_id=get_integration_obj)

        assert sonarqube_code_complexity, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_widget_sonarqube_code_complexity(),
            sonarqube_code_complexity,
            "Schema validation failed for scm commits single stat  widget endpoint")
