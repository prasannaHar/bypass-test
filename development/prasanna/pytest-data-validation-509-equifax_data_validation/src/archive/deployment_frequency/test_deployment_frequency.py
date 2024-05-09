## this widget is deprecated -- hence moving these tcs to archive

import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestDeploymentFrequency:
    @pytest.mark.run(order=1)
    def test_deployment_frequency_001(self,create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Deployment Frequency"""

        LOG.info("==== create widget with available filter ====")
        deployment_frequency = create_widget_helper_object.create_deployment_frequency_widget(
            integration_id=get_integration_obj)
        assert deployment_frequency, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_deployment_frequency(),
                                                       deployment_frequency,
                                                       "Schema validation failed for Deployment Frequency  endpoint")

    @pytest.mark.run(order=2)
    def test_deployment_frequency_002(self, create_generic_object,
                                      create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Deployment Frequency"""

        LOG.info("==== create widget with available filter ====")
        LOG.info("==== Filters : PR merged In last 4 months  ====")
        gt, lt = create_generic_object.get_epoc_time(value=4)
        pr_merged_in = {"$gt": gt, "$lt": lt}
        filters = {"pr_merged_at": pr_merged_in}
        deployment_frequency = create_widget_helper_object.create_deployment_frequency_widget(
            integration_id=get_integration_obj, var_filters=filters)
        assert deployment_frequency, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_deployment_frequency(),
                                                       deployment_frequency,
                                                       "Schema validation failed for Deployment Frequency  endpoint")
