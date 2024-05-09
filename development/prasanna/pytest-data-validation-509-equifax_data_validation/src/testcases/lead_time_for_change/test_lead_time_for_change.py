import logging
import pytest
from src.lib.generic_helper.generic_helper import *

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestLeadTimeForChange:
    @pytest.mark.run(order=1)
    def test_lead_time_for_changes_001(self,create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Lead Time For Changes"""

        LOG.info("==== create widget with available filter ====")
        create_lead_time_for_change = create_widget_helper_object.create_lead_time_for_changes_widget(
            integration_id=get_integration_obj)
        assert create_lead_time_for_change, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_lead_time_for_change(),
                                                       create_lead_time_for_change,
                                                       "Schema validation failed for  Lead Time For Changes endpoint")

    @pytest.mark.run(order=2)
    def test_lead_time_for_changes_002(self, create_generic_object,
                                       create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of Lead Time For Changes"""

        LOG.info("==== create widget with available filter ====")
        LOG.info("==== Filters : PR merged In last 4 months  ====")
        gt, lt = create_generic_object.get_epoc_time(value=4)
        pr_merged_in = {"$gt": gt, "$lt": lt}
        filters = {"pr_merged_at": pr_merged_in}
        create_lead_time_for_change = create_widget_helper_object.create_lead_time_for_changes_widget(
            integration_id=get_integration_obj, var_filters=filters)
        assert create_lead_time_for_change, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_lead_time_for_change(),
                                                       create_lead_time_for_change,
                                                       "Schema validation failed for  Lead Time For Changes endpoint")
