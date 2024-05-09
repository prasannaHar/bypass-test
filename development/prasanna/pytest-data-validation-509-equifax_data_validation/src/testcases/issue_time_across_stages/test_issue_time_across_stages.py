import logging
import random

import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestTimeAcrossStages:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_time_across_stages_001(self, create_issue_time_across_stages_object, create_generic_object,
                                           create_widget_helper_object, widget_schema_validation):

        """Validate alignment of issue time across stages"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        resolved_at = {"$gt": gt, "$lt": lt}
        create_issue_time_across_stages = create_issue_time_across_stages_object.issue_time_across_stages(
            resolved_at=resolved_at,
        )
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_time_across_stages, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_issue_time_across_stages(),
                                                       create_issue_time_across_stages,
                                                       "Schema validation failed for create issue time across stages  widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_time_across_stages_002(self, create_issue_time_across_stages_object, create_generic_object,
                                           create_widget_helper_object, widget_schema_validation):

        """Validate alignment of issue time across stages"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        resolved_at = {"$gt": gt, "$lt": lt}
        create_issue_time_across_stages = create_issue_time_across_stages_object.issue_time_across_stages(
            resolved_at=resolved_at)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_time_across_stages, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_widget_issue_time_across_stages(),
            create_issue_time_across_stages,
            "Schema validation failed for create issue time across stages  widget endpoint")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_issue_time_across_stages_003(self, create_issue_time_across_stages_object, create_generic_object,
                                          create_widget_helper_object, widget_schema_validation, get_integration_obj):

        """Validate alignment of issue time across stages"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        resolved_at = {"$gt": gt, "$lt": lt}

        get_filter_response_status = create_generic_object.get_filter_options(arg_filter_type=["status"],
                                                                              arg_integration_ids=get_integration_obj)
        temp_records_status = [get_filter_response_status['records'][0]['status']]
        status_value = []
        for eachstatus in temp_records_status[0]:
            status_value.append(eachstatus['key'])

        filters = {"exclude": {"stages": random.sample(status_value, min(3, len(status_value)))},
                   "partial_match": {"sprint_report": {"$begins": "FE"}},
                   "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}}
        create_issue_time_across_stages = create_issue_time_across_stages_object.issue_time_across_stages(
            resolved_at=resolved_at, var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_time_across_stages, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_widget_issue_time_across_stages(),
            create_issue_time_across_stages,
            "Schema validation failed for create issue time across stages  widget endpoint")


    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_issue_time_across_stages_004(self, create_issue_time_across_stages_object, create_generic_object,
                                          create_widget_helper_object, widget_schema_validation, get_integration_obj):

        """Validate alignment of issue time across stages"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=4)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        resolved_at = {"$gt": gt, "$lt": lt}

        get_filter_response_status = create_generic_object.get_filter_options(arg_filter_type=["status"],
                                                                              arg_integration_ids=get_integration_obj)
        temp_records_status = [get_filter_response_status['records'][0]['status']]
        status_value = []
        for eachstatus in temp_records_status[0]:
            status_value.append(eachstatus['key'])

        filters = {"exclude": {"stages": random.sample(status_value, min(3, len(status_value)))},
                   "partial_match": {"sprint_report": {"$begins": "FE"}},
                   "or": {"statuses": ["BACKLOG", "BLOCKED", "DONE"]}}
        create_issue_time_across_stages = create_issue_time_across_stages_object.issue_time_across_stages(
            resolved_at=resolved_at, var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_time_across_stages, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(
            widget_schema_validation.create_widget_issue_time_across_stages(),
            create_issue_time_across_stages,
            "Schema validation failed for create issue time across stages  widget endpoint")

        LOG.info("==== Validate the data in the widget of issue backlog report with drilldown ====")
        key_value = create_issue_time_across_stages_object.issue_time_across_stages(resolved_at=resolved_at,
                                                                                    var_filters=filters, keys=True)

        for eachRecord in key_value:
            drilldown = create_issue_time_across_stages_object.issue_time_across_stages_drilldown(var_filters=filters,
                                                                                                  key=eachRecord[
                                                                                                      "stage"],
                                                                                                  resolved_at=resolved_at)
            assert eachRecord["total_tickets"] == drilldown['_metadata'][
                "total_count"], "Mismatch data in the drill down: " + eachRecord["stage"]
        LOG.info("==== Validate the data in the widget with different filters ====")