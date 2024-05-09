import logging
import random

import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestScmReviewCollaborationReport:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_review_collaboration_report_001(self, create_scm_review_collaboration_object, get_integration_obj):
        """Validate alignment of scm review collaboration report"""

        LOG.info("==== create widget with available filter ====")
        create_scm_rework_report = create_scm_review_collaboration_object.scm_review_collaboration_report(
            integration_id=get_integration_obj)

        assert create_scm_rework_report, "widget is not created"

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_review_collaboration_report_002(self, create_scm_review_collaboration_object, get_integration_obj):
        """Validate alignment of scm review collaboration report"""

        LOG.info("==== create widget with available filter ====")
        create_scm_rework_report = create_scm_review_collaboration_object.scm_review_collaboration_report(
            integration_id=get_integration_obj)

        assert create_scm_rework_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of SCM review collaboration report with drilldown ====")
        key_value = create_scm_review_collaboration_object.scm_review_collaboration_report(
            integration_id=get_integration_obj, keys=True)
        LOG.info("key and total tickets of widget : {}".format(key_value))

        for key, count in key_value.items():
            drilldown = create_scm_review_collaboration_object.scm_review_collaboration_drilldown(
                integration_id=get_integration_obj,
                key=key)

            assert count == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + key

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_scm_review_collaboration_report_003(self, create_scm_review_collaboration_object, get_integration_obj):
        """Validate alignment of scm review collaboration report"""

        LOG.info("==== create widget with available filter ====")
        filters = {"missing_fields": {"pr_merged": False}}
        create_scm_rework_report = create_scm_review_collaboration_object.scm_review_collaboration_report(
            integration_id=get_integration_obj, var_filters=filters)

        assert create_scm_rework_report, "widget is not created"

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_scm_review_collaboration_report_004(self, create_scm_review_collaboration_object,
                                                 get_integration_obj):
        """Validate alignment of scm review collaboration report"""

        LOG.info("==== create widget with available filter ====")
        filters = {"missing_fields": {"pr_merged": False}}
        create_scm_rework_report = create_scm_review_collaboration_object.scm_review_collaboration_report(
            integration_id=get_integration_obj,var_filters=filters)

        assert create_scm_rework_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of SCM review collaboration report with drilldown ====")
        key_value = create_scm_review_collaboration_object.scm_review_collaboration_report(
            integration_id=get_integration_obj, keys=True,var_filters=filters)
        LOG.info("key and total tickets of widget : {}".format(key_value))

        for key, count in key_value.items():
            drilldown = create_scm_review_collaboration_object.scm_review_collaboration_drilldown(
                integration_id=get_integration_obj,var_filters=filters,
                key=key)

            assert count == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + key
