import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestLeadTimeSSReport:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_lead_time_single_stat_001(self, create_generic_object, create_lead_time_single_stat_object):
        """Validate alignment of issue lead time single stat"""

        LOG.info("==== create widget with available filter ====")
        create_issue_resolution_time_single_stat = create_lead_time_single_stat_object.lead_time_single_stat()
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_lead_time_single_stat_002(self, create_generic_object, create_lead_time_single_stat_object):
        """Validate alignment of issue lead time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=12)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolved = {"$gt": gt, "$lt": lt}
        filters = {"jira_issue_resolved_at": issue_resolved}
        create_issue_resolution_time_single_stat = create_lead_time_single_stat_object.lead_time_single_stat(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_lead_time_single_stat_003(self, create_generic_object, create_lead_time_single_stat_object):
        """Validate alignment of issue lead time single stat"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=12)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolved = {"$gt": gt, "$lt": lt}
        project_names = create_generic_object.env["project_names"]
        filters = {"jira_issue_resolved_at": issue_resolved, "jira_projects": project_names}
        create_issue_resolution_time_single_stat = create_lead_time_single_stat_object.lead_time_single_stat(
            var_filters=filters)
        LOG.info("==== Validate Widget Schema ====")
        assert create_issue_resolution_time_single_stat, "widget is not created"
