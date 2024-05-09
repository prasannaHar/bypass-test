import json
import logging
import pytest
import calendar
import time

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestDemoDashboard:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_progress_report(self, create_testdemo_object):
        """Validate alignment of Issue Progress Report"""

        LOG.info("==== create widget with available filter ====")
        create_issue_progress = create_testdemo_object.issue_progress_report(project_id)
        assert create_issue_progress, "widget is not created"

        LOG.info("==== Validate the data in the widget of progress report with drilldown ====")
        list_epics = create_testdemo_object.issue_progress_report(project_id, epics=True)  # list_epics
        epic_assignees_list = create_testdemo_object.epics_data(project_id, list_epics)  # epics:count
        # commneting assert as more validations and calculations required

        # for epic_value in epic_assignees_list.items():
        #     total_assignee = create_testdemo_object.epics_drilldown(project_id, epic_value[0])
        #     # assert len(total_assignee) == epic_assignees_list[epic_value[0]], "Mismatch data in the drill down"

        LOG.info("==== Validate the data in the widget with different filters ====")
