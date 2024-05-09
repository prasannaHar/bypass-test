import json
import logging
from copy import deepcopy

import pytest
import urllib3
import pandas as pd

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.jira_report_helper import TestJiraReportHelper
from src.testcases.issues_report.issues_report_testcase_helper import TestIssuesReportHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesSingleStatHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.jira_helper = TestJiraReportHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.integration_id = self.generic.integration_ids_basedon_workspace()
        self.issues_report = TestIssuesReportHelper(generic_helper)

    def issues_single_stat(self, time_range_value, across="issue_created", var_filters=False, story=False):
        """ create issues single report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {across + "_at": time_range_value, "integration_ids": self.integration_id}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issues_report(ou_ids=org_id, filters=filters,
                                                across=across, story=story)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def get_issues_response(self, payload):
        url = self.generic.connection['base_url'] + self.api_info['jira_tickets_report']
        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def issues_payload_generation(self, filter, ou_id, sprint_field, across):

        payload = {"filter": filter,
                   "ou_ids": [ou_id],
                   "ou_user_filter_designation": {"sprint": [sprint_field]},
                   "across": across
                   }

        LOG.info("payload-----{}".format(json.dumps(payload)))

        return payload

    def widget_creation_and_comparing__issues_single_stat_vs_issues_report(self, filter, filter2, int_ids, gt, lt,
                                                                           custom_values,
                                                                           exclude, ou_id, datetime_filters, sprint,
                                                                           sprint_field,
                                                                           across=None):
        flag_list = []
        try:
            issue_single_stat_filter = self.jira_helper.jira_filter_creation(filter_type=filter, filter2=filter2,
                                                                             integration_id=int_ids,
                                                                             gt=gt, lt=lt,
                                                                             custom_values=custom_values,
                                                                             exclude=exclude,
                                                                             datetime_filters=datetime_filters,
                                                                             sprint=sprint)

            payload = self.issues_payload_generation(filter=issue_single_stat_filter, ou_id=ou_id,
                                                     sprint_field=sprint_field,
                                                     across=across)

            LOG.info("Widget Payload------{}".format(json.dumps(payload)))
            res = self.get_issues_response(payload)
            issues_single_stat_df = pd.DataFrame()
            if len(res['records']) == 0:
                pytest.skip("There are no records returned")
            if len(res['records']) != 0:
                issues_single_stat_df = pd.json_normalize(res['records'])

            issues_report_payload = deepcopy(payload)
            issues_report_payload['filter']['metric'] = "ticket"
            issues_report_payload['across'] = "assignee"

            issues_report_response = self.get_issues_response(issues_report_payload)
            issues_report_df = pd.json_normalize(issues_report_response['records'])
            total_count_issues_single_stat = issues_single_stat_df['total_tickets'].sum()
            total_count_issues_report = issues_report_df['total_tickets'].sum()
            if total_count_issues_single_stat != total_count_issues_report:
                flag_list.append(f"total_count_issues_report--{total_count_issues_report} "
                                 f"and total_count_issues_single_stat dont match---{total_count_issues_single_stat}")


        except Exception as ex:
            LOG.info(f"Exception caught in widget_creation_issues_single_stat---{ex}")
            flag_list.append("exception occured---{}".format(ex))

        return flag_list
