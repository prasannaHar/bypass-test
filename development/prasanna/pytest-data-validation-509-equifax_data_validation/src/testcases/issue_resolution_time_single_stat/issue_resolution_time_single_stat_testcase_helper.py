import json
import logging
import pytest
import pandas as pd
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.jira_report_helper import TestJiraReportHelper
from src.testcases.issues_report.issues_report_testcase_helper import TestIssuesReportHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueResolutionTimeSingleStatHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.jira_helper = TestJiraReportHelper(generic_helper)
        self.issue_helper = TestIssuesReportHelper(generic_helper)

    def issue_resolution_time_single_stat(self, time_range_value,
                                          across="issue_created", agg_type="average", var_filters=False, ):
        """ create issue  issue_resolution_time_single_stat"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        filters = {across + "_at": time_range_value, "agg_type": agg_type,
                   "integration_ids": integration_id}

        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_resolution_time_report(ou_ids=org_id, filters=filters, across=across)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def get_resolution_time_report_response(self, payload):
        url = self.generic.connection['base_url'] + self.api_info['resolution_time_report']
        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def payload_resolution_single_stat(self, filter, agg_type, ou_id, sprint_field, across):
        if agg_type:
            filter.update({"agg_type": agg_type})
        payload = {
            "filter": filter,
            "ou_ids": [ou_id],
            "ou_user_filter_designation": {"sprint": [sprint_field]},
            "across": across
        }

        return payload

    def create_resolution_single_stat_widget(self, filter, filter2, across, exclude, sprint, agg_type, datetime_filter,
                                             gt, lt, custom_values, int_ids, ou_id, sprint_field):
        flag = []
        try:
            filter_single_stat = self.jira_helper.jira_filter_creation(filter_type=filter, filter2=filter2,
                                                                       integration_id=int_ids,
                                                                       gt=gt, lt=lt,
                                                                       custom_values=custom_values, sort_x_axis=None,
                                                                       exclude=exclude, metric=None,
                                                                       datetime_filters=datetime_filter,
                                                                       sprint=sprint)

            payload = self.payload_resolution_single_stat(filter=filter_single_stat, agg_type=agg_type, ou_id=ou_id,
                                                          sprint_field=sprint_field,
                                                          across=across)

            single_stat_resp = self.get_resolution_time_report_response(payload)
            df1 = pd.json_normalize(single_stat_resp['records'], max_level=1)
            df1 = df1['total_tickets']
            single_stat_count = df1.sum()

            payload_issues = self.issue_helper.issue_report_payload_generation(filter=filter_single_stat, ou_id=ou_id,
                                                                               sprint_field=sprint_field, across=across,
                                                                               across_limit=20, stacks=None)
            del payload_issues['filter']['agg_type']

            issue_report_res = self.issue_helper.get_issues_report_response(payload_issues)
            df2 = pd.json_normalize(issue_report_res['records'], max_level=1)
            df2 = df2['total_tickets']
            issues_count = df2.sum()
            if issues_count != single_stat_count:
                flag.append({"issues_count": issues_count, "single_stat_count": single_stat_count,
                             "payload_issues": json.dumps(payload_issues),
                             "single_stat_payload": json.dumps(payload)})

        except Exception as ex:
            LOG.info(f"Exception in create_resolution_single_stat_widget---{ex}")
            flag.append({"exception": ex})

        return flag
