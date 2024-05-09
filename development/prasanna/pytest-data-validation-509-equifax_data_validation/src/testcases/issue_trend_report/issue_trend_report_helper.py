import inspect
import json
import logging
import random
from copy import deepcopy
import pandas as pd
import pytest

import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.api_reusable_functions import ApiReusableFunctions
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesTrendReportHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.api_reusable_funct = ApiReusableFunctions(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.widgetresuable = WidgetReusable(generic_helper)

    def filter_creation(self, filter_type, filter2, integration_id, gt, lt, custom_values, sort_x_axis,
                        exclude=None, datetime_filters=None, sprint=None):
        """{"filter":{"assignees":["1fd81edb-f109-47f3-9ad5-13c7c8561e27","52f7faef-bf59-4811-a8db-e34a1e1ef607"],
        "sort_xaxis":"default_old-latest","integration_ids":["71","76"]},"across":"trend","ou_ids":["733"],
        "ou_user_filter_designation":{"sprint":["customfield_10103"]}}"""
        filter = {"integration_ids": integration_id, "visualization": "bar_chart", "sort_x_axis": sort_x_axis}

        if sprint:
            if sprint == 'ACTIVE':
                filter.update({"sprint_states": ["ACTIVE"]})
            else:
                filter.update({"last_sprint": True})

        if datetime_filters:
            filter.update({datetime_filters: {"$gt": gt, "$lt": lt}})

        if sort_x_axis:
            filter.update({"sort_xaxis": sort_x_axis})

        if filter_type:
            if "custom" in filter_type:
                filter.update({"custom_fields": {filter_type: custom_values}})

            elif filter_type == "projects":
                projects = self.env_info['project_names']
                if len(projects) != 0:
                    filter.update({"projects": projects})
                else:
                    filter_update = self.generic.max_3_values_filter_type(filter_type, int_ids=integration_id)
                    filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(filter_type)
                    filter.update({filter_update[0][0]: filter_update[0][1]})
            else:
                filter_update = self.generic.max_3_values_filter_type(filter_type, int_ids=integration_id)
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(filter_type)
                filter.update({filter_update[0][0]: filter_update[0][1]})

        if filter2:
            projects = self.env_info['project_names']
            if len(projects) == 0:
                filter_update = self.generic.max_3_values_filter_type(filter2, int_ids=integration_id)
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(filter2)
                filter.update({filter_update[0][0]: filter_update[0][1]})
            else:
                filter.update({"projects": projects})

        if exclude:
            filter_update = self.generic.max_3_values_filter_type(exclude, int_ids=integration_id, exclude=True)
            if len(filter_update) == 0:
                pass
            else:
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(exclude)
                filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})

        return filter

    def issue_trend_report_payload_generation(self, filter, ou_id, sprint_field, across):
        """"filter":{"assignees":["1fd81edb-f109-47f3-9ad5-13c7c8561e27","52f7faef-bf59-4811-a8db-e34a1e1ef607"],
        "sort_xaxis":"default_old-latest","integration_ids":["71","76"]},"across":"trend","ou_ids":["733"],
        "ou_user_filter_designation":{"sprint":["customfield_10103"]}}"""
        # breakpoint()

        payload = {"filter": filter,
                   "ou_ids": [ou_id],
                   "ou_user_filter_designation": {"sprint": [sprint_field]},
                   "across": across
                   }

        return payload

    def get_issues_trend_report_response(self, payload, report):
        try:
            if report == "resolution_time_report":
                end_point = self.api_info['resolution_time_report']
            elif report == "jira_response_time":
                end_point = self.api_info['jira_response_time']
            elif report == "jira_tickets_report":
                end_point = self.api_info['jira_tickets_report']
            elif report == "bounce_widget_api_url":
                end_point = self.api_info['bounce_widget_api_url']
            elif report == "jira_hops_report":
                end_point = self.api_info['jira_hops_report']

            url = self.generic.connection['base_url'] + end_point
            response = self.generic.execute_api_call(url, "post", data=payload)
            return response
        except Exception as ex:
            LOG.info("Exception caught is---{}".format(ex))
            return None

    def widget_creation_issues_trend_report(self, filter, report, ou_id, sprint_field, across=None):

        try:

            payload = self.issue_trend_report_payload_generation(filter=filter, ou_id=ou_id, sprint_field=sprint_field,
                                                                 across=across)

            LOG.info("Widget Payload------{}".format(json.dumps(payload)))
            # breakpoint()
            res = self.get_issues_trend_report_response(payload, report)
            # LOG.info("widget_response-----{}".format(json.dumps(res)))

            if len(res['records']) == 0:
                pytest.skip("Widget is not returning any data---{}".format(len(res['records'])))
            if len(res['records']) != 0:
                df = pd.json_normalize(res['records'])
                return df

        except Exception as ex:
            LOG.info("Exceptiom occured in widget_creation_issues_trend_report-----{}".format(ex))
            df = pd.DataFrame()
            return df
