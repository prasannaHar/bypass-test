import logging
from copy import deepcopy

import pytest
import pandas as pd

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.jira_report_helper import TestJiraReportHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueHygieneHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.jira_helper = TestJiraReportHelper(generic_helper)

    def widget_payload(self, filter_type, filter2, integration_id, gt, lt, custom_values,
                       exclude, datetime_filters, sprint, hygiene_types, ou_id, sprint_field):
        """{"across":"project","filter":{"hideScore":false,"integration_ids":["71"],
        "hygiene_types":["INACTIVE_ASSIGNEES"]},"ou_ids":["2327"],
        "ou_user_filter_designation":{"sprint":["customfield_10103"]}}"""

        filter = self.jira_helper.jira_filter_creation(filter_type=filter_type, filter2=filter2,
                                                       integration_id=integration_id, gt=gt,
                                                       lt=lt, custom_values=custom_values,
                                                       exclude=exclude, datetime_filters=datetime_filters,
                                                       sprint=sprint, sort_x_axis=None)

        filter["hygiene_types"] = [hygiene_types]
        del filter["visualization"]
        payload = {"across": "project", "filter": filter, "ou_ids": [ou_id],
                   "ou_user_filter_designation": {"sprint": [sprint_field]}}

        return filter, payload

    def widget_response(self, payload):
        ticket_report_url = self.generic.connection['base_url'] + self.api_info["jira_tickets_report"]
        response = self.generic.execute_api_call(ticket_report_url, "post", data=payload)
        return response

    def drill_down_payload(self, widget_payload, hygiene_types, page=None, page_size=100):
        """{"page": 0, "page_size": 10, "sort": [{"id": "bounces", "desc": true}],
         "filter": {"hideScore": false, "integration_ids": ["71"], "hygiene_types": ["IDLE"]}, "across": "project",
         "ou_ids": ["2327"], "ou_user_filter_designation": {"sprint": ["customfield_10103"]},
         "ou_exclusions": ["hygiene_types"], "widget_id": "3b0ded50-1984-11ee-8a3e-b3531f89186c"}"""

        drilldown_payload = deepcopy(widget_payload)
        drilldown_payload['sort'] = [{"id": "bounces", "desc": True}]
        if page:
            drilldown_payload['page']: page
            drilldown_payload['page_size']: page_size
        drilldown_payload['filter']['hygiene_types'] = [hygiene_types]
        drilldown_payload["ou_exclusions"] = ["hygiene_types"]

        return drilldown_payload

    def drilldown_response(self, payload):
        drilldown_url = self.generic.connection['base_url'] + self.api_info["drill_down_api_url"]
        drilldown_res = self.generic.execute_api_call(drilldown_url, "post", data=payload)
        return drilldown_res

    def check_if_all_values_equal(self, df, column_name, value):
        """
        Checks if all values in a column are equal to a given value in a DataFrame.

        Args:
          df: The DataFrame to check.
          column_name: The name of the column to check.
          value: The value to check for.

        Returns:
          True if all values in the column are equal to the given value, False otherwise.
        """

        all_values_equal = True
        values = []
        for value_in_column in df[column_name]:
            if value_in_column != value:
                values.append({"value_in_column": value_in_column, "value": value})
                all_values_equal = False
                break

        return all_values_equal, values

    def create_widget_and_verify_drilldown(self, filter_type, filter2, integration_id, gt, lt, custom_values,
                                           exclude, datetime_filters, sprint, hygiene_types, ou_id, sprint_field):
        flag_list = []

        try:
            filter, widget_payload = self.widget_payload(filter_type, filter2, integration_id, gt, lt, custom_values,
                                                         exclude, datetime_filters, sprint, hygiene_types, ou_id,
                                                         sprint_field)

            response = self.widget_response(widget_payload)
            if response['count'] != 0:
                widget_response_df = pd.json_normalize(response['records'], max_level=2)
                widget_total_ticket_count = widget_response_df['total_tickets'].sum()

            else:
                widget_total_ticket_count = 0
            drilldown_payload = self.drill_down_payload(widget_payload, hygiene_types, page=0)
            drilldown_res = self.drilldown_response(payload=drilldown_payload)
            drilldown_df = self.widget.generate_paginated_drilldown_data(
                payload=self.drill_down_payload(widget_payload, hygiene_types),
                url=(self.generic.connection['base_url'] + self.api_info["drill_down_api_url"]))

            total_records = drilldown_res["_metadata"]["total_count"]
            hygiene_dict = {"NO_DUE_DATE": "issue_due_at", "NO_ASSIGNEE": "assignee_id",
                            "POOR_DESCRIPTION": "desc_size",
                            "NO_COMPONENTS": "component_list"}
            if len(drilldown_df) != 0:
                if hygiene_types == "NO_COMPONENTS":
                    value = []
                else:
                    value = 0
                if hygiene_types in list(hygiene_dict.keys()):
                    all_values_equal, values = self.check_if_all_values_equal(df=drilldown_df,
                                                                              column_name=hygiene_dict[hygiene_types],
                                                                              value=value)
                    if all_values_equal is False:
                        flag_list.append(f"{hygiene_types[hygiene_types]} is not having all zeros--{values}")

            LOG.info(f"widget_total_ticket_count--{widget_total_ticket_count},drilldown total_records--{total_records}")
            if widget_total_ticket_count != total_records:
                flag_list.append(
                    f"Count not matching widget_total_ticket_count--{widget_total_ticket_count},drilldown total_records--{total_records}")

        except Exception as ex:
            LOG.info(f"Exception found in create_widget_and_verify_drilldown in hygenie report section---{ex}")
            flag_list.append({"exception": ex})


        return flag_list, widget_payload
