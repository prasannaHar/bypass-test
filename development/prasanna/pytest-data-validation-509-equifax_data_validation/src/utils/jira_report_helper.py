import logging
import os
import pandas as pd
from pandas.io.json import json_normalize

from src.utils.generate_ou_payloads import *

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.api_reusable_functions import ApiReusableFunctions
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestJiraReportHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.api_reusable_funct = ApiReusableFunctions(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.widgetresuable = WidgetReusable(generic_helper)

    def jira_filter_creation(self, filter_type, filter2, integration_id, gt, lt, custom_values,
                             sort_x_axis="label_high-low",
                             exclude=None, metric="ticket", datetime_filters=None, sprint=None, report_type=None,
                             ba=None, dashboard_filter=None):
        """creating jira/azure filter object for payload with different filter_type"""
        # jira
        """{"filter":{"metric":"ticket","sort_xaxis":"value_high-low",
        "visualization":"bar_chart","integration_ids":["229","231"]},
        """
        # breakpoint()
        filter = {"integration_ids": integration_id, "visualization": "bar_chart"}

        if report_type == "resolution_time_report":
            del filter["visualization"]

        if sprint:
            if sprint == 'ACTIVE':
                filter.update({"sprint_states": ["ACTIVE"]})
            else:
                filter.update({"last_sprint": True})

        if metric:
            filter.update({"metric": metric})

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
            # breakpoint()
            filter_update = self.generic.max_3_values_filter_type(exclude, int_ids=integration_id, exclude=True)
            if len(filter_update) == 0:
                pass
            else:
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(exclude)
                if ba:
                    if "custom" in exclude:
                        filter.update({"exclude": {"custom_fields": filter_update[0][1]}})
                    else:
                        filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})

                else:
                    filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})

        if dashboard_filter:
            # breakpoint()
            filter_update = self.generic.max_3_values_filter_type(dashboard_filter, int_ids=integration_id,
                                                                  exclude=True)
            if len(filter_update) == 0:
                pass
            else:
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(dashboard_filter)
                if ba:
                    if "custom" in dashboard_filter:
                        filter.update({"or": {"custom_fields": filter_update[0][1]}})
                    else:
                        filter.update({"or": {filter_update[0][0]: filter_update[0][1]}})

                else:
                    filter.update({"or": {filter_update[0][0]: filter_update[0][1]}})

        return filter


