
import logging
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.datetime_reusable_functions import DateTimeReusable
import pandas as pd
import numpy as np
from copy import deepcopy

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class ADOCustomFieldsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.datetimeutil = DateTimeReusable()
        self.ado_default_timerange = self.env_info['ado_default_time_range']

    def ado_retrive_custom_filter(self, custom_filter_type, ou_id):
        """this logic will helps to find out the custom filter which has max number of total tickets"""
        custom_fields = self.generic.get_integration_custom_details_ado(ou_id=ou_id)
        required_fields = [string for string in custom_fields if string.startswith(custom_filter_type)]
        max_total_tickets = 0
        freq_metric = "none"
        for each_field in required_fields:
            if "Date" not in each_field:
                total_tickets = self.generic.get_ado_filter_options(
                                            arg_filter_type=each_field, ou_id=ou_id, 
                                            total_tickets=True)
                if max_total_tickets < total_tickets:
                    max_total_tickets = total_tickets
                    freq_metric = each_field        
        return freq_metric


    def ado_dynamic_filter_maker(self, filter_name, ou_id, time_range_filter=None, timerange_factor=1):
        """  this function helps in retrieving the given filter key value pair """
        try:
            if time_range_filter:
                ## time range filter
                gt, lt = self.generic.get_epoc_utc(
                            value_and_type=self.ado_default_timerange,timerange_factor=timerange_factor)
                return filter_name, {"$gt": gt, '$lt': lt}
            rev_filter_type = True
            if filter_name in ["Custom", "Microsoft"]:
                filter_name = self.ado_retrive_custom_filter(
                                        custom_filter_type=filter_name, ou_id=ou_id[0])
                rev_filter_type = False
            ## dynamic filter
            if filter_name != "none":
                filter_vals = self.generic.get_ado_filter_options(
                                            arg_filter_type=filter_name, ou_id=ou_id[0], 
                                            rev_filter_type=rev_filter_type)
                if filter_name in ["teams", "code_area"]:
                    return "workitem_attributes", {filter_name:filter_vals}
                elif ((filter_name.startswith("Custom")) or (filter_name.startswith("Microsoft"))):
                    return "workitem_custom_fields", {filter_name:filter_vals}
                else:
                    return filter_name, filter_vals
            if filter_name == "none":
                pytest.skip("selected filter criteria doesn't exists")
            return None
        except Exception as ex:
            LOG.info("exeception occured in ado_dynamic_filter_maker {}".format(ex))
            return None

    def ado_drilldown_widget_specific_filter_maker(self, across, drilldown_key_val, 
                                interval, ou_id, time_range_filter, sprint_report=None):
        across_vs_drilldown_req_key_name_mapping = self.api_info["ado_im_filter_names_mapping"]
        ## widget req_key_val argument specific filter name identification mapping 
        req_key_name = across
        if across in across_vs_drilldown_req_key_name_mapping.keys():
            req_key_name = across_vs_drilldown_req_key_name_mapping[across]
        if sprint_report:
            req_key_name = "sprint_report"
        if across in ["bi_week"]:
            interval = deepcopy(across)
            across = "completed_at"
        ## widget specific filter
        if(across in ["reviewer_count", "approver_count"]):
            temp_filter_name =  "num_" + (across.split("_"))[0] + "s"
            return temp_filter_name, {"$gte": drilldown_key_val, "$lte": drilldown_key_val} 
        elif across in ["trend"]:
            return "ingested_at", int(drilldown_key_val)
        elif across in ["teams", "code_area"]:
            return "workitem_attributes",  {across:[drilldown_key_val]}
        elif across in ["story_points"]:
            return "workitem_story_points", {"$gte": drilldown_key_val, "$lte": drilldown_key_val}
        elif(across in ["workitem_created_at", "workitem_updated_at", "workitem_resolved_at", "completed_at"]):
            ## widget selected time range filter
            timerange_key, timerange_val = self.ado_dynamic_filter_maker(
                                                    filter_name=time_range_filter, 
                                                    ou_id=ou_id,time_range_filter=True)
            gt_widget,lt_widget = timerange_val["$gt"], timerange_val["$lt"]
            gt, lt = "", ""
            if interval == "week": 
                gt, lt = self.datetimeutil.get_week_range(drilldown_key_val)
            elif interval == "bi_week":
                gt, lt = self.datetimeutil.get_biweek_range(drilldown_key_val)                
            elif interval == "month":
                gt, lt = self.datetimeutil.get_month_range(drilldown_key_val)
            elif interval == "quarter":
                gt, lt = self.datetimeutil.get_quarter_range(drilldown_key_val)
            if not (gt_widget <= gt <= lt_widget):
                gt = gt_widget
            if not (gt_widget <= lt <= lt_widget):
                lt = lt_widget
            return across, {"$lt": lt, "$gt": gt}
        else:
            return req_key_name, [drilldown_key_val]
