import logging
import pandas as pd
import numpy as np
import datetime

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.datetime_reusable_functions import DateTimeReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class ADOSprintReportsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.datetimeutil = DateTimeReusable()
        self.ado_default_timerange = self.env_info['ado_default_time_range']
        self.data_time = datetime.datetime.now()
    
    def ado_sprint_metrics_trend_payload_generate(self, required_test_params, 
                    integration_ids, ou_id, ado_custom_field_helper_object=None,
                    time_range_filter2=False):
        across,interval,time_range_filter,filters,filter2 = required_test_params
        ## default filters
        dyn_filters = {"include_workitem_ids":True,"metric":["creep_done_points",
                            "commit_done_points","commit_not_done_points","creep_not_done_points"],
                            "integration_ids":integration_ids}
        ## dynamic filter
        filters_key, filters_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=filters, ou_id=ou_id)
        dyn_filters[filters_key] = filters_val
        ## project filter
        filters2_key, filters2_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=filter2, ou_id=ou_id)
        dyn_filters[filters2_key] = filters2_val
        ## time range filter
        timerange_key, timerange_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=time_range_filter, ou_id=ou_id,
                                                    time_range_filter=True, timerange_factor=2)
        dyn_filters[timerange_key] = timerange_val
        if time_range_filter2:
            dyn_filters["started_at"] = timerange_val
        ## payload making
        payload = {"filter":dyn_filters,"across":across,"across_limit":20,"ou_ids":ou_id,
                   "ou_user_filter_designation":{"sprint":["sprint_report"]}}
        if interval != "none":
            payload["interval"] = interval
        return payload

    def ado_sprint_metrics_trend_payload_generate_drilldown(self, required_test_params,  
                                integration_ids, ou_id, req_key_val=None, pagination_flag=None,
                                ado_custom_field_helper_object=None, sprint_report=True,
                                time_range_filter2=False):
        across,interval,time_range_filter,filters,filter2 = required_test_params
        ## default filters
        dyn_filters = {"include_workitem_ids":True,
                "integration_ids":integration_ids,"include_issue_keys":True}
        ## dynamic filter
        filters_key, filters_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=filters, ou_id=ou_id)
        dyn_filters[filters_key] = filters_val
        ## project filter
        filters2_key, filters2_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=filter2, ou_id=ou_id)
        dyn_filters[filters2_key] = filters2_val
        ## time range filter
        timerange_key, timerange_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=time_range_filter, ou_id=ou_id,
                                                    time_range_filter=True, timerange_factor=2)
        dyn_filters[timerange_key] = timerange_val
        if time_range_filter2:
            dyn_filters["started_at"] = timerange_val
        ## widget specific filter
        if across not in ["bi_week"]:
            widget_key, widget_key_val = ado_custom_field_helper_object.ado_drilldown_widget_specific_filter_maker(
                                                        across=across, drilldown_key_val=req_key_val, interval=interval, 
                                                        ou_id=ou_id, time_range_filter=time_range_filter, 
                                                        sprint_report=sprint_report)
            dyn_filters[widget_key] = widget_key_val
        ## payload making        
        payload = {"filter":dyn_filters, "ou_ids":ou_id,
                   "ou_user_filter_designation":{"sprint":["sprint_report"]}}
        ## ou exclusions
        payload["ou_exclusions"] = [across]
        ##limiting the drill-down data based on page size
        if pagination_flag:
            payload["page"]=0
            payload["page_size"]=10

        return payload

    def ado_sprint_report_keys_data_validator(self, drilldown_response_data):
        data_validation_flg = True
        try:
            workitems_keys_list = []
            required_keys = ['commit_delivered_keys','committed_keys',
                             'creep_keys','delivered_creep_keys','delivered_keys']
            for eachkey in required_keys:
                if eachkey in drilldown_response_data :
                    workitems_keys_list = workitems_keys_list + drilldown_response_data[eachkey]
            if len(list(set(workitems_keys_list))) != drilldown_response_data['total_workitems']:
                data_validation_flg = False
        except Exception as ex:
            LOG.info("exeception occured in keys data validator {}".format(ex))
            return False, False
        return True, data_validation_flg

    def ado_sprint_trend_widget_vs_drilldown_data_validator(self, widget_response, 
                            drilldown_response, req_columns, sort_column_name=None,
                            unique_id=""):
        result_flag = True
        if widget_response['count'] != drilldown_response['count']:
            LOG.info("Widget vs Drilldown count not matching")
            LOG.info("Widget count - {}".format(widget_response["count"]))
            LOG.info("Drilldown count - {}".format(drilldown_response["count"]))
            result_flag = False
        try:
            widget_response_df = pd.json_normalize(widget_response['records'])
            drilldown_response_df = pd.json_normalize(drilldown_response['records'])
            if sort_column_name:
                widget_response_df = widget_response_df.sort_values(sort_column_name)
                drilldown_response_df = drilldown_response_df.sort_values(sort_column_name)
            else:
                widget_response_df = widget_response_df.sort_values(req_columns[0])
                drilldown_response_df = drilldown_response_df.sort_values(req_columns[0])
            if req_columns:               
                widget_response_df = pd.DataFrame(widget_response_df, columns=req_columns)
                drilldown_response_df = pd.DataFrame(drilldown_response_df, columns=req_columns)
            widget_response_df = widget_response_df.astype(str)
            drilldown_response_df = drilldown_response_df.astype(str)
            res = pd.merge(widget_response_df, drilldown_response_df, on=req_columns,
                            how='outer', indicator=True)
            LOG.info("Widget data - {}".format(res[res['_merge'] == 'right_only']))
            LOG.info("Drilldown data - {}".format(res[res['_merge'] == 'left_only']))
            dd_df = pd.DataFrame([unique_id + "-" + self.data_time.strftime("%m/%d/%Y, %H:%M:%S")])
            dd_df.to_csv( "log_updates/" + unique_id + '.csv', header=True, index=False, mode='a')
            if len(res[res['_merge'] == 'right_only']) != 0:
                result_flag = False
            if len(res[res['_merge'] == 'left_only']) != 0:
                (res[res['_merge'] == 'left_only']).to_csv(
                    "log_updates/" + unique_id + '.csv', header=True,index=False, mode='a')
                result_flag = False
        except Exception as ex:
            LOG.info("Exception occured in widget vs drilldown data validator {}".format(ex))
            return False, False
        return True, result_flag

    def ado_sprint_metrics_single_stat_payload_generate(self, required_test_params, 
                    integration_ids, ou_id, ado_custom_field_helper_object,
                    time_range_filter2=False):
        time_range_filter,filters,filter2 = required_test_params
        ## default filters
        dyn_filters = {"include_workitem_ids":True,"agg_type":"average",
                            "integration_ids":integration_ids}
        ## dynamic filter
        filters_key, filters_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=filters, ou_id=ou_id)
        dyn_filters[filters_key] = filters_val
        ## teams filter
        filters2_key, filters2_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=filter2, ou_id=ou_id)
        dyn_filters[filters2_key] = filters2_val
        ## time range filter
        timerange_key, timerange_val = ado_custom_field_helper_object.ado_dynamic_filter_maker(
                                                    filter_name=time_range_filter, ou_id=ou_id,
                                                    time_range_filter=True, timerange_factor=2)
        dyn_filters[timerange_key] = timerange_val
        if time_range_filter2:
            dyn_filters["started_at"] = timerange_val
        ## payload making
        payload = {"filter":dyn_filters,"ou_ids":ou_id,
                   "ou_user_filter_designation":{"sprint":["sprint_report"]}}
        return payload
    