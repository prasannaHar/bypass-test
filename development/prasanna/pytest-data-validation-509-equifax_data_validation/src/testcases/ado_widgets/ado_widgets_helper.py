import logging

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.datetime_reusable_functions import DateTimeReusable
import pandas as pd
import numpy as np

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class ADOWidgetsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.datetimeutil = DateTimeReusable()
        self.ado_default_timerange = self.env_info['ado_default_time_range']

    def ado_issues_report_payload_generate(self, required_test_params, 
                    integration_ids, ou_id, ado_custom_field_helper_object=None, hygiene=None):
        across,interval,metric,time_range_filter,filters,filter2,stacks = required_test_params
        rev_filter_type = True
        if filters in ["Custom", "Microsoft"]:
            filters = ado_custom_field_helper_object.ado_retrive_custom_filter(
                                    custom_filter_type=filters, ou_id=ou_id[0])
            rev_filter_type = False
        dyn_filters = {"metric":[metric],"integration_ids":integration_ids}
        if hygiene:
            dyn_filters["hideScore"] = False
            dyn_filters["workitem_hygiene_types"] = [hygiene]
            del dyn_filters["metric"]
        ## dynamic filter 
        if filters != "none":
            filter_vals = self.generic.get_ado_filter_options(
                                        arg_filter_type=filters, ou_id=ou_id[0], 
                                        rev_filter_type=rev_filter_type)
            if filters in ["teams", "code_area"]:
                dyn_filters["workitem_attributes"] = {filters:filter_vals}
            elif ((filters.startswith("Custom")) or (filters.startswith("Microsoft"))):
                dyn_filters["workitem_custom_fields"] = {filters:filter_vals}
            else:
                dyn_filters[filters] = filter_vals
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_ado_filter_options(
                                        arg_filter_type=filter2, ou_id=ou_id[0], 
                                        rev_filter_type=True)
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.ado_default_timerange)
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## payload making        
        payload = {"filter":dyn_filters,"across":across,"across_limit":20,"ou_ids":ou_id,
                   "ou_user_filter_designation":{"sprint":["sprint_report"]}}
        if hygiene: del payload['across_limit']
        if interval != "none":
            payload["interval"] = interval
        if stacks != "none":
            if stacks in ["Custom", "Microsoft"]:
                stacks = ado_custom_field_helper_object.ado_retrive_custom_filter(
                                        custom_filter_type=stacks, ou_id=ou_id[0])
            payload["stacks"] = [stacks]

        return payload

    def ado_issues_report_payload_generate_drilldown(self, required_test_params,  
                                integration_ids, ou_id, req_key_val,pagination_flag=None,
                                ado_custom_field_helper_object=None, hygiene=None):
        across_vs_drilldown_req_key_name_mapping = self.api_info["ado_im_filter_names_mapping"]
        across,interval,metric,time_range_filter,filters,filter2,stacks = required_test_params
        rev_filter_type = True
        if filters in ["Custom", "Microsoft"]:
            filters = ado_custom_field_helper_object.ado_retrive_custom_filter(
                                    custom_filter_type=filters, ou_id=ou_id[0])
            rev_filter_type = False
        dyn_filters = {"metric":[metric],"integration_ids":integration_ids}
        if hygiene:
            dyn_filters["hideScore"] = False
            dyn_filters["workitem_hygiene_types"] = [hygiene]
            del dyn_filters["metric"]
        ## dynamic filter 
        if filters != "none":
            filter_vals = self.generic.get_ado_filter_options(
                                        arg_filter_type=filters, ou_id=ou_id[0], 
                                        rev_filter_type=rev_filter_type)
            if filters in ["teams", "code_area"]:
                dyn_filters["workitem_attributes"] = {filters:filter_vals}
            elif ((filters.startswith("Custom")) or (filters.startswith("Microsoft"))):
                dyn_filters["workitem_custom_fields"] = {filters:filter_vals}
            else:
                dyn_filters[filters] = filter_vals
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_ado_filter_options(
                                        arg_filter_type=filter2, ou_id=ou_id[0], 
                                        rev_filter_type=True)
        if stacks != "none":
            dyn_filters["stacks"] = [stacks]
        ## time range filter
        gt_widget, lt_widget = self.generic.get_epoc_utc(value_and_type=self.ado_default_timerange)
        dyn_filters[time_range_filter] = {"$gt": gt_widget, '$lt': lt_widget}
        ## widget req_key_val argument specific filter name identification mapping 
        req_key_name = across
        if across in across_vs_drilldown_req_key_name_mapping.keys():
            req_key_name = across_vs_drilldown_req_key_name_mapping[across]
        ## widget specific filter
        if(across in ["reviewer_count", "approver_count"]):
            temp_filter_name =  "num_" + (across.split("_"))[0] + "s"
            dyn_filters[temp_filter_name] = {"$gte": req_key_val, "$lte": req_key_val} 
        elif across in ["trend"]:
            dyn_filters["ingested_at"] = int(req_key_val)
        elif across in ["teams", "code_area"]:
            dyn_filters["workitem_attributes"] = {across:[req_key_val]}
        elif across in ["story_points"]:
            dyn_filters["workitem_story_points"] = {"$gte": req_key_val, "$lte": req_key_val}
        elif(across in ["workitem_created_at", "workitem_updated_at", "workitem_resolved_at"]):
            gt, lt = "", ""
            if interval == "week": 
                gt, lt = self.datetimeutil.get_week_range(req_key_val)
            elif interval == "month":
                gt, lt = self.datetimeutil.get_month_range(req_key_val)
            elif interval == "quarter":
                gt, lt = self.datetimeutil.get_quarter_range(req_key_val)
            if not (gt_widget <= gt <= lt_widget):
                gt = gt_widget
            if not (gt_widget <= lt <= lt_widget):
                lt = lt_widget
            dyn_filters[across] = {"$lt": lt, "$gt": gt}
        elif hygiene:
            print("drilldown specific key not required")
        else:
            dyn_filters[req_key_name] = [req_key_val]
        # if hygiene: del dyn_filters[req_key_name]
        ## payload making        
        payload = {"filter":dyn_filters,"across":across, "ou_ids":ou_id,
                   "ou_user_filter_designation":{"sprint":["sprint_report"]}}
        ## ou exclusions
        if across in ["sprint"]:
            payload["ou_exclusions"] = [across]
        if hygiene: payload["ou_exclusions"]=["workitem_hygiene_types"]
        ##limiting the drill-down data based on page size
        if pagination_flag:
            payload["page"]=0
            payload["page_size"]=10

        return payload

    def ado_issues_report_widget_versus_drilldown_data_consistency_check(self, 
                        widget_val, drilldown_resp_df, req_column):
        data_consistency_flg = True
        try:
            drilldown_resp_df = drilldown_resp_df.fillna(0)
            drilldown_val = 0
            if req_column in drilldown_resp_df.columns.tolist():
                drilldown_val = drilldown_resp_df[req_column].sum()
            if not (-1 <= widget_val - round(drilldown_val) <= 1 ):
                LOG.info("widget versus drill-down data mis-match - " + str( widget_val - round(drilldown_val)))
                data_consistency_flg = False
        except Exception as ex:
            LOG.info("exeception occured in data consistency check {}".format(ex))
            return False, False
        return True, data_consistency_flg

    def ado_issues_report_stacks_data_validator(self, response_data, req_metric):
        data_validation_flg = True
        try:
            response_data_df = pd.json_normalize(response_data, max_level=1)
            response_data_df['data_validation'] = response_data_df.apply(
                        lambda x: self.ado_validate_stacks_data(
                                    x['stacks'], x[req_metric], req_metric),axis=1)
            result_df = response_data_df[response_data_df['data_validation']==False]
            if len(result_df) != 0:
                data_validation_flg = False
        except Exception as ex:
            LOG.info("exeception occured in stacks data validator {}".format(ex))
            return False, False
        return True, data_validation_flg
    
    def ado_validate_stacks_data(self, stacks_data, total_count, req_metric):
        comparison_flg = False
        ## calculating the lead time from ticket stages
        stacked_data_df = pd.json_normalize(stacks_data, max_level=1)
        stacked_data_df = stacked_data_df.fillna(0)
        total_count_calc = 0
        if req_metric in stacked_data_df.columns.to_list():
            total_count_calc = stacked_data_df[req_metric].sum()
        ## validating the actual lead time with the lead time in the api level
        if -1 <= total_count-total_count_calc <= 1:
            comparison_flg = True
        return comparison_flg


    def ado_issues_report_drilldown_data_validator(self, response_data_df, required_test_params, 
                                 api_reusable_functions_object, integration_ids, ou_id=None,
                                 ado_custom_field_helper_object=None, filter_indexes=[4,5]):
        data_validation_flg = True
        try:
            ## retrieve filter v/s key values mapping function
            ado_im_filter_names_mapping = self.api_info["ado_im_filter_names_mapping"]
            filter_vs_df_columns_maping = api_reusable_functions_object.swap_dict_keys_vals(
                                                        input_dict=ado_im_filter_names_mapping)
            filter_vs_drilldown_column_mapping = self.api_info["ado_im_filter_vs_drilldown_column_mapping"]
            ## test params classifcation
            filters,filter2 = required_test_params[filter_indexes[0]], required_test_params[filter_indexes[1]]
            rev_filter_type = True
            if filters in ["Custom", "Microsoft"]:
                filters = ado_custom_field_helper_object.ado_retrive_custom_filter(
                                        custom_filter_type=filters, ou_id=ou_id[0])
                rev_filter_type = False
            ## dynamic filter 
            if filters not in ["none", "workitem_hygiene_types","workitem_sprint_states", 
                               "workitem_story_points"]:
                ## filter values used
                filter_values_sent = self.generic.get_ado_filter_options(
                                        arg_filter_type=filters, integrationID=integration_ids, 
                                        rev_filter_type=rev_filter_type)
                ## received filter values from drill-down data
                req_column = filters
                if filters in filter_vs_df_columns_maping.keys():
                    req_column = (filter_vs_df_columns_maping[filters])[0]
                if (req_column in filter_vs_drilldown_column_mapping.keys()):
                    req_column = filter_vs_drilldown_column_mapping[req_column]
                if (req_column.startswith("Custom")) or (req_column.startswith("Microsoft")):
                    req_column = "custom_fields." + req_column
                filter_values_actual =  response_data_df[req_column].values.tolist()
                if type(filter_values_actual) != type(filter_values_actual[0]):
                    filter_values_actual = [x for x in filter_values_actual if pd.notnull(x)]
                ## validating drill-down data
                data_validation_flg = api_reusable_functions_object.list_vals_comparision(
                                                reference_vals=filter_values_sent,
                                                received_vals=filter_values_actual)
            ## project filter
            if filter2 != "none":
                filter2_values_sent = self.generic.get_ado_filter_options(
                                        arg_filter_type=filter2, integrationID=integration_ids, 
                                        rev_filter_type=True)
                ## received filter values from drill-down data
                req_column = filter2
                if filter2 not in response_data_df.columns.to_list():
                    req_column = (filter_vs_df_columns_maping[filter2])[0]
                filter2_values_actual =  response_data_df[req_column].values.tolist()
                ## validating drill-down data
                data_validation_flg = api_reusable_functions_object.list_vals_comparision(
                                                reference_vals=filter2_values_sent,
                                                received_vals=filter2_values_actual)
        except Exception as ex:
            LOG.info("exeception occured in data validator {}".format(ex))
            return False, False

        return True, data_validation_flg


    def ado_issue_resolution_time_report_payload_generate(self, required_test_params, 
                    integration_ids, ou_id, ado_custom_field_helper_object=None):
        across,interval,time_range_filter,filters,filter2 = required_test_params
        rev_filter_type = True
        if filters in ["Custom", "Microsoft"]:
            filters = ado_custom_field_helper_object.ado_retrive_custom_filter(
                                    custom_filter_type=filters, ou_id=ou_id[0])
            rev_filter_type = False
        dyn_filters = {"metric":["median_resolution_time","number_of_tickets_closed",
                        "90th_percentile_resolution_time","average_resolution_time"],
                        "sort_xaxis":"value_high-low","integration_ids":integration_ids}
        ## dynamic filter 
        if filters != "none":
            filter_vals = self.generic.get_ado_filter_options(
                                        arg_filter_type=filters, ou_id=ou_id[0], 
                                        rev_filter_type=rev_filter_type)
            if filters in ["teams", "code_area"]:
                dyn_filters["workitem_attributes"] = {filters:filter_vals}
            elif ((filters.startswith("Custom")) or (filters.startswith("Microsoft"))):
                dyn_filters["workitem_custom_fields"] = {filters:filter_vals}
            else:
                dyn_filters[filters] = filter_vals
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_ado_filter_options(
                                        arg_filter_type=filter2, ou_id=ou_id[0], 
                                        rev_filter_type=True)
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.ado_default_timerange)
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## payload making        
        payload = {"filter":dyn_filters,"across":across,"ou_ids":ou_id,
                   "sort":[{"id":"resolution_time","desc":True}],
                   "ou_user_filter_designation":{"sprint":["sprint_report"]}}
        if interval != "none":
            payload["interval"] = interval

        return payload

    def ado_issue_resolution_time_report_payload_generate_drilldown(self, required_test_params,  
                                integration_ids, ou_id, req_key_val,pagination_flag=None,
                                ado_custom_field_helper_object=None):
        across_vs_drilldown_req_key_name_mapping = self.api_info["ado_im_filter_names_mapping"]
        across,interval,time_range_filter,filters,filter2 = required_test_params
        rev_filter_type = True
        if filters in ["Custom", "Microsoft"]:
            filters = ado_custom_field_helper_object.ado_retrive_custom_filter(
                                    custom_filter_type=filters, ou_id=ou_id[0])
            rev_filter_type = False
        dyn_filters = {"metric":["median_resolution_time","number_of_tickets_closed",
                        "90th_percentile_resolution_time","average_resolution_time"],
                        "integration_ids":integration_ids, "include_solve_time":True}
        ## dynamic filter 
        if filters != "none":
            filter_vals = self.generic.get_ado_filter_options(
                                        arg_filter_type=filters, ou_id=ou_id[0], 
                                        rev_filter_type=rev_filter_type)
            if filters in ["teams", "code_area"]:
                dyn_filters["workitem_attributes"] = {filters:filter_vals}
            elif ((filters.startswith("Custom")) or (filters.startswith("Microsoft"))):
                dyn_filters["workitem_custom_fields"] = {filters:filter_vals}
            else:
                dyn_filters[filters] = filter_vals
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_ado_filter_options(
                                        arg_filter_type=filter2, ou_id=ou_id[0], 
                                        rev_filter_type=True)
        ## time range filter
        gt_widget, lt_widget = self.generic.get_epoc_utc(value_and_type=self.ado_default_timerange)
        dyn_filters[time_range_filter] = {"$gt": gt_widget, '$lt': lt_widget}
        ## widget req_key_val argument specific filter name identification mapping 
        req_key_name = across
        if across in across_vs_drilldown_req_key_name_mapping.keys():
            req_key_name = across_vs_drilldown_req_key_name_mapping[across]
        ## widget specific filter
        if(across in ["reviewer_count", "approver_count"]):
            temp_filter_name =  "num_" + (across.split("_"))[0] + "s"
            dyn_filters[temp_filter_name] = {"$gte": req_key_val, "$lte": req_key_val} 
        elif across in ["trend"]:
            dyn_filters["ingested_at"] = int(req_key_val)
        elif across in ["teams", "code_area"]:
            dyn_filters["workitem_attributes"] = {across:[req_key_val]}
        elif across in ["story_points"]:
            dyn_filters["workitem_story_points"] = {"$gte": req_key_val, "$lte": req_key_val}
        elif(across in ["workitem_created_at", "workitem_updated_at", "workitem_resolved_at"]):
            gt, lt = "", ""
            if interval == "week": 
                gt, lt = self.datetimeutil.get_week_range(req_key_val)
            elif interval == "month":
                gt, lt = self.datetimeutil.get_month_range(req_key_val)
            elif interval == "quarter":
                gt, lt = self.datetimeutil.get_quarter_range(req_key_val)
            if not (gt_widget <= gt <= lt_widget):
                gt = gt_widget
            if not (gt_widget <= lt <= lt_widget):
                lt = lt_widget
            dyn_filters[across] = {"$lt": lt, "$gt": gt}
        else:
            dyn_filters[req_key_name] = [req_key_val]
        ## payload making        
        payload = {"filter":dyn_filters,"across":across, "ou_ids":ou_id,
                   "ou_user_filter_designation":{"sprint":["sprint_report"]}}
        ## ou exclusions
        if across in ["sprint"]:
            payload["ou_exclusions"] = [across]
        ##limiting the drill-down data based on page size
        if pagination_flag:
            payload["page"]=0
            payload["page_size"]=10

        return payload

