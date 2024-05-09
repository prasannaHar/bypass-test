import logging

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.datetime_reusable_functions import DateTimeReusable
import pandas as pd
import numpy as np

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmPrsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.datetimeutil = DateTimeReusable()

    def scm_prs_report(self, integration_id, across="repo_id", var_filters=False,
                       keys=False,user_type="ADMIN"):
        """ create scm_prs_report"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"code_change_size_unit": "files", "code_change_size_config": {"small": "50", "medium": "150"},
                   "comment_density_size_config": {"shallow": "1", "good": "5"},
                   "integration_ids": integration_id}
        if var_filters:
            filters.update(var_filters)
        base_url = self.generic.connection["base_url"] + self.api_info["scm_prs-report"]

        resp = self.widget.create_widget_report(ou_ids=org_id, filters=filters, across=across, base_url=base_url,user_type=user_type)
        if resp:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    multikeys.append({"key": key["key"], "count": key["count"]})
                return multikeys
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def scm_pr_response_time_drilldown(self, integration_id, key, key_option="repo_ids", across="repo_id",
                                       var_filters=False):
        """ create scm_prs_report drilldown  """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id, key_option: [key], "code_change_size_unit": "files",
                   "code_change_size_config": {"small": "50", "medium": "150"},
                   "comment_density_size_config": {"shallow": "1", "good": "5"}}
        if var_filters:
            filters.update(var_filters)

        resp = self.widget.scm_pr_list_drilldown_list(ou_ids=org_id, filters=filters, ou_exclusions=key_option,
                                                      across=across)

        return resp

    def scm_prs_report_payload_generate(self, required_test_params, integration_ids, ou_id):
        across,interval,time_range_filter,filters,filter2,filter3,\
            code_change_size_unit,sort_xaxis = required_test_params
        dyn_filters = {"sort_xaxis":sort_xaxis,"integration_ids":integration_ids,
                    "code_change_size_unit":code_change_size_unit,
                    "code_change_size_config":{"small":"50","medium":"150"},
                    "comment_density_size_config":{"shallow":"1","good":"5"}}
        ## dynamic filter 
        if filters != "none":
            dyn_filters[filters] = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## project filter
        if filter3 != "none":
            dyn_filters[filter3] = self.generic.get_filter_options_scm(
                                        report_type="prs",arg_filter_type=filter3, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## number of approvers/reviwers filter
        if filter2 != "none":
            dyn_filters[filter2] = {"$gt": "1", "$lt": "10"}
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## payload making        
        payload = {"filter":dyn_filters,"across":across,
            "sort":[{"id":"count","desc":True}],"ou_ids":ou_id}
        if interval != "none":
            payload["interval"] = interval

        return payload


    def scm_prs_report_payload_generate_drilldown(self, required_test_params, 
                                integration_ids, ou_id, req_key_val):
        ou_exclusion_field_maping = self.api_info["scm_exclusions_vs_across_mapping"]
        across,interval,time_range_filter,filters,filter2,filter3,\
            code_change_size_unit,sort_xaxis = required_test_params
        dyn_filters = {"integration_ids":integration_ids,
                    "code_change_size_unit":code_change_size_unit,
                    "code_change_size_config":{"small":"50","medium":"150"},
                    "comment_density_size_config":{"shallow":"1","good":"5"}}
        ## dynamic filter 
        if filters != "none":
            dyn_filters[filters] = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## project filter
        if filter3 != "none":
            dyn_filters[filter3] = self.generic.get_filter_options_scm(
                                        report_type="prs",arg_filter_type=filter3, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## number of approvers/reviwers filter
        if filter2 != "none":
            dyn_filters[filter2] = {"$gt": "1", "$lt": "10"}
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## widget req_key_val argument specific filter name identification mapping 
        req_key_name = across
        if across in ou_exclusion_field_maping.keys():
            req_key_name = ou_exclusion_field_maping[across]
        ## widget specific filter
        if(across in ["reviewer_count", "approver_count"]):
            temp_filter_name =  "num_" + (across.split("_"))[0] + "s"
            dyn_filters[temp_filter_name] = {"$gte": req_key_val, "$lte": req_key_val} 
        elif(across in ["pr_closed", "pr_created"]):
            gt, lt = "", ""
            if interval == "week": 
                gt, lt = self.datetimeutil.get_week_range(req_key_val)
            elif interval == "month":
                gt, lt = self.datetimeutil.get_month_range(req_key_val)
            elif interval == "quarter":
                gt, lt = self.datetimeutil.get_quarter_range(req_key_val)
            dyn_filters[across+"_at"] = {"$lt": lt, "$gt": gt}
        else:
            dyn_filters[req_key_name] = [req_key_val]
        ## payload making        
        payload = {"filter":dyn_filters,"across":across,
                    "ou_ids":ou_id,"ou_exclusions":[req_key_name]}

        return payload

    def scm_prs_report_drilldown_data_validator(self, response_data_df, required_test_params, 
                                 api_reusable_functions_object, integration_ids):
        data_validation_flg = True
        try:
            ## retrieve filter v/s key values mapping function
            ou_exclusion_field_maping = self.api_info["scm_exclusions_vs_across_mapping"]
            filter_vs_df_columns_maping = api_reusable_functions_object.swap_dict_keys_vals(
                                                        input_dict=ou_exclusion_field_maping)
            ## test params classifcation
            across,interval,time_range_filter,filters,filter2,filter3,\
                code_change_size_unit,sort_xaxis = required_test_params
            ## dynamic filter 
            if filters not in ["none", "has_issue_keys"]:
                ## filter values used
                filter_values_sent = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
                ## received filter values from drill-down data
                req_column = (filter_vs_df_columns_maping[filters])[0]
                if req_column in ["assignee", "reviewer", "commenter"]:
                    req_column = req_column + "_ids"
                elif req_column == "creator": 
                    req_column = "creator_id"
                elif req_column == "label":
                    req_column = "labels"
                if req_column not in response_data_df.columns.tolist():
                    return True, True
                filter_values_actual =  response_data_df[req_column].values.tolist()
                filter_values_actual_temp = []
                if type(filter_values_actual[0]) == type([]):
                    for sublist in filter_values_actual:
                        cnt = 0
                        for item in sublist:
                            if item in filter_values_sent:
                                filter_values_actual_temp.append(item)
                                cnt = 1
                                break
                        if cnt == 0:
                            data_validation_flg = False
                    filter_values_actual = filter_values_actual_temp
                ## validating drill-down data
                if not all(item in filter_values_sent for item in filter_values_actual):
                    data_validation_flg = False
            ## project filter
            if filter3 != "none":
                filter3_values_sent = self.generic.get_filter_options_scm(
                                            report_type="prs",arg_filter_type=filter3, 
                                            integrationID=integration_ids,rev_filter_type=True)
                ## received filter values from drill-down data
                req_column = filter3
                if filter3 not in response_data_df.columns.to_list():
                    req_column = (filter_vs_df_columns_maping[filter3])[0]
                if req_column not in response_data_df.columns.tolist():
                    return True, True
                filter3_values_actual =  response_data_df[req_column].values.tolist()
                ## validating drill-down data
                if not all(item in filter3_values_sent for item in filter3_values_actual):
                    data_validation_flg = False
        except Exception as ex:
            LOG.info("exeception occured in data validator {}".format(ex))
            return False, False

        return True, data_validation_flg
    
    def scm_prs_single_stat_payload_generate(self, required_test_params, integration_ids, ou_id):
        across,time_range_filter,filters,filter2,filter3 = required_test_params
        {"across":"pr_created","ou_ids":["1"],"widget_id":"a4c10440-208b-11ee-b6d5-cf5032d83c7a"}
        dyn_filters = {"integration_ids":integration_ids,
                    "code_change_size_unit":"files",
                    "code_change_size_config":{"small":"50","medium":"150"},
                    "comment_density_size_config":{"shallow":"1","good":"5"}}
        ## dynamic filter 
        if filters != "none":
            dyn_filters[filters] = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## project filter
        if filter3 != "none":
            dyn_filters[filter3] = self.generic.get_filter_options_scm(
                                        report_type="prs",arg_filter_type=filter3, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## number of approvers/reviwers filter
        if filter2 != "none":
            dyn_filters[filter2] = {"$gt": "1", "$lt": "10"}
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## payload making
        payload = {"filter":dyn_filters,"across":across,"ou_ids":ou_id}
        return payload
