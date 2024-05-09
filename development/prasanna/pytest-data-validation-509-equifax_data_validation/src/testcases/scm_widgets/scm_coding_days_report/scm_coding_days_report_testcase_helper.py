import logging
import pandas as pd
import time

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmCodingDaysHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()

    def scm_coding_days_report(self, integration_id, committed_at, across="repo_id", interval="week", var_filters=False,
                               keys=False):
        """ create scm coding days report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"sort_xaxis": "value_high-low", "committed_at": committed_at,
                   "integration_ids": integration_id}
        if var_filters:
            filters.update(var_filters)
        base_url = self.generic.connection["base_url"] + self.api_info["scm_coding_days_report"]
        resp = self.widget.create_widget_report(ou_ids=org_id, filters=filters, across=across,
                                                interval=interval, base_url=base_url)
        if resp:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    multikeys.append({"key": key["key"], "mean": key["mean"]})
                return multikeys
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def scm_coding_days_report_drilldown(self, integration_id, committed_at, key, key_option="repo_ids",
                                         across="repo_id", var_filters=False):
        """ create scm coding days report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"committed_at": committed_at, "integration_ids": integration_id, key_option: [key],
                   "include_metrics": True}
        if var_filters:
            filters.update(var_filters)

        resp = self.widget.scm_commits_drilldown_list(ou_ids=org_id, filters=filters, across=across)

        return resp

    def scm_coding_days_report_payload_generate(self, required_test_params, integration_ids, ou_id):
        across,metrics,interval,time_range_filter,filters,filter2 = required_test_params
        dyn_filters = {"sort_xaxis":"value_high-low", "integration_ids":integration_ids, "metrics":metrics+"_"+interval}
        ## dynamic filter 
        if filters != "none":
            dyn_filters[filters] = self.generic.get_filter_options_scm(
                                        report_type="commits", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_filter_options_scm(
                                        report_type="commits",arg_filter_type=filter2, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## payload making        
        payload = {"filter":dyn_filters,"across":across,"ou_ids":ou_id, "interval":interval,
                   "sort":[{"id":"commit_days","desc":True}]}

        return payload


    def scm_coding_days_report_payload_generate_drilldown(self, required_test_params, 
                                integration_ids, ou_id, req_key_val):
        ou_exclusion_field_maping = self.api_info["scm_exclusions_vs_across_mapping"]
        across,metrics,interval,time_range_filter,filters,filter2 = required_test_params
        dyn_filters =  {"sort_xaxis":"value_high-low","include_metrics":False, 
                        "integration_ids":integration_ids, 
                        "metrics":metrics+"_"+interval}
        ## dynamic filter 
        if filters != "none":
            dyn_filters[filters] = self.generic.get_filter_options_scm(
                                        report_type="commits", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_filter_options_scm(
                                        report_type="commits",arg_filter_type=filter2, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## time range filter
        gt_widget, lt_widget = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        dyn_filters[time_range_filter] = {"$gt": gt_widget, '$lt': lt_widget}
        ## widget req_key_val argument specific filter name identification mapping 
        req_key_name = across
        if across in ou_exclusion_field_maping.keys():
            req_key_name = ou_exclusion_field_maping[across]
        dyn_filters[req_key_name] = [req_key_val]
        ## payload making 
        payload = {"filter":dyn_filters,"across":across,
                    "ou_ids":ou_id, "ou_exclusions":[req_key_name]}
        return payload

    def scm_coding_days_widget_versus_drilldown_data_consistency_check(self, 
                        required_test_params, widget_vals, drilldown_resp_df):
        across,metrics,interval,time_range_filter,filters,filter2 = required_test_params
        coding_days_multiplication_factor =  self.api_info["scm_coding_days_config"]
        coding_days_multiplication_factor_tuning =  self.api_info["scm_coding_days_config_tuning"]
        data_consistency_flg = True
        try:
            ## converting commited at epoch timestamps to date only column
            if len(drilldown_resp_df['committed_at'])>0:
                if  drilldown_resp_df['committed_at'].tolist()[0] > time.mktime(time.gmtime()):
                    drilldown_resp_df['committed_at'] = drilldown_resp_df['committed_at']//1000
            drilldown_resp_df = drilldown_resp_df.fillna(0)
            drilldown_resp_df['committed_at_date'] = pd.to_datetime(drilldown_resp_df['committed_at'], unit='s')
            drilldown_resp_df['committed_at_date'] = pd.to_datetime(drilldown_resp_df['committed_at_date'], format='%d-%M-%Y')
            drilldown_resp_df['committed_at_date'] = drilldown_resp_df['committed_at_date'].astype(str)
            split_values = drilldown_resp_df['committed_at_date'].str.split(' ', expand=True)
            drilldown_resp_df['committed_at_date'] = split_values[0]
            ## retrieving the unique commit dates
            commit_dates = drilldown_resp_df['committed_at_date'].values.tolist()
            ## coding days calculation
            committed_in_days = self.env_info['scm_default_time_range'][0]
            number_of_coding_days = len(list(set(commit_dates)))/committed_in_days ## - commited in days
            number_of_coding_days_based_interval = number_of_coding_days * coding_days_multiplication_factor[interval]
            tuning_factor = coding_days_multiplication_factor_tuning[interval]
            if not (-tuning_factor <= (widget_vals[0] - number_of_coding_days_based_interval) <= tuning_factor):
                data_consistency_flg = False
        except Exception as ex:
            LOG.info("exeception occured in data consistency {}".format(ex))
            return False, False
        return True, data_consistency_flg

    def scm_coding_days_report_drilldown_data_validator(self, response_data_df, required_test_params, 
                                 api_reusable_functions_object, integration_ids):
        data_validation_flg = True
        try:
            ## retrieve filter v/s key values mapping function
            ou_exclusion_field_maping = self.api_info["scm_exclusions_vs_across_mapping"]
            filter_vs_df_columns_maping = api_reusable_functions_object.swap_dict_keys_vals(
                                                        input_dict=ou_exclusion_field_maping)
            ## test params classifcation
            across,metrics,interval,time_range_filter,filters,filter2 = required_test_params
            ## dynamic filter 
            if filters not in ["none", "has_issue_keys", "code_change_sizes"]:
                ## filter values used
                filter_values_sent = self.generic.get_filter_options_scm(
                                        report_type="commits", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
                ## received filter values from drill-down data
                req_column = (filter_vs_df_columns_maping[filters])[0]
                if req_column in ["assignee", "reviewer", "commenter"]:
                    req_column = req_column + "_ids"
                elif req_column in ["committer", "author"]: 
                    req_column = req_column + "_id"
                elif req_column in ["file_type"]:
                    req_column = "file_types"
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
            if filter2 != "none":
                filter2_values_sent = self.generic.get_filter_options_scm(
                                            report_type="commits",arg_filter_type=filter2,
                                            integrationID=integration_ids,rev_filter_type=True)
                ## received filter values from drill-down data
                req_column = filter2
                if filter2 not in response_data_df.columns.to_list():
                    req_column = (filter_vs_df_columns_maping[filter2])[0]
                filter2_values_actual =  response_data_df[req_column].values.tolist()
                ## validating drill-down data
                if not all(item in filter2_values_sent for item in filter2_values_actual):
                    data_validation_flg = False
        except Exception as ex:
            LOG.info("exeception occured in data validator {}".format(ex))
            return False, False

        return True, data_validation_flg