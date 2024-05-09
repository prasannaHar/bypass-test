import logging
import pandas as pd

from src.utils.datetime_reusable_functions import DateTimeReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmPrLeadTimeHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.env_info = self.generic.get_env_based_info()
        self.datetimeutil = DateTimeReusable()

    def scm_pr_lead_time_by_stage_payload_generate(self, required_test_params, integration_ids, ou_id, 
                            api_reusable_functions_object, adv_config={"exclude":False,
                            "partial_match":False,"filter_vals": []}):
        time_range_filter,filters,filter2= required_test_params
        dyn_filters = {"calculation":"pr_velocity",
                   "ratings":["good","slow","needs_attention"],
                   "velocity_config_id":self.generic.env["env_scm_velocity_config_id"],
                   "limit_to_only_applicable_data":False,
                   "integration_ids":integration_ids}
        ## dynamic filter 
        if filters != "none":
            if adv_config["partial_match"]==False:
                filter_vals = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
                if adv_config["exclude"]==True:  dyn_filters["exclude"] = {filters:filter_vals}
                else: dyn_filters[filters] = filter_vals
            else:
                filter_val_pattern = api_reusable_functions_object.partial_match_filter_value_generator(
                                                strings=adv_config["filter_vals"])
                if adv_config["exclude"]==True:
                    dyn_filters["exclude"] = {"partial_match":{filters:{"$contains":filter_val_pattern}}}
                else:
                    dyn_filters["partial_match"] = {filters:{"$contains":filter_val_pattern}}
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_filter_options_scm(
                                        report_type="prs",arg_filter_type=filter2, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(
                    value_and_type=self.env_info['scm_default_time_range'], 
                    timerange_factor=0.5)
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## payload making        
        payload = {"filter":dyn_filters,"across":"velocity","ou_ids":ou_id,
                   "ou_user_filter_designation":{"jira":["none"]}}

        return payload


    def scm_pr_lead_time_by_stage_payload_generate_drilldown(self, required_test_params, 
                                integration_ids, ou_id, req_key_val, api_reusable_functions_object, 
                                adv_config={"exclude":False,"partial_match":False,"filter_vals": []}):
        time_range_filter,filters,filter2 = required_test_params
        dyn_filters = {"ratings":["good","slow","needs_attention","missing"],
                        "velocity_config_id":self.generic.env["env_scm_velocity_config_id"],
                        "limit_to_only_applicable_data":False,
                        "calculation":"pr_velocity","integration_ids":integration_ids,
                        "histogram_stage_name":req_key_val}
        ## dynamic filter 
        if filters != "none":
            if adv_config["partial_match"]==False:
                filter_vals = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
                if adv_config["exclude"]==True:  dyn_filters["exclude"] = {filters:filter_vals}
                else: dyn_filters[filters] = filter_vals
            else:
                filter_val_pattern = api_reusable_functions_object.partial_match_filter_value_generator(
                                                strings=adv_config["filter_vals"])
                if adv_config["exclude"]==True:
                    dyn_filters["exclude"] = {"partial_match":{filters:{"$contains":filter_val_pattern}}}
                else:
                    dyn_filters["partial_match"] = {filters:{"$contains":filter_val_pattern}}
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_filter_options_scm(
                                        report_type="prs",arg_filter_type=filter2, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## time range filter
        gt_widget, lt_widget = self.generic.get_epoc_utc(
                                        value_and_type=self.env_info['scm_default_time_range'], 
                                        timerange_factor=0.5)
        dyn_filters[time_range_filter] = {"$gt": gt_widget, '$lt': lt_widget}
        ## payload making 
        payload = {"sort":[{"id":req_key_val,"desc":True}],"filter":dyn_filters,"across":"values",
                    "ou_ids":ou_id, "ou_user_filter_designation":{"jira":["none"]},
                    "ou_exclusions":["values"]}

        return payload
    
    def scm_pr_lead_time_retrive_mean_median_p90_p95(self, response_data_df, key_name):
        try:
            response_data_df = response_data_df[response_data_df['key']==key_name]
            response_data_df = response_data_df.fillna(0)
            mean = response_data_df["mean"].sum()/response_data_df.shape[0]
            median_values = response_data_df["median"].values.tolist()
            median_index = int(len(median_values) / 2)
            median = median_values[median_index]
            p95 = response_data_df["mean"].quantile(0.95)
            p90 = response_data_df["mean"].quantile(0.90)
        except Exception as ex:
            LOG.error("exception occured during mean value calulcation" + str(ex))
            return 0,0,0,0
        return mean,median,p90,p95

    def scm_pr_lead_time_by_stage_drilldown_data_validator(self, response_data_df):
        data_validation_flg = True
        try:
            response_data_df['data_validation'] = response_data_df.apply(
                        lambda x: self.pr_lead_time_calculator(x['data'], x['total']),axis=1)
            result_df = response_data_df[response_data_df['data_validation']==False]
            if len(result_df) != 0:
                data_validation_flg = False
        except Exception as ex:
            LOG.info("exeception occured in data validator {}".format(ex))
            return False, False

        return True, data_validation_flg
    
    def pr_lead_time_calculator(self, stage_data, lead_time):
        comparison_flg = False
        ## calculating the lead time from ticket stages
        staged_data_df = pd.json_normalize(stage_data, max_level=1)
        staged_data_df = staged_data_df.fillna(0)
        lead_time_actual = 0
        if "median" in staged_data_df.columns.to_list():
            lead_time_actual = staged_data_df['median'].sum()
        ## validating the actual lead time with the lead time in the api level
        if -1 <= lead_time-lead_time_actual <= 1:
            comparison_flg = True
        return comparison_flg
    