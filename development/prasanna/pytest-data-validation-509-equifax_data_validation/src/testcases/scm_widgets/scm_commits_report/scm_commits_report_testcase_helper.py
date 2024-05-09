import logging

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.datetime_reusable_functions import DateTimeReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmCommitsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.datetimeutil = DateTimeReusable()

    def scm_commits_report_payload_generate(self, required_test_params, integration_ids, ou_id):
        across,interval,time_range_filter,filters,filter2,\
            code_change_size_unit = required_test_params
        dyn_filters = {"code_change_size_unit":code_change_size_unit,
                        "integration_ids":integration_ids,
                        "code_change_size_config":{"small":"50","medium":"150"}}
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
        payload = {"filter":dyn_filters,"across":across,"ou_ids":ou_id}
        if interval != "none":
            payload["interval"] = interval

        return payload


    def scm_commits_report_payload_generate_drilldown(self, required_test_params, 
                                integration_ids, ou_id, req_key_val):
        ou_exclusion_field_maping = self.api_info["scm_exclusions_vs_across_mapping"]
        across,interval,time_range_filter,filters,filter2,\
            code_change_size_unit = required_test_params
        dyn_filters = {"integration_ids":integration_ids,
                    "code_change_size_unit":code_change_size_unit,
                    "code_change_size_config":{"small":"50","medium":"150"},
                    "include_metrics":False}
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
        ## widget specific filter
        if interval == "day_of_week":
            dyn_filters[interval] = [req_key_val]
        elif(across in ["trend"]):
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
            dyn_filters["committed_at"] = {"$lt": lt, "$gt": gt}
        else:
            dyn_filters[req_key_name] = [req_key_val]
        ## payload making 
        payload = {"filter":dyn_filters,"across":across,
                    "ou_ids":ou_id}
        if across != "trend":
            payload["ou_exclusions"] = [req_key_name]

        return payload

    def scm_commits_report_drilldown_data_validator(self, response_data_df, required_test_params, 
                                 api_reusable_functions_object, integration_ids):
        data_validation_flg = True
        try:
            ## retrieve filter v/s key values mapping function
            ou_exclusion_field_maping = self.api_info["scm_exclusions_vs_across_mapping"]
            filter_vs_df_columns_maping = api_reusable_functions_object.swap_dict_keys_vals(
                                                        input_dict=ou_exclusion_field_maping)
            ## test params classifcation
            across,interval,time_range_filter,filters,filter2,\
                code_change_size_unit = required_test_params
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