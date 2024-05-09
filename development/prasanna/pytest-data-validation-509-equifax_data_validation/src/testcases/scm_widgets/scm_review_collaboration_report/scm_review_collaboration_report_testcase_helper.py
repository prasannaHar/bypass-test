import logging
import pytest
from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmReviewCollaborationHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()

    def scm_review_collaboration_report(self, integration_id, var_filters=False,
                                        keys=False):
        """ create scm review collaboration report """
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id,
                    "pr_closed_at": {"$gt": gt, "$lt": lt}}

        if var_filters:
            filters.update(var_filters)
        base_url = self.generic.connection["base_url"] + self.api_info["scm_collab_report"]
        resp = self.widget.create_widget_report(ou_ids=org_id, filters=filters,base_url=base_url)
        if resp:
            if keys:
                multikeys = {}
                for key in resp["records"]:
                    try:
                        if key["key"] in multikeys:
                            multikeys[key["key"]] = multikeys[key["key"]] + key["count"]
                        else:
                            multikeys[key["key"]] = key["count"]

                    except:
                        continue
                sorted_dict = dict(sorted(multikeys.items(), key=lambda x: x[1], reverse=True)[:3])
                return sorted_dict
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            pytest.skip("no data in widget API")
            return None

    def scm_review_collaboration_drilldown(self, integration_id, key, key_option="creators",
                                    var_filters=False):
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id, key_option: [key], "pr_closed_at": {"$gt": gt, "$lt": lt}}
        if var_filters:
            filters.update(var_filters)

        resp = self.widget.scm_pr_list_drilldown_list(ou_ids=org_id, filters=filters, ou_exclusions=key_option)

        return resp


    def scm_review_collaboration_report_payload_generate(self, required_test_params, integration_ids, ou_id):
        filters,filter2,time_range_filter,pr_merged = required_test_params
        dyn_filters = { "integration_ids":integration_ids }        
        ## dynamic filter 
        if filters != "none":
            dyn_filters[filters] = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_filter_options_scm(
                                        report_type="prs",arg_filter_type=filter2, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## pr filter
        if pr_merged != "none":
            temp_filter_val = True
            if pr_merged == "false": temp_filter_val = False
            dyn_filters["missing_fields"] = {"pr_merged":temp_filter_val}
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## payload making        
        payload = {"filter":dyn_filters,"ou_ids":ou_id}

        return payload


    def scm_review_collaboration_report_payload_generate_drilldown(self, required_test_params, 
                                integration_ids, ou_id, req_key_val, req_key_name="creators"):
        filters,filter2,time_range_filter,pr_merged = required_test_params
        dyn_filters = { "integration_ids":integration_ids }        
        ## dynamic filter 
        if filters != "none":
            dyn_filters[filters] = self.generic.get_filter_options_scm(
                                        report_type="prs", arg_filter_type=filters, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## project filter
        if filter2 != "none":
            dyn_filters[filter2] = self.generic.get_filter_options_scm(
                                        report_type="prs",arg_filter_type=filter2, 
                                        integrationID=integration_ids,rev_filter_type=True)
        ## pr filter
        if pr_merged != "none":
            temp_filter_val = True
            if pr_merged == "false": temp_filter_val = False
            dyn_filters["missing_fields"] = {"pr_merged":temp_filter_val}
        ## time range filter
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        dyn_filters[time_range_filter] = {"$gt": gt, '$lt': lt}
        ## widget req_key_val argument specific filter name identification mapping 
        dyn_filters[req_key_name] = [req_key_val]
        ## payload making        
        payload = {"filter":dyn_filters,"ou_ids":ou_id,"ou_exclusions":[req_key_name]}

        return payload

    def scm_review_collaboration_report_drilldown_data_validator(self, response_data_df, required_test_params, 
                                 api_reusable_functions_object, integration_ids):
        data_validation_flg = True
        try:
            ## retrieve filter v/s key values mapping function
            ou_exclusion_field_maping = self.api_info["scm_exclusions_vs_across_mapping"]
            filter_vs_df_columns_maping = api_reusable_functions_object.swap_dict_keys_vals(
                                                        input_dict=ou_exclusion_field_maping)
            ## test params classifcation
            filters,filter2,time_range_filter,pr_merged = required_test_params
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
                filter_values_actual =  response_data_df[req_column].values.tolist()
                if type(filter_values_actual[0]) == type([]):
                    filter_values_actual = [item for sublist in filter_values_actual for item in sublist]                
                ## validating drill-down data
                if not all(item in filter_values_sent for item in filter_values_actual):
                    data_validation_flg = False
            ## project filter
            if filter2 != "none":
                filter2_values_sent = self.generic.get_filter_options_scm(
                                            report_type="prs",arg_filter_type=filter2, 
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