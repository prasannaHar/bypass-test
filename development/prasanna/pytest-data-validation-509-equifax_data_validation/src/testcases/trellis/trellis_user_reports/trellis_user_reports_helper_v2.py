import logging
import os
import pytest
import itertools
import pandas as pd


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TrellisUserReportHelperV2:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()

    def trellis_user_report_params(self):
        trellis_config = self.generic.get_env_based_info(key="trellis_config")
        if type(trellis_config) == type(False):
            return []
        # Generate all unique combinations using itertools.product
        req_combinations = list(itertools.product(
                            trellis_config["intervals"], 
                            trellis_config["collections"], 
                            [trellis_config["contributors"]]))
        return req_combinations

    def trellis_user_report_widget_vs_drilldown_metric_value_analysis(self, trellis_helper_object,
                trellis_validations_helper_object, trellis_profile_helper_object, 
                trellis_user_report, ou_user_id, trellis_profile, interval, force_source=None):
        metrics_validation_results = []
        user_report_parsed_data = trellis_validations_helper_object.trellis_user_report_parsed_data(
                                        trellis_user_report=trellis_user_report)
        for each_factor in user_report_parsed_data:
            factor_details = user_report_parsed_data[each_factor]
            for each_metric in factor_details["sub-features"]:
                sub_feature_metric_value = ( (factor_details["sub-features"])[each_metric] )
                drilldown_response = trellis_helper_object.retrieve_trellis_user_report_drilldown(
                                                    ou_user_id=ou_user_id,
                                                    trelis_profile=trellis_profile,
                                                    interval=interval, feature_name=each_metric, 
                                                    force_source=force_source)
                calculated_widget_value = trellis_helper_object.trellis_drilldown_metric_value_calculation(
                                                    arg_drill_down_response=drilldown_response,
                                                    arg_metric_name=trellis_profile_helper_object.\
                                                        retreive_trellis_profile_metric_name(
                                                            metric_name=each_metric,interval=interval),
                                                        arg_interval=interval)
                if type("") == type(calculated_widget_value):
                    metrics_validation_results.append(
                        [ou_user_id, interval ,each_metric,
                        sub_feature_metric_value, calculated_widget_value, "Fail"] )
                elif (int(sub_feature_metric_value) == int(calculated_widget_value)) or (-0.5 <= (sub_feature_metric_value - calculated_widget_value) <= 0.5):
                    metrics_validation_results.append(
                        [ou_user_id, interval ,each_metric,
                        sub_feature_metric_value, calculated_widget_value, "Pass"] )
                else:
                    metrics_validation_results.append(
                        [ou_user_id, interval ,each_metric,
                        sub_feature_metric_value, calculated_widget_value, "Fail"] )
        metrics_validation_results_df = pd.DataFrame(metrics_validation_results, 
                                            columns =["OU user-id", "Time period",  'Feature Name', 
                                                    'Widget Result', "Drill-down Result", "Status"])
        return metrics_validation_results_df

