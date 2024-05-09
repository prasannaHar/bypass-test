import logging
import pandas as pd

from src.utils.trellis_scores_helper import TrellisScoresHelper
from src.utils.trellis_profile_helper import TrellisProfileHelper


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TrellisValidationsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.baseurl = self.generic.connection["base_url"]
        self.scores_helper = TrellisScoresHelper()
        self.profile_helper = TrellisProfileHelper(generic_helper)


    def trellis_metric_score_validation(self, metric_name, metric_value, metric_score, 
                                    trellis_profile, week_interval=None):
        ## metric settings retriever
        metric_settings_retriever = self.profile_helper.trellis_profile_retreive_metric_settings(
                                        trellis_profile=trellis_profile,
                                        metric_name=metric_name, interval=week_interval)
        name = metric_settings_retriever[0]
        max_value = metric_settings_retriever[1]
        ll = metric_settings_retriever[2]
        ul = metric_settings_retriever[3]
        metric_score_calc = self.scores_helper.trellis_metric_score_calculation(metric_actual_name=metric_name,
                                metric_value=metric_value, profile_metric_name=name, 
                                metric_profile_max_value=max_value, 
                                metric_profile_upper_limit=ul, metric_profile_lower_limit=ll, 
                                week_interval=week_interval)
        ## score validation
        return (-1 <= metric_score-metric_score_calc <=1)


    def trellis_invalid_scores_validation(self, trellis_user_report):
        normalised_data_set = []
        section_responses = trellis_user_report['section_responses']
        org_user_details = { key:val for key,val in trellis_user_report.items() if key in ["org_user_id","full_name","email"]}
        for sublist in section_responses:
            sublist.update(org_user_details)
            normalised_data_set.append(sublist)
        scores_df = pd.json_normalize(normalised_data_set)
        invalid_scores_df = scores_df.loc[(scores_df['score'] >= 100) | (scores_df['score'] < 0 )]
        return len(invalid_scores_df)!=0


    def trellis_metric_and_factor_scores_validation(self, trellis_user_report, trellis_profile, week_interval=None):
        score_validation_failed_metrics = []
        scores_validation_failed_features = []    
        for each_element in trellis_user_report["section_responses"]:
            temp_sub_feature_and_score_dict = {}
            feature_wise_scores = 0
            valid_sub_features_cntr = 0
            for each_sub_element in each_element["feature_responses"]:
                if "score" in each_sub_element:
                    feature_wise_scores = feature_wise_scores + each_sub_element["score"]
                    mean = 0
                    if "mean" in each_sub_element:
                        mean = each_sub_element["mean"]
                    metric_score_validation = self.trellis_metric_score_validation(
                                                metric_name=each_sub_element["name"],
                                                metric_value=mean,
                                                metric_score=each_sub_element["score"],
                                                trellis_profile=trellis_profile,
                                                week_interval = week_interval)
                    if not metric_score_validation:
                        score_validation_failed_metrics.append(each_sub_element["name"])
                    valid_sub_features_cntr = valid_sub_features_cntr+1
                if "mean" in each_sub_element:
                    temp_sub_feature_and_score_dict[each_sub_element["name"]] = each_sub_element["mean"]
                else:
                    temp_sub_feature_and_score_dict[each_sub_element["name"]] = 0
            if "score" in each_element:
                if valid_sub_features_cntr!=0:
                    calculated_score = feature_wise_scores/valid_sub_features_cntr
                    if not ( -0.5 <=(calculated_score - each_element["score"]) <= 0.5):
                        scores_validation_failed_features.append(each_element["name"])
        return scores_validation_failed_features, score_validation_failed_metrics 


    def trellis_validate_missing_factors_and_metrics(self, trellis_user_report, trellis_profile, interval):
        parsed_user_report = self.trellis_user_report_parsed_data(
                                    trellis_user_report=trellis_user_report)
        missing_factors = []
        missing_metrics = []
        ## retreive enabled factors and metrics details
        trellis_profile_enabled_factors, trellis_profile_enabled_metrics = \
                    self.profile_helper.trellis_profile_retrieve_enabled_factors_and_metrics(
                                trellis_profile=trellis_profile)
        ## factors validation
        section_keys_availability_status = {}
        faeture_keys_availability_status = {}
        for eachEnabledSection in trellis_profile_enabled_factors:
            if eachEnabledSection in parsed_user_report.keys(): pass
            else: section_keys_availability_status[eachEnabledSection] = False
        if len(section_keys_availability_status) != 0:
            missing_factors.append(section_keys_availability_status)
        ## metrics validation
        for eachEnabledFeature in trellis_profile_enabled_metrics:
            associated_section, required_feature_name = eachEnabledFeature.split("_")
            dps_available_features_dict = parsed_user_report[associated_section]["sub-features"]
            ## retrieve the dynamic feature names
            metric_names_user_report = dps_available_features_dict.keys()
            if (interval in ["LAST_WEEK", "LAST_TWO_WEEKS"]
                        ) and ("month" in required_feature_name) and (
                            "days" not in required_feature_name):
                dps_available_features_dict_temp = []
                for each_response_key in dps_available_features_dict.keys():
                    dps_available_features_dict_temp.append( 
                        self.profile_helper.retreive_trellis_profile_metric_name(metric_name=each_response_key,interval=interval))
                metric_names_user_report = dps_available_features_dict_temp
            if required_feature_name in metric_names_user_report: pass
            else: faeture_keys_availability_status[eachEnabledFeature] = False
        if len(faeture_keys_availability_status) != 0:
            missing_metrics.append(faeture_keys_availability_status)
        return missing_factors, missing_metrics


    def trellis_user_report_parsed_data(self, trellis_user_report):
        DPS_configured_sections = {}
        for each_element in trellis_user_report["section_responses"]:
            temp_sub_feature_and_score_dict = {}
            for each_sub_element in each_element["feature_responses"]:
                if "mean" in each_sub_element:
                    temp_sub_feature_and_score_dict[each_sub_element["name"]] = each_sub_element["mean"]
                else:
                    temp_sub_feature_and_score_dict[each_sub_element["name"]] = 0
            if "score" in each_element:
                DPS_configured_sections[each_element["name"]] = {"mean": each_element["score"], 
                    "sub-features": temp_sub_feature_and_score_dict}
            else:
                DPS_configured_sections[each_element["name"]] = {"mean": 0, 
                    "sub-features": temp_sub_feature_and_score_dict}
        return DPS_configured_sections
