import logging
import re
import pandas as pd 

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TrellisProfileHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.baseurl = self.generic.connection["base_url"]


    def retrieve_trellis_profile_response(self, trellis_profile):
        ## retrieve trellis profile settings
        url = self.baseurl + (self.generic.get_api_info())["dev_prod_profile"] + "/" + trellis_profile
        response = self.generic.execute_api_call(url, "get")
        return response


    def retreive_trellis_profile_metric_name(self, metric_name, interval):
        if (interval in ["LAST_WEEK", "LAST_TWO_WEEKS"]) and ("week" in metric_name) and ("days" not in metric_name):
            split_string_by_multiple_delimiters = lambda input_string, delimiters: re.split('|'.join(map(re.escape, delimiters)), input_string)
            delimiters = [" in "]
            str_identifier = split_string_by_multiple_delimiters(metric_name, delimiters)[1]
            return metric_name.replace(" in "+str_identifier,' per month')
        return metric_name


    def trellis_profile_retreive_metric_settings(self, trellis_profile, metric_name, interval):
        trellis_profile_response = self.retrieve_trellis_profile_response(
                                            trellis_profile=trellis_profile)
        # Extract max_value, lower_limit_percentage and upper_limit_percentage using json_normalize
        features_data = []
        for section in trellis_profile_response['sections']:
            features_data += section['features']
        df = pd.json_normalize(features_data)
        df = df[['name', 'max_value', 'lower_limit_percentage', 'upper_limit_percentage']]
        if interval:
            metric_name = self.retreive_trellis_profile_metric_name(metric_name=metric_name, interval=interval)
        df_filtered = df[df["name"]==metric_name]
        return (df_filtered.values.tolist())[0]


    def trellis_profile_retrieve_enabled_factors_and_metrics(self, trellis_profile):
        response = self.retrieve_trellis_profile_response(trellis_profile=trellis_profile)
        retrieve_available_sections = response["sections"]
        enabled_sections = []
        enabled_features = []
        for eachSection in retrieve_available_sections:
            sectionenabled_status = eachSection["enabled"]
            if sectionenabled_status:
                enabled_sections.append(eachSection["name"])
                available_features = eachSection["features"]
                for eachAvailableFeature in available_features:
                    featureenabled_status = eachAvailableFeature["enabled"]
                    if featureenabled_status:
                        enabled_features.append(eachSection["name"] + "_" + eachAvailableFeature["name"])
        return enabled_sections, enabled_features

