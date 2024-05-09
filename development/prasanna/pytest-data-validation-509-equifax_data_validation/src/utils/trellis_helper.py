import logging
import pandas as pd

from src.utils.trellis_metrics_helper import TrellisMetricsHelper
from src.utils.trellis_scores_helper import TrellisScoresHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.lib.core_reusable_functions import *


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TrellisHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.baseurl = self.generic.connection["base_url"]
        self.metrics_helper = TrellisMetricsHelper()
        self.scores_helper = TrellisScoresHelper()
        self.widget_helper = TestWidgetHelper(generic_helper)

    def retrieve_trellis_user_report(self, ou_user_id, trellis_profile, interval, 
                        force_source=None, on_fly_calc=None, user_type="ADMIN_DPS"):
        no_of_months, gt, lt = epoch_timeStampsGenerationForRequiredTimePeriods(interval)
        url = self.baseurl + "dev_productivity/reports/fixed_intervals/users/ou_user_ids/" + ou_user_id
        payload = {"page": 0,"page_size": 100,"filter": {
            "interval": interval,
            "dev_productivity_profile_id": trellis_profile}}
        if on_fly_calc:
            url = self.baseurl + "dev_productivity/reports/users?there_is_no_cache=true"
            filters = {"user_id_type": "ou_user_ids","user_id_list": [ou_user_id],
                    "time_range": {"$gt": gt,"$lt": lt},
                    "dev_productivity_profile_id": trellis_profile}
            if force_source:
                filters["force_source"] = force_source
            payload = {"page": 0,"page_size": 10,"sort": [],"filter": filters }
        response_json_format = self.generic.rbac_user(url, "post", data=payload, user_type=user_type)
        if on_fly_calc:
            return (response_json_format["records"])[0]
        return response_json_format


    def retrieve_trellis_user_report_drilldown(self, ou_user_id, interval, feature_name, 
                            user_type="ADMIN_DPS", trelis_profile=None, 
                            force_source=None):
        no_of_months, gt, lt = epoch_timeStampsGenerationForRequiredTimePeriods(interval)
        url = self.baseurl + "dev_productivity/reports/feature_details"
        if force_source:
            url = url + "?there_is_no_cache=true&force_source=" + force_source
        filters = {"user_id_type": "ou_user_ids",
            "user_id_list": [ou_user_id],"feature_name": feature_name,
            "time_range": {"$gt": gt,"$lt": lt},"partial": {},"interval":interval }
        if trelis_profile:
            filters["dev_productivity_profile_id"] = trelis_profile
        payload = {"page": 0,"page_size": 10000,
            "sort": [],"filter": filters,"across": ""}
        response = self.generic.rbac_user(url, "post", data=payload, user_type=user_type)
        return response


    def trellis_drilldown_metric_value_calculation(self, arg_drill_down_response, 
                                    arg_metric_name, arg_interval, raw_stats=None):
        no_of_months, gt, lt = epoch_timeStampsGenerationForRequiredTimePeriods(arg_interval)
        try:
            if raw_stats:
                no_of_months = 1
            drill_records_records_section = ( ( (arg_drill_down_response['records'])[0] ))
            drill_down_records_count = 0
            if "count" in drill_records_records_section:
                drill_down_records_count =  drill_records_records_section["count"] 
            else: return drill_down_records_count

            if drill_down_records_count==0: return 0 

            if arg_metric_name in ["Number of PRs per month","Number of Commits per month", 
                        "Number of PRs approved per month","Number of PRs commented on per month"]:
                return self.metrics_helper.trellis_calculate_average(
                                drilldown_data = drill_records_records_section, 
                                months = no_of_months)
            elif arg_metric_name in ["High Impact bugs worked on per month", "High Impact stories worked on per month",
                            "Number of bugs worked on per month", "Number of stories worked on per month"]:
                return self.metrics_helper.trellis_calculate_ticket_portion(
                                drilldown_data = drill_records_records_section, 
                                months = no_of_months)
            elif arg_metric_name in ["Number of Story Points worked on per month"]:
                return self.metrics_helper.trellis_calculate_story_points_portion(
                                drilldown_data = drill_records_records_section, 
                                months = no_of_months)
            elif arg_metric_name in ["Average response time for PR approvals"]:
                return self.metrics_helper.trellis_calculate_response_time(
                                drilldown_data = drill_records_records_section, 
                                tag_name = "approval_time")
            elif arg_metric_name == "Average response time for PR comments":
                return self.metrics_helper.trellis_calculate_response_time(
                                drilldown_data = drill_records_records_section, 
                                tag_name = "comment_time")
            elif arg_metric_name == "Average time spent working on Issues":
                return self.metrics_helper.trellis_calculate_response_time(
                                drilldown_data = drill_records_records_section, 
                                tag_name = "assignee_time")
            elif arg_metric_name == "Average Coding days per week":
                return self.metrics_helper.trellis_calculate_coding_days(
                                drilldown_data = drill_records_records_section, 
                                months = no_of_months, interval=arg_interval)
            elif arg_metric_name == "Average PR Cycle Time":
                return self.metrics_helper.trellis_calculate_pr_cycle_time(
                                drilldown_data = drill_records_records_section)
            elif arg_metric_name == "Percentage of Rework":
                return self.metrics_helper.trellis_calculate_rework(
                                drilldown_data = drill_records_records_section,
                                legacy_rework=False)
            elif arg_metric_name == "Percentage of Legacy Rework":
                return self.metrics_helper.trellis_calculate_rework(
                                drilldown_data = drill_records_records_section,
                                legacy_rework=True)
            elif arg_metric_name == "Lines of Code per month":
                return self.metrics_helper.trellis_calculate_lines_of_code(
                                drilldown_data = drill_records_records_section, 
                                months = no_of_months)
            elif arg_metric_name == "Technical Breadth - Number of unique file extension":
                return self.metrics_helper.trellis_calculate_repo_breadth(
                                drilldown_data = drill_records_records_section,
                                tech_breadth=True)
            elif arg_metric_name == "Repo Breadth - Number of unique repo":
                return self.metrics_helper.trellis_calculate_repo_breadth(
                                drilldown_data = drill_records_records_section,
                                tech_breadth=False)
            else: return 0
        except Exception as ex:
            LOG.error("failure due to {}".format(ex))
            return 0

    def trellis_retrieve_contributor_ids(self, collection_id, interval="LAST_MONTH"):
        ## retrieve collection associated contributors list
        columns = self.generic.api_data["trellis_score_user_columns"]
        filters = {"ou_ref_ids": [collection_id], "interval": interval}
        response_data = self.widget_helper.create_trellis_score_report(filters=filters)
        response_data = response_data["records"]
        response_df = pd.json_normalize(response_data, "section_responses", columns,
                                        record_prefix="section_response.")
        return (response_df.org_user_id.unique()).tolist()
