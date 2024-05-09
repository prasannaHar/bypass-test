import logging
import math
import pandas as pd


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TrellisScoresHelper:

    def trellis_metric_score_calculation(self, metric_actual_name,metric_value, 
                        profile_metric_name, metric_profile_max_value, 
                        metric_profile_upper_limit=None, metric_profile_lower_limit=None,
                        week_interval=None):
        if (float(metric_value) % 1) >= 0.5:
            metric_value = math.ceil(metric_value)
        else:
            metric_value = round(metric_value)
        metric_value = round(metric_value)
        metric_score_calc = 0
        max_ref_multiplication_factor = 10
        max_score = 90
        max_ref_value = metric_profile_max_value * max_ref_multiplication_factor
        ## metric type indentifier
        timing_metric_flag = False
        if profile_metric_name in ["Average PR approval time", "Average PR comment time","Average Coding days per week" ,
                    "Average PR Cycle Time", "Average time spent working on Issues", "Average response time for PR approvals", 
                    "Average response time for PR comments"]:
            timing_metric_flag = True
        ## metric performance identifier
        bad_performance_flag = False
        if profile_metric_name in ["Average PR approval time", "Average PR comment time", "Percentage of Rework", 
                    "Percentage of Legacy Rework", "Average PR Cycle Time", 
                    "Average time spent working on Issues", "Average response time for PR approvals", 
                    "Average response time for PR comments"]:
            bad_performance_flag = True
        ## metric value extrapolation for the score calculation in case LAST_WEEK & LAST_TWO_WEEKS intervals
        if ( (week_interval != None) and 
                ("week" in metric_actual_name) and 
                (metric_actual_name not in ["Average Coding days per week"])):
            if week_interval == "LAST_WEEK":
                metric_value = metric_value * 4
            elif week_interval == "LAST_TWO_WEEKS":
                metric_value = metric_value * 2
        ## metric score calculation
        if timing_metric_flag:
            max_ref_value = max_ref_value * 86400
            metric_profile_max_value = metric_profile_max_value * 86400
            if not bad_performance_flag:
                if metric_value == 0:
                    metric_score_calc = 0
                elif metric_value <= metric_profile_max_value:
                    metric_score_calc = min(100, int((metric_value*max_score)/metric_profile_max_value))
                elif metric_value >= max_ref_value:
                    metric_score_calc = 99
                else:
                    additional_count_pct = metric_value * 100 / max_ref_value
                    add_on_pct = int(additional_count_pct * max_ref_multiplication_factor / 100)
                    metric_score_calc = max_score + add_on_pct
            else:
                if metric_value == 0:
                    metric_score_calc = max_score
                else:
                    metric_score_calc = 100 - (min(100, int((metric_value*100)/metric_profile_max_value)))
        else:
            if not bad_performance_flag:
                if metric_value == 0:
                    metric_score_calc = 0
                elif metric_value <= metric_profile_max_value:
                    metric_score_calc = min(max_score, int((metric_value*max_score)/metric_profile_max_value))
                elif metric_value >= max_ref_value:
                    metric_score_calc = 99
                else:
                    additional_count_pct = metric_value * 100 / max_ref_value
                    add_on_pct = int(additional_count_pct * max_ref_multiplication_factor / 100)
                    metric_score_calc = max_score + add_on_pct
            else:
                if metric_value == 0:
                    metric_score_calc = max_score
                else:
                    metric_score_calc = 100 - (min(100, int( (metric_value*100)/metric_profile_max_value )))
        return metric_score_calc

