import logging
import datetime

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TrellisMetricsHelper:

    def trellis_calculate_average(self, drilldown_data, months):
        """ Trellis Metrics:
            Number of PRs per month,
            Number of Commits per month,
            Number of PRs approved per month
            Number of PRs commented on per month
        """
        drilldown_records_count =  drilldown_data["count"] 
        average_metric_value = drilldown_records_count/months
        return  round(average_metric_value, 2)


    def trellis_calculate_ticket_portion(self, drilldown_data, months):
        """ Trellis Metrics:
            High Impact bugs worked on per month,
            High Impact stories worked on per month,
            Number of bugs worked on per month,
            Number of stories worked on per month
        """
        total_approval_time = 0
        for each_drill_down_record in drilldown_data["records"]:
            total_approval_time = total_approval_time + each_drill_down_record["ticket_portion"]
        average_metric_value = total_approval_time/months
        return round(average_metric_value, 2)


    def trellis_calculate_story_points_portion(self, drilldown_data, months):
        """ Trellis Metrics:
            Number of Story Points worked on per month
        """
        total_approval_time = 0
        for each_drill_down_record in drilldown_data["records"]:
            if "story_points_portion" in each_drill_down_record:
                total_approval_time = total_approval_time + each_drill_down_record["story_points_portion"]
            elif "story_point_portion" in each_drill_down_record:
                total_approval_time = total_approval_time +each_drill_down_record["story_point_portion"]
        average_metric_value = total_approval_time/months
        return round(average_metric_value, 2)


    def trellis_calculate_response_time(self, drilldown_data, tag_name):
        """ Trellis Metrics: 
            Average response time for PR approvals, 
            Average response time for PR comments,
            Average time spent working on Issues
        """
        drilldown_records_count =  drilldown_data["count"] 
        total_approval_time = 0
        for each_drill_down_record in drilldown_data["records"]:
            total_approval_time = total_approval_time + each_drill_down_record[tag_name]
        return total_approval_time/drilldown_records_count


    def trellis_calculate_coding_days(self, drilldown_data, months, interval):
        """ Trellis Metrics: 
            Average Coding days per week
        """
        temp_commit_dates_list =[]
        for each_drill_down_record in drilldown_data["records"]:
            current_timestamp = int(datetime.datetime.now().strftime("%s"))
            committed_at = each_drill_down_record["committed_at"]
            if committed_at > current_timestamp:
                committed_at = committed_at/1000
            temp_commit_datetime =datetime.datetime.fromtimestamp(committed_at)-datetime.timedelta(minutes=330)
            temp_commitdate = datetime.date(temp_commit_datetime.year, temp_commit_datetime.month, temp_commit_datetime.day)
            temp_commit_dates_list.append(temp_commitdate)        
        temp_commit_dates_list = set(temp_commit_dates_list)
        temp_commit_dates_list = list(temp_commit_dates_list)
        time_period_in_weeks = float(months*30)/7
        if months==1: time_period_in_weeks = 4
        if interval == "LAST_TWO_WEEKS":
            time_period_in_weeks = 2
        elif interval == "LAST_WEEK":
            time_period_in_weeks = 1
        average_metric_value = len(temp_commit_dates_list)/time_period_in_weeks
        # average_metric_value = round(average_metric_value,0)
        return average_metric_value*86400
    

    def trellis_calculate_pr_cycle_time(self, drilldown_data):
        """ Trellis Metrics: 
            Average PR Cycle Time
        """
        drilldown_records_count =  drilldown_data["count"] 
        total_approval_time = 0
        for each_drill_down_record in drilldown_data["records"]:
            temp_pr_closed_time = each_drill_down_record["pr_closed_at"]
            temp_pr_create_time = each_drill_down_record["pr_created_at"]
            time_pr_duration = float(temp_pr_closed_time) - float(temp_pr_create_time)
            total_approval_time = total_approval_time + time_pr_duration
        average_metric_value = total_approval_time/drilldown_records_count
        return round(average_metric_value,1)


    def trellis_calculate_rework(self, drilldown_data, legacy_rework=False):
        """ Trellis Metrics: 
            Percentage of Rework
            Percentage of Legacy Rework
        """
        tag_name = "total_refactored_lines"
        if legacy_rework:
            tag_name = "total_legacy_lines"
        total_legacy_code_lines = 0
        total_lines = 0
        for each_drill_down_record in drilldown_data["records"]:
            total_legacy_code_lines = total_legacy_code_lines + each_drill_down_record[tag_name]
            total_lines = total_lines + each_drill_down_record["tot_lines_added"] + each_drill_down_record["tot_lines_changed"] +each_drill_down_record["tot_lines_removed"]
        average_metric_value = 0
        if total_lines != 0: average_metric_value = total_legacy_code_lines*100/total_lines
        return round(average_metric_value, 2)


    def trellis_calculate_lines_of_code(self, drilldown_data, months):
        """ Trellis Metrics: 
            Lines of Code per month
        """
        total_approval_time = 0
        for each_drill_down_record in drilldown_data["records"]:
            total_approval_time = total_approval_time + each_drill_down_record["changes"] + each_drill_down_record["additions"]
        average_metric_value = total_approval_time/months
        return round(average_metric_value, 2)


    def trellis_calculate_repo_breadth(self, drilldown_data, tech_breadth=False):
        """ Trellis Metrics: 
            Technical Breadth - Number of unique file extension
            Repo Breadth - Number of unique repo
        """
        tag_name = "repo_breadth"
        if tech_breadth:
            tag_name = "tech_breadth"
        complete_repo_breadth_details = []
        for each_drill_down_record in drilldown_data["records"]:
            complete_repo_breadth_details = complete_repo_breadth_details + each_drill_down_record[tag_name]
        complete_repo_breadth_details = set(complete_repo_breadth_details)
        complete_repo_breadth_details = list(complete_repo_breadth_details)
        return len(complete_repo_breadth_details)

