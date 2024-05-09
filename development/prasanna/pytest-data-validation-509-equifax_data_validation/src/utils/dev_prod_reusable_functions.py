from email.errors import FirstHeaderLineIsContinuationDefect
from multiprocessing import AuthenticationError
from time import time
from traceback import print_tb
import requests
import json
import pandas as pd 
import os
import pytest
import logging
import re
import math

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGHelper
from src.lib.html_auto_code_generators import *
from src.lib.core_reusable_functions import *
from src.lib.file_operations import *
from src.lib.sqlite3_db_operations import *
from src.lib.automatic_mail_sender import *


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

helper_object = TGHelper()

def retrieve_dev_user_profile_api_response(application_url, ou_user_id, ou_associated_trelis_profile_id, 
                                           timeperiod_interval, force_source=None, on_fly_calc=None, 
                                           user_type="ADMIN_DPS"):
    no_of_months, gt, lt = epoch_timeStampsGenerationForRequiredTimePeriods(timeperiod_interval)
    url = application_url + "dev_productivity/reports/fixed_intervals/users/ou_user_ids/" + ou_user_id
    payload = {"page": 0,"page_size": 100,"filter": {
        "interval": timeperiod_interval,
        "dev_productivity_profile_id": ou_associated_trelis_profile_id}}
    if on_fly_calc:
        url = application_url + "dev_productivity/reports/users?there_is_no_cache=true"
        filters = {"user_id_type": "ou_user_ids","user_id_list": [ou_user_id],
                "time_range": {"$gt": gt,"$lt": lt},
                "dev_productivity_profile_id": ou_associated_trelis_profile_id}
        if force_source:
            filters["force_source"] = force_source
        payload = {"page": 0,"page_size": 10,"sort": [],"filter": filters }
    helper_object = TGHelper()
    response_json_format = helper_object.rbac_user(url, "post", data=payload, user_type=user_type)
    if on_fly_calc:
        return (response_json_format["records"])[0]
    return response_json_format


def retrieve_drilldown_api_response(application_url, ou_user_id,interval, feature_name , 
                                    user_type="ADMIN_DPS",ou_associated_trelis_profile=None, force_source=None):
    no_of_months, gt, lt = epoch_timeStampsGenerationForRequiredTimePeriods(interval)
    url = application_url + "dev_productivity/reports/feature_details"
    if force_source:
        url = url + "?there_is_no_cache=true&force_source=" + force_source
    filters = {"user_id_type": "ou_user_ids",
        "user_id_list": [ou_user_id],"feature_name": feature_name,
        "time_range": {"$gt": gt,"$lt": lt},"partial": {},"interval":interval }
    if ou_associated_trelis_profile:
        filters["dev_productivity_profile_id"] = ou_associated_trelis_profile
    payload = {"page": 0,"page_size": 10000,
        "sort": [],"filter": filters,"across": ""}
    helper_object = TGHelper()
    response = helper_object.rbac_user(url, "post", data=payload, user_type=user_type)
    return response


def widget_value_calculation_from_drilldown(arg_drill_down_response, arg_metric_name, arg_interval, raw_stats=None):
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

        if arg_metric_name=="Number of PRs per month" or arg_metric_name=="Number of Commits per month" or \
            arg_metric_name=="Number of PRs approved per month" or arg_metric_name=="Number of PRs commented on per month":
            average_metric_value = drill_down_records_count/no_of_months
            average_metric_value = round(average_metric_value, 2)
            avg_logic_calculation_type_FLG = False
            return average_metric_value       

        elif arg_metric_name=="High Impact bugs worked on per month" or arg_metric_name=="High Impact stories worked on per month" or\
            arg_metric_name == "Number of bugs worked on per month" or arg_metric_name == "Number of stories worked on per month":
            avg_logic_calculation_type_FLG = True
            total_approval_time = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                total_approval_time = total_approval_time + each_drill_down_record["ticket_portion"]
            average_metric_value = total_approval_time/no_of_months
            average_metric_value = round(average_metric_value, 2)
            return average_metric_value

        elif arg_metric_name == "Number of Story Points worked on per month":
            total_approval_time = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                if "story_points_portion" in each_drill_down_record:
                    total_approval_time = total_approval_time + each_drill_down_record["story_points_portion"]
                elif "story_point_portion" in each_drill_down_record:
                    total_approval_time = total_approval_time +each_drill_down_record["story_point_portion"]
            average_metric_value = total_approval_time/no_of_months
            average_metric_value = round(average_metric_value, 2)
            return average_metric_value

        elif arg_metric_name == "Average response time for PR approvals":
            total_approval_time = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                total_approval_time = total_approval_time + each_drill_down_record["approval_time"]
            average_metric_value = total_approval_time/drill_down_records_count
            return average_metric_value

        elif arg_metric_name == "Average response time for PR comments":
            total_approval_time = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                total_approval_time = total_approval_time + each_drill_down_record["comment_time"]
            average_metric_value = total_approval_time/drill_down_records_count
            return average_metric_value

        elif arg_metric_name == "Average time spent working on Issues":
            total_approval_time = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                total_approval_time = total_approval_time + each_drill_down_record["assignee_time"]
            average_metric_value = total_approval_time/drill_down_records_count
            return average_metric_value

        elif arg_metric_name == "Average Coding days per week":
            total_approval_time = 0
            temp_commit_dates_list =[]
            import datetime
            for each_drill_down_record in drill_records_records_section["records"]:
                current_timestamp = int(datetime.datetime.now().strftime("%s"))
                committed_at = each_drill_down_record["committed_at"]
                if committed_at > current_timestamp:
                    committed_at = committed_at/1000
                temp_commit_datetime =datetime.datetime.fromtimestamp(committed_at)-datetime.timedelta(minutes=330)
                temp_commitdate = datetime.date(temp_commit_datetime.year, temp_commit_datetime.month, temp_commit_datetime.day)
                temp_commit_dates_list.append(temp_commitdate)        
            temp_commit_dates_list = set(temp_commit_dates_list)
            temp_commit_dates_list = list(temp_commit_dates_list)
            time_period_in_weeks = float(no_of_months*30)/7
            if no_of_months==1: time_period_in_weeks = 4
            if arg_interval == "LAST_TWO_WEEKS":
                time_period_in_weeks = 2
            elif arg_interval == "LAST_WEEK":
                time_period_in_weeks = 1
            average_metric_value = len(temp_commit_dates_list)/time_period_in_weeks
            # average_metric_value = round(average_metric_value,0)
            average_metric_value = average_metric_value*86400
            return average_metric_value

        elif arg_metric_name == "Average PR Cycle Time":
            total_approval_time = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                temp_pr_closed_time = each_drill_down_record["pr_closed_at"]
                temp_pr_create_time = each_drill_down_record["pr_created_at"]
                time_pr_duration = float(temp_pr_closed_time) - float(temp_pr_create_time)
                total_approval_time = total_approval_time + time_pr_duration
            average_metric_value = total_approval_time/drill_down_records_count
            average_metric_value = round(average_metric_value,1)
            return average_metric_value

        elif arg_metric_name == "Percentage of Rework":
            total_legacy_code_lines = 0
            total_lines = 0
            total_percentage_of_legacy_rework = 0
            temp_count = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                temp_total_number_of_lines_changed = (each_drill_down_record["tot_lines_added"] + each_drill_down_record["tot_lines_changed"] +each_drill_down_record["tot_lines_removed"])
                temp_calculate_percetnage_of_rework = 0
                if temp_total_number_of_lines_changed != 0:
                    temp_calculate_percetnage_of_rework = (each_drill_down_record["total_refactored_lines"]*100)/temp_total_number_of_lines_changed
                    temp_count = temp_count + 1
                total_legacy_code_lines = total_legacy_code_lines + each_drill_down_record["total_refactored_lines"]
                total_lines = total_lines + each_drill_down_record["tot_lines_added"] + each_drill_down_record["tot_lines_changed"] +each_drill_down_record["tot_lines_removed"]
                total_percentage_of_legacy_rework = total_percentage_of_legacy_rework + temp_calculate_percetnage_of_rework
            average_metric_value = 0
            if total_lines != 0: average_metric_value = total_legacy_code_lines*100/total_lines
            percentage_of_legacy_rework = round(average_metric_value, 2)
            return percentage_of_legacy_rework

        elif arg_metric_name == "Percentage of Legacy Rework":
            total_legacy_code_lines = 0
            total_lines = 0
            total_percentage_of_legacy_rework = 0
            temp_count = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                temp_total_number_of_lines_changed = (each_drill_down_record["tot_lines_added"] + each_drill_down_record["tot_lines_changed"] +each_drill_down_record["tot_lines_removed"])
                temp_calculate_percetnage_of_rework = 0
                if temp_total_number_of_lines_changed != 0:
                    temp_calculate_percetnage_of_rework = (each_drill_down_record["total_legacy_lines"]*100)/temp_total_number_of_lines_changed
                    temp_count = temp_count + 1
                total_legacy_code_lines = total_legacy_code_lines + each_drill_down_record["total_legacy_lines"]
                total_lines = total_lines + each_drill_down_record["tot_lines_added"] + each_drill_down_record["tot_lines_changed"] +each_drill_down_record["tot_lines_removed"]
                total_percentage_of_legacy_rework = total_percentage_of_legacy_rework + temp_calculate_percetnage_of_rework
            percentage_of_legacy_rework = 0
            if total_lines != 0:  percentage_of_legacy_rework = total_legacy_code_lines*100/total_lines
            percentage_of_legacy_rework = round(percentage_of_legacy_rework, 2)
            return percentage_of_legacy_rework

        elif arg_metric_name == "Lines of Code per month":
            total_approval_time = 0
            for each_drill_down_record in drill_records_records_section["records"]:
                total_approval_time = total_approval_time + each_drill_down_record["changes"] + each_drill_down_record["additions"]
            average_metric_value = total_approval_time/no_of_months
            average_metric_value = round(average_metric_value, 2)
            return average_metric_value

        elif arg_metric_name == "Technical Breadth - Number of unique file extension":
            complete_repo_breadth_details = []
            for each_drill_down_record in drill_records_records_section["records"]:
                complete_repo_breadth_details = complete_repo_breadth_details + each_drill_down_record["tech_breadth"]
            complete_repo_breadth_details = set(complete_repo_breadth_details)
            complete_repo_breadth_details = list(complete_repo_breadth_details)
            number_unique_repo_details = len(complete_repo_breadth_details)
            return number_unique_repo_details

        elif arg_metric_name == "Repo Breadth - Number of unique repo":
            complete_repo_breadth_details = []
            for each_drill_down_record in drill_records_records_section["records"]:
                complete_repo_breadth_details = complete_repo_breadth_details + each_drill_down_record["repo_breadth"]
            complete_repo_breadth_details = set(complete_repo_breadth_details)
            complete_repo_breadth_details = list(complete_repo_breadth_details)
            number_unique_repo_details = len(complete_repo_breadth_details)
            return number_unique_repo_details

        else: return 0
    
    except Exception as ex:
        LOG.error("failure due to {}".format(ex))
        return 0


def dev_prod_result_analysis_and_mailer(arg_dps_metrics_result_DF, arg_dps_test_cases,arg_ou_users_considered, arg_env_name, arg_tenant_name, include_reports=None, 
                                        sender_email=None,target_emails=None, accesscode=None ):
    arg_dps_metrics_result_DF = arg_dps_metrics_result_DF.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False)
    DPS_Metrics_Result_DF_Failed = arg_dps_metrics_result_DF.loc[arg_dps_metrics_result_DF['Status'] == "Fail"]
    arg_dps_metrics_result_DF.to_csv("../result/complete_details.csv", index=False)
    if len(DPS_Metrics_Result_DF_Failed)!=0:
        DPS_Metrics_Result_DF_Failed.to_csv("../result/failure_details.csv", index=False)
    DPS_Metrics_Result_DF_Failed_List = DPS_Metrics_Result_DF_Failed.values.tolist()
    DF_Column_names = DPS_Metrics_Result_DF_Failed.columns.to_list()
    mailer_content = ""
    html_start_content, html_end_content = html_start_and_end_content_retrieve()
    mailer_content = mailer_content + html_start_content
    mailer_content = mailer_content + """<p>Sharing the Dev Productivity - Data validation test details, </p>"""

    ## DPS test case pass/fail analysis
    dps_test_cases_list = arg_dps_test_cases
    dps_test_cases_list_final = []
    dps_test_cases_pass_count = 0
    for each_dps_test_case_elem in dps_test_cases_list:
        if each_dps_test_case_elem[1] == "Pass":
            dps_test_cases_pass_count = dps_test_cases_pass_count + 1
        temp_feature_and_sub_feature_details = (each_dps_test_case_elem[0]).split("_")
        dps_test_cases_list_final.append([temp_feature_and_sub_feature_details[0],temp_feature_and_sub_feature_details[1], each_dps_test_case_elem[1]])
    dps_test_cases_fail_count = len(dps_test_cases_list) - dps_test_cases_pass_count

    ## test cases overview table html code generation
    overview_table_column_names =  ["#OU users considered",	"#Test cases executed",	"#Test cases passed",	"#Test cases failed"]
    overview_table_data = [ [arg_ou_users_considered, len(dps_test_cases_list), dps_test_cases_pass_count, dps_test_cases_fail_count ] ]
    overview_table_html_code = html_dynamic_table_generation(
        arg_table_headers=overview_table_column_names,
        arg_table_data=overview_table_data)
    mailer_content = mailer_content + overview_table_html_code

    ##test scenarios overview table html code generation
    mailer_content = mailer_content + """<p> <b> Test-scenarios results overview:</b> </p>"""
    test_scenario_table_column_names = [ "Feature", "Test scenario", "Result"]
    test_scenario_table_data = dps_test_cases_list_final
    test_scenario_table_html_code = html_dynamic_table_generation(
        arg_table_headers=test_scenario_table_column_names,
        arg_table_data=test_scenario_table_data)
    mailer_content = mailer_content + test_scenario_table_html_code

    ##failure details table html code generation
    if len(DPS_Metrics_Result_DF_Failed) >=1:
        mailer_content = mailer_content + """<p style="color:red;"> <b>Failure details: <b></p><br>"""
        failure_details_table_column_names = DF_Column_names
        failure_details_table_data = DPS_Metrics_Result_DF_Failed_List
        failure_details_table_html_code = html_dynamic_table_generation(
            arg_table_headers=failure_details_table_column_names,
            arg_table_data=failure_details_table_data)
        mailer_content = mailer_content + failure_details_table_html_code
    else:
        mailer_content = mailer_content + "<p>No failures are observed</p><br>"

    mailer_content = mailer_content + "Complete Test details and Failure details are attached in email."
    mailer_content = mailer_content + html_end_content

    from datetime import datetime
    now = datetime.now()
    TodaysDateGenerated = now.strftime("%d-%m-%Y %H:%M:%S")
    mail_documents_need_to_be_attached = ['../result/complete_details.csv', '../result/failure_details.csv']
    if(len(DPS_Metrics_Result_DF_Failed) == 0 ):
        mail_documents_need_to_be_attached = ['../result/complete_details.csv']

    if include_reports:
        mail_documents_need_to_be_attached.append("../result/result.xml")
        mail_documents_need_to_be_attached.append("../result/report.html")


    automatic_mail_using_gmail_fun(
        arg_sender = sender_email,
        arg_req_email_address=target_emails,
        arg_subject= arg_env_name + "_" + arg_tenant_name + ": " + 'Dev Productivity - Data validation based on widget and drill-down values ' + str(TodaysDateGenerated),
        arg_mail_content=mailer_content,
        arg_required_attachments=mail_documents_need_to_be_attached,
        arg_smtp_access_code=accesscode)

def trellis_profile_metric_name_retriever(metric_name, interval):
    if (interval in ["LAST_WEEK", "LAST_TWO_WEEKS"]) and ("week" in metric_name) and ("days" not in metric_name):
        split_string_by_multiple_delimiters = lambda input_string, delimiters: re.split('|'.join(map(re.escape, delimiters)), input_string)
        delimiters = [" in "]
        str_identifier = split_string_by_multiple_delimiters(metric_name, delimiters)[1]
        return metric_name.replace(" in "+str_identifier,' per month')
    return metric_name

def trellis_metric_settings_retriever(profile_settings, metric_name, interval):
    # Extract max_value, lower_limit_percentage and upper_limit_percentage using json_normalize
    features_data = []
    for section in profile_settings['sections']:
        features_data += section['features']
    df = pd.json_normalize(features_data)
    df = df[['name', 'max_value', 'lower_limit_percentage', 'upper_limit_percentage']]
    if interval:
        metric_name = trellis_profile_metric_name_retriever(metric_name=metric_name, interval=interval)
    df_filtered = df[df["name"]==metric_name]
    return (df_filtered.values.tolist())[0]
    

def trellis_metric_score_validation(metric_name, metric_value, metric_score, profile_settings, week_interval=None):
    if (float(metric_value) % 1) >= 0.5:
        metric_value = math.ceil(metric_value)
    else:
        metric_value = round(metric_value)
    metric_value = round(metric_value)
    metric_score_calc = 0
    max_ref_multiplication_factor = 10
    max_score = 90
    ## metric settings retriever
    metric_settings_retriever = trellis_metric_settings_retriever(
                                    profile_settings=profile_settings,
                                    metric_name=metric_name, interval=week_interval)
    name = metric_settings_retriever[0]
    max_value = metric_settings_retriever[1]
    ll = metric_settings_retriever[2]
    ul = metric_settings_retriever[3]
    max_ref_value = max_value * max_ref_multiplication_factor
    ## metric type indentifier
    timing_metric_flag = False
    if name in ["Average PR approval time", "Average PR comment time","Average Coding days per week" ,
                "Average PR Cycle Time", "Average time spent working on Issues", "Average response time for PR approvals", 
                "Average response time for PR comments"]:
        timing_metric_flag = True
    ## metric performance identifier
    bad_performance_flag = False
    if name in ["Average PR approval time", "Average PR comment time", "Percentage of Rework", 
                "Percentage of Legacy Rework", "Average PR Cycle Time", 
                "Average time spent working on Issues", "Average response time for PR approvals", 
                "Average response time for PR comments"]:
        bad_performance_flag = True
    ## metric value extrapolation for the score calculation in case LAST_WEEK & LAST_TWO_WEEKS intervals
    if ( (week_interval != None) and 
            ("week" in metric_name) and 
            (metric_name not in ["Average Coding days per week"])):
        if week_interval == "LAST_WEEK":
            metric_value = metric_value * 4
        elif week_interval == "LAST_TWO_WEEKS":
            metric_value = metric_value * 2
    ## metric score calculation
    if timing_metric_flag:
        max_ref_value = max_ref_value * 86400
        max_value = max_value * 86400
        if not bad_performance_flag:
            if metric_value == 0:
                metric_score_calc = 0
            elif metric_value <= max_value:
                metric_score_calc = min(100, int((metric_value*max_score)/max_value))
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
                metric_score_calc = 100 - (min(100, int((metric_value*100)/max_value)))
    else:
        if not bad_performance_flag:
            if metric_value == 0:
                metric_score_calc = 0
            elif metric_value <= max_value:
                metric_score_calc = min(max_score, int((metric_value*max_score)/max_value))
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
                metric_score_calc = 100 - (min(100, int( (metric_value*100)/max_value )))
    ## validating the score
    return (-1 <= metric_score-metric_score_calc <=1)


def dev_prod_results_generation(arg_ou_user_ids_list, arg_ou_user_time_periods, arg_application_url, 
                        arg_dp_temp_db_file_name, arg_temp_db_file_name_tcresults, 
                        arg_temp_db_file_name_tcresultdata, on_fly_calc=None, force_source=None):
    LoopFirstTimeFlag = True
    if os.path.exists(arg_dp_temp_db_file_name):
        dps_tc_results = sqlite3_database_retrieve_table_data(
            arg_db_file_name=arg_dp_temp_db_file_name,
            arg_table_name=arg_temp_db_file_name_tcresults)
        if len(dps_tc_results) > 0: LoopFirstTimeFlag = False
    dps_test_cases = {}
    DPS_Metrics_Result_List = []
    for each_ou_user_details in arg_ou_user_ids_list:
        for each_time_interval in arg_ou_user_time_periods:
            week_interval_val = None
            if each_time_interval in ["LAST_WEEK", "LAST_TWO_WEEKS"]:
                week_interval_val = each_time_interval
            each_ou_user_details_temp = each_ou_user_details.split(":")
            each_ou_user, each_ou_user_trelis_profile = each_ou_user_details_temp[0], each_ou_user_details_temp[1]
            ## retrieve trellis profile settings
            dev_prod_profile_api_url = helper_object.connection["base_url"] + (helper_object.get_api_info())["dev_prod_profile"] + "/" + each_ou_user_trelis_profile
            dev_prod_profile_response = helper_object.execute_api_call(dev_prod_profile_api_url, "get") ##
            ou_user_id = each_ou_user
            timeperiod_interval = each_time_interval
            dev_productivity_user_overview = retrieve_dev_user_profile_api_response(
                application_url=arg_application_url, 
                ou_user_id=ou_user_id,
                ou_associated_trelis_profile_id=each_ou_user_trelis_profile, 
                timeperiod_interval=timeperiod_interval,
                on_fly_calc=on_fly_calc,force_source=force_source)
            normalised_data_set = []
            section_responses = dev_productivity_user_overview['section_responses']
            org_user_details = { key:val for key,val in dev_productivity_user_overview.items() if key in ["org_user_id","full_name","email"]}    
            for sublist in section_responses:
                sublist.update(org_user_details)
                normalised_data_set.append(sublist)
            scores_df = pd.json_normalize(normalised_data_set)
            invalid_scores_df = scores_df.loc[(scores_df['score'] >= 100) | (scores_df['score'] < 0 )]
            assert len(invalid_scores_df)==0, "user report has invalid scores"+str(invalid_scores_df)
            DPS_configured_sections = {}
            for each_element in dev_productivity_user_overview["section_responses"]:
                temp_sub_feature_and_score_dict = {}
                feature_wise_scores = 0
                valid_sub_features_cntr = 0
                for each_sub_element in each_element["feature_responses"]:
                    if "score" in each_sub_element:
                        feature_wise_scores = feature_wise_scores + each_sub_element["score"]
                        ## metric score validation --
                        mean = 0
                        if "mean" in each_sub_element:
                            mean = each_sub_element["mean"]
                        metric_score_validation = trellis_metric_score_validation(
                                                    metric_name=each_sub_element["name"],
                                                    metric_value=mean,
                                                    metric_score=each_sub_element["score"],
                                                    profile_settings=dev_prod_profile_response,
                                                    week_interval = week_interval_val)
                        assert metric_score_validation, "metric scores are incorrect" ##
                        valid_sub_features_cntr = valid_sub_features_cntr+1
                    if "mean" in each_sub_element:
                        temp_sub_feature_and_score_dict[each_sub_element["name"]] = each_sub_element["mean"]
                    else:
                        temp_sub_feature_and_score_dict[each_sub_element["name"]] = 0
                if "score" in each_element:
                    DPS_configured_sections[each_element["name"]] = {"mean": each_element["score"], 
                        "sub-features": temp_sub_feature_and_score_dict}
                    if valid_sub_features_cntr!=0:
                        assert -0.5 <=((feature_wise_scores/valid_sub_features_cntr) - each_element["score"]) <= 0.5, "feature scores are not consistent with the individual scores"
                else:
                    DPS_configured_sections[each_element["name"]] = {"mean": 0, 
                        "sub-features": temp_sub_feature_and_score_dict}

            if LoopFirstTimeFlag:
                from copy import deepcopy
                dps_test_cases_temp =  deepcopy(DPS_configured_sections)
                dps_test_cases = []
                for each_temp_keyy in dps_test_cases_temp:
                    each_temp_keyy_sub_features = (dps_test_cases_temp[each_temp_keyy])["sub-features"]
                    for each_sub_faeture_elem in each_temp_keyy_sub_features:
                        dps_test_cases.append([each_temp_keyy+ '_' + each_sub_faeture_elem, "Pass"])

                sqlite3_database_insert_data_into_table(arg_db_file_name=arg_dp_temp_db_file_name, 
                            arg_table_name=arg_temp_db_file_name_tcresults, 
                            arg_records_needs_to_be_inserted=dps_test_cases)

                trellis_profile_enabled_sections, trellis_profile_enabled_features = trellis_profile_retrieve_enabled_sections_and_features(
                    arg_application_url=arg_application_url, 
                    arg_req_trellis_profile_id=each_ou_user_trelis_profile)

                section_keys_availability_status = {}
                faeture_keys_availability_status = {}
                for eachEnabledSection in trellis_profile_enabled_sections:
                    if eachEnabledSection in DPS_configured_sections.keys(): pass
                    else: section_keys_availability_status[eachEnabledSection] = False
                assert len(section_keys_availability_status)==0, \
                    "Trellis profile associated sections are missing in trellis user overview" + str(section_keys_availability_status)

                ## retrieve enabled features trellis user report v/s trellis profile
                for eachEnabledFeature in trellis_profile_enabled_features:
                    associated_section, required_feature_name = eachEnabledFeature.split("_")
                    dps_available_features_dict = DPS_configured_sections[associated_section]["sub-features"]
                    ## retrieve the dynamic feature names
                    metric_names_user_report = dps_available_features_dict.keys()
                    if (timeperiod_interval in ["LAST_WEEK", "LAST_TWO_WEEKS"]
                                ) and ("month" in required_feature_name) and (
                                    "days" not in required_feature_name):
                        dps_available_features_dict_temp = []
                        for each_response_key in dps_available_features_dict.keys():
                            dps_available_features_dict_temp.append( 
                                trellis_profile_metric_name_retriever(metric_name=each_response_key,interval=timeperiod_interval))
                        metric_names_user_report = dps_available_features_dict_temp
                    if required_feature_name in metric_names_user_report: pass
                    else: faeture_keys_availability_status[eachEnabledFeature] = False
                assert len(faeture_keys_availability_status)==0, \
                    "Trellis profile associated sections are missing in trellis user overview" + str(faeture_keys_availability_status)

            for each_dps_feature_name in DPS_configured_sections:
                temp_dps_feature = DPS_configured_sections[each_dps_feature_name]
                for each_dps_sub_feature in temp_dps_feature["sub-features"]:
                    sub_feature_metric_value = ( (temp_dps_feature["sub-features"])[each_dps_sub_feature] )
                    temp_drill_down_data_selected_time_period = retrieve_drilldown_api_response(
                        application_url=arg_application_url, 
                        ou_user_id=ou_user_id,
                        ou_associated_trelis_profile=each_ou_user_trelis_profile,
                        interval=timeperiod_interval, 
                        feature_name=each_dps_sub_feature, force_source=force_source)
                    calculated_widget_value = widget_value_calculation_from_drilldown(
                        arg_drill_down_response=temp_drill_down_data_selected_time_period,
                        arg_metric_name=trellis_profile_metric_name_retriever(
                                            metric_name=each_dps_sub_feature,
                                            interval=timeperiod_interval),
                        arg_interval=timeperiod_interval)
                    if type("") == type(calculated_widget_value):
                        DPS_Metrics_Result_List.append(
                            [each_ou_user, each_time_interval ,each_dps_sub_feature,
                             sub_feature_metric_value, calculated_widget_value, "Fail"] )
                        sqlite3_database_update_query(arg_db_file_name=arg_dp_temp_db_file_name,
                                arg_table_name=arg_temp_db_file_name_tcresults,
                                column_names_to_be_updated=["Result"],column_values_to_be_updated=["Fail"], 
                                condition_columns=["Feature_TestScenario"], 
                                condition_column_values=[each_dps_feature_name + '_' + each_dps_sub_feature])
                    elif (int(sub_feature_metric_value) == int(calculated_widget_value)) or (-0.5 <= (sub_feature_metric_value - calculated_widget_value) <= 0.5):
                        DPS_Metrics_Result_List.append(
                            [each_ou_user, each_time_interval ,each_dps_sub_feature,
                             sub_feature_metric_value, calculated_widget_value, "Pass"] )
                    else:
                        DPS_Metrics_Result_List.append(
                            [each_ou_user, each_time_interval ,each_dps_sub_feature,
                             sub_feature_metric_value, calculated_widget_value, "Fail"] )
                        sqlite3_database_update_query(arg_db_file_name=arg_dp_temp_db_file_name,
                                arg_table_name=arg_temp_db_file_name_tcresults,
                                column_names_to_be_updated=["Result"],column_values_to_be_updated=["Fail"], 
                                condition_columns=["Feature_TestScenario"], 
                                condition_column_values=[each_dps_feature_name + '_' + each_dps_sub_feature])
                
    sqlite3_database_insert_data_into_table(arg_db_file_name=arg_dp_temp_db_file_name,
        arg_table_name=arg_temp_db_file_name_tcresultdata,
        arg_records_needs_to_be_inserted=DPS_Metrics_Result_List)

    argOut_DPS_Metrics_Result_DF = pd.DataFrame(DPS_Metrics_Result_List, 
                                        columns =["OU user-id", "Time period",  'Feature Name', 
                                                  'Widget Result', "Drill-down Result", "Status"])
    DPS_Metrics_Result_DF_Failed = argOut_DPS_Metrics_Result_DF.loc[argOut_DPS_Metrics_Result_DF['Status'] == "Fail"]
    pytest.dpstccounter = pytest.dpstccounter + 1

    return DPS_Metrics_Result_DF_Failed


def dev_prod_fetch_input_data(arg_config_file_data):
    ou_user_ids_list = ""
    for each_input_ou_userid in arg_config_file_data[1:]:
        if not each_input_ou_userid.startswith("#"):
            ou_user_ids_list = ou_user_ids_list + each_input_ou_userid + ","
    ou_user_ids_list = (ou_user_ids_list[:len(ou_user_ids_list)-1]).split(",")
    return (arg_config_file_data[0]).split(","), ou_user_ids_list

def trellis_profile_retrieve_enabled_sections_and_features(arg_application_url,  arg_req_trellis_profile_id, user_type="ADMIN_DPS"):
    url = arg_application_url + "dev_productivity_profiles/" + arg_req_trellis_profile_id
    payload = {}
    helper_object = TGHelper()
    response = helper_object.rbac_user(url, "get", data=payload, user_type=user_type)
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

