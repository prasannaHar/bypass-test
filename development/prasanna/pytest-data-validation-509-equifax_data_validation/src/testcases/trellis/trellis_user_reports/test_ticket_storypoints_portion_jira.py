import logging
import pytest
import pandas as pd
import itertools
import json

from src.utils.dev_prod_reusable_functions import dev_prod_fetch_input_data
from src.lib.core_reusable_functions import epoch_timeStampsGenerationForRequiredTimePeriods as TPhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisTicketStoryPointPortion:


    jira_feature = [
                    "Number of bugs worked on per month", 
                    # "Number of stories worked on per month", 
                    # "Number of Story Points worked on per month"
                    ]

    @pytest.mark.parametrize("jira_feature", jira_feature)
    @pytest.mark.run(order=1)
    def test_trellis_ticket_and_storypoint_portion(self,jira_feature, create_widget_helper_object,
                                    get_integration_obj,create_generic_object, create_customer_object):

        ## retrieving required user and profile ids
        ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
            arg_config_file_data=create_generic_object.env["dps_test_config_with_names"])
        required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))

        zero_list = []
        list_not_match = []
        not_executed_list = []
        failure_data = []
        for eachrecord in required_tests:
            try:
                userid_and_profile_id = (eachrecord[0]).split(":")
                no_of_months, gt, lt = TPhelper(eachrecord[1])
                filters = {"user_id_type": "ou_user_ids", "user_id_list": [userid_and_profile_id[0]],
                           "feature_name": jira_feature,
                           "time_range": {"$gt": str(gt), "$lt": str(lt)}, "partial": {},
                           "dev_productivity_profile_id": userid_and_profile_id[1]}
                payload = {"filter": filters, "sort": [], "across": ""}
                LOG.info("payload {} ".format(json.dumps(payload)))

                base_url = create_generic_object.connection["base_url"] + \
                    create_generic_object.api_data["trellis_feature_list"] 
                feature_list_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                issues = (feature_list_response["records"][0])["records"]

                ## checking for feature lits api records count -- current iteration will be skipped in case of no data
                if len(issues) == 0:  
                    zero_list.append(eachrecord)
                    continue

                for eachissue in issues:
                    failure_flag = False
                    issue_keys = [eachissue["key"]]
                    assignee_name = userid_and_profile_id[2]
                    ticket_portion_actual = eachissue["ticket_portion"]
                    story_points_portion_actual = eachissue["story_points_portion"]

                    issue_response = create_widget_helper_object.retrieve_issue_details(
                        integration_id=get_integration_obj, issue_keys=issue_keys)
                    story_points, ticket_portion_calc, storypoints_portion_calc = create_customer_object.trellis_calculate_ado_ticket_portion(workitem_response=issue_response, assignee_name=assignee_name)
                    tickets_result_diff = ticket_portion_actual-ticket_portion_calc
                    storypoints_result_diff = story_points_portion_actual-storypoints_portion_calc
                    if not ( -0.05 <= tickets_result_diff <= 0.05):
                        list_not_match.append(
                            "Ticket portion difference: org user details {orguserid} & \
                                workitem id - {id} with difference {res_diff} ".format(
                            orguserid=eachrecord, id=issue_keys, res_diff=tickets_result_diff))
                        failure_flag = True
                    if not ( -0.05 <= storypoints_result_diff <= 0.05):
                        list_not_match.append(
                            "Story points portion difference: org user details {orguserid} \
                                & workitem id - {id} with difference {res_diff} ".format(
                            orguserid=eachrecord, id=issue_keys, res_diff=storypoints_result_diff))
                        failure_flag = True
                    if(failure_flag):
                        failure_data.append([userid_and_profile_id, issue_keys[0],story_points,
                                    ticket_portion_actual, ticket_portion_calc,tickets_result_diff,
                                    story_points_portion_actual, storypoints_portion_calc, storypoints_result_diff])
                        
            except Exception as ex:
                not_executed_list.append(eachrecord)

        LOG.info("No data list(drill-downs with No Data) {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        if len(failure_data)>=1:
            failure_data_df = pd.DataFrame(failure_data, columns = ["org user name", "workitem id", "story points",
                                "ticket portion-feature list", "ticket portion-workitem list", "ticket-diff", 
                                "storypoints portion-feature list", "storypoints portion-workitem list", "storypoint-diff" ])
            failure_data_df.to_csv("log_updates/trellis_story_ticket_portion_result" + jira_feature + ".csv")

        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

