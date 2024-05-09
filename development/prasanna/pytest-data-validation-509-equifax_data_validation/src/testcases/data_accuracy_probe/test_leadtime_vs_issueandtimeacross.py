import logging
import random
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestLeadtimeVsissueandacross:
    generic_object = TGhelper()
    issue_type = generic_object.api_data["issue_type"]

    @pytest.mark.run(order=1)
    # @pytest.mark.dataaccuracy
    @pytest.mark.parametrize("issue_type", issue_type)
    def test_leadtime_by_oneday(self, issue_type, create_dataaccuracy_object, create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        dropdown_value = []
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt_created, lt_created = create_generic_object.get_epoc_time(value=1, type="days")
        for val in df_sprint:
            try:
                leadtime_customfield = ou_user_filter
                leadtime_customfield['github'] = ["none"]
                leadtime_payload = create_dataaccuracy_object.accuracy_payload(
                    calculation="ticket_velocity",
                    ratings=["good", "slow", "needs_attention"],
                    jira_issue_types=[issue_type],
                    velocity_config_id=create_generic_object.env["env_jira_velocity_config_id"],
                    jira_issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="velocity", ou_ids=val,
                    customfield=leadtime_customfield)

                leadtime_url = create_generic_object.connection[
                                   "base_url"] + create_generic_object.api_data["velocity"]
                leadtime_response = create_generic_object.execute_api_call(leadtime_url, "post", data=leadtime_payload)

                acrossjira_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="average_time",
                    issue_types=[issue_type],
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="none", ou_ids=val,
                    customfield=ou_user_filter)
                acrossjira_url = create_generic_object.connection[
                                     "base_url"] + create_generic_object.api_data["issue_time_across_stages_jira"]

                acrossjira_response = create_generic_object.execute_api_call(acrossjira_url, "post",
                                                                             data=acrossjira_payload)

                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket",
                    issue_types=[issue_type],
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="status", ou_ids=val,
                    customfield=ou_user_filter)
                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]

                issue_response = create_generic_object.execute_api_call(issue_url, "post",
                                                                        data=issue_payload)
                result_comparing1 = create_dataaccuracy_object.compare_data_in_df(leadtime_response,
                                                                                  acrossjira_response,
                                                                                  data_name='ready for prod')
                if not result_comparing1:
                    LOG.info("Not matching the ISSUE TIME ACROSS STAGES widget")
                    zero_list.append(val)
                result_comparing2 = create_dataaccuracy_object.compare_data_in_df(leadtime_response, issue_response,
                                                                                  data_name='ready for prod')
                if not result_comparing2:
                    LOG.info("Not matching the ISSUE REPORT widget")
                    zero_list.append(val)
                leadtime_payload["across"] = "values"
                velocity_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["velocity_values"]
                dropdown_leadtime = create_generic_object.execute_api_call(velocity_url, "post",
                                                                        data=leadtime_payload)
                dropdown_value.append(dropdown_leadtime["count"])
                list_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["drill_down_api_url"]
                acrossjira_payload["filter"]["stages"] = ["DONE"]
                acrossjira_payload["across"] = "none"
                dropdown_across = create_generic_object.execute_api_call(list_url, "post",
                                                                           data=acrossjira_payload)
                dropdown_value.append(dropdown_across["count"])
                issue_payload["filter"]["statuses"] = ["DONE"]
                issue_payload["across"] = "status"
                dropdown_issue = create_generic_object.execute_api_call(list_url, "post",
                                                                         data=issue_payload)
                dropdown_value.append(dropdown_issue["count"])
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("count of dropdown values {}".format(dropdown_value))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(set(dropdown_value)) == 1, "Dropdown value is not matched {}".format(set(dropdown_value))
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(zero_list) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=2)
    # @pytest.mark.dataaccuracy
    @pytest.mark.parametrize("issue_type", issue_type)
    def test_leadtime_by_oneweek(self, issue_type, create_dataaccuracy_object, create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        dropdown_value = []
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt_created, lt_created = create_generic_object.get_epoc_time(value=14, type="days")
        for val in df_sprint:
            try:
                leadtime_customfield = ou_user_filter
                leadtime_customfield['github'] = ["none"]
                leadtime_payload = create_dataaccuracy_object.accuracy_payload(
                    calculation="ticket_velocity",
                    ratings=["good", "slow", "needs_attention"],
                    jira_issue_types=[issue_type],
                    velocity_config_id=create_generic_object.env["env_jira_velocity_config_id"],
                    jira_issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="velocity", ou_ids=val,
                    customfield=leadtime_customfield)

                leadtime_url = create_generic_object.connection[
                                   "base_url"] + create_generic_object.api_data["velocity"]
                leadtime_response = create_generic_object.execute_api_call(leadtime_url, "post", data=leadtime_payload)

                acrossjira_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="average_time",
                    issue_types=[issue_type],
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="none", ou_ids=val,
                    customfield=ou_user_filter)
                acrossjira_url = create_generic_object.connection[
                                     "base_url"] + create_generic_object.api_data["issue_time_across_stages_jira"]

                acrossjira_response = create_generic_object.execute_api_call(acrossjira_url, "post",
                                                                             data=acrossjira_payload)

                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket",
                    issue_types=[issue_type],
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="status", ou_ids=val,
                    customfield=ou_user_filter)
                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]

                issue_response = create_generic_object.execute_api_call(issue_url, "post",
                                                                        data=issue_payload)
                result_comparing1 = create_dataaccuracy_object.compare_data_in_df(leadtime_response,
                                                                                  acrossjira_response,
                                                                                  data_name='ready for prod')
                if not result_comparing1:
                    LOG.info("Not matching the ISSUE TIME ACROSS STAGES widget")
                    zero_list.append(val)
                result_comparing2 = create_dataaccuracy_object.compare_data_in_df(leadtime_response, issue_response,
                                                                                  data_name='ready for prod')
                if not result_comparing2:
                    LOG.info("Not matching the ISSUE REPORT widget")
                    zero_list.append(val)
                leadtime_payload["across"] = "values"
                velocity_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["velocity_values"]
                dropdown_leadtime = create_generic_object.execute_api_call(velocity_url, "post",
                                                                        data=leadtime_payload)
                dropdown_value.append(dropdown_leadtime["count"])
                list_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["drill_down_api_url"]
                acrossjira_payload["filter"]["stages"] = ["DONE"]
                acrossjira_payload["across"] = "none"
                dropdown_across = create_generic_object.execute_api_call(list_url, "post",
                                                                           data=acrossjira_payload)
                dropdown_value.append(dropdown_across["count"])
                issue_payload["filter"]["statuses"] = ["DONE"]
                issue_payload["across"] = "status"
                dropdown_issue = create_generic_object.execute_api_call(list_url, "post",
                                                                         data=issue_payload)
                dropdown_value.append(dropdown_issue["count"])
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("count of dropdown values {}".format(dropdown_value))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(set(dropdown_value)) == 1, "Dropdown value is not matched {}".format(set(dropdown_value))
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(zero_list) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=3)
    # @pytest.mark.dataaccuracy
    @pytest.mark.parametrize("issue_type", issue_type)
    def test_leadtime_by_onemonth(self, issue_type, create_dataaccuracy_object, create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        dropdown_value = []
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt_created, lt_created = create_generic_object.get_epoc_time(value=1, type="months")
        for val in df_sprint:
            try:
                leadtime_customfield = ou_user_filter
                leadtime_customfield['github'] = ["none"]
                leadtime_payload = create_dataaccuracy_object.accuracy_payload(
                    calculation="ticket_velocity",
                    ratings=["good", "slow", "needs_attention"],
                    jira_issue_types=[issue_type],
                    velocity_config_id=create_generic_object.env["env_jira_velocity_config_id"],
                    jira_issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="velocity", ou_ids=val,
                    customfield=leadtime_customfield)

                leadtime_url = create_generic_object.connection[
                                   "base_url"] + create_generic_object.api_data["velocity"]
                leadtime_response = create_generic_object.execute_api_call(leadtime_url, "post", data=leadtime_payload)

                acrossjira_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="average_time",
                    issue_types=[issue_type],
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="none", ou_ids=val,
                    customfield=ou_user_filter)
                acrossjira_url = create_generic_object.connection[
                                     "base_url"] + create_generic_object.api_data["issue_time_across_stages_jira"]

                acrossjira_response = create_generic_object.execute_api_call(acrossjira_url, "post",
                                                                             data=acrossjira_payload)

                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket",
                    issue_types=[issue_type],
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="status", ou_ids=val,
                    customfield=ou_user_filter)
                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]

                issue_response = create_generic_object.execute_api_call(issue_url, "post",
                                                                        data=issue_payload)
                result_comparing1 = create_dataaccuracy_object.compare_data_in_df(leadtime_response,
                                                                                  acrossjira_response,
                                                                                  data_name='ready for prod')
                if not result_comparing1:
                    LOG.info("Not matching the ISSUE TIME ACROSS STAGES widget")
                    zero_list.append(val)
                result_comparing2 = create_dataaccuracy_object.compare_data_in_df(leadtime_response, issue_response,
                                                                                  data_name='ready for prod')
                if not result_comparing2:
                    LOG.info("Not matching the ISSUE REPORT widget")
                    zero_list.append(val)
                leadtime_payload["across"] = "values"
                velocity_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["velocity_values"]
                dropdown_leadtime = create_generic_object.execute_api_call(velocity_url, "post",
                                                                        data=leadtime_payload)
                dropdown_value.append(dropdown_leadtime["count"])
                list_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["drill_down_api_url"]
                acrossjira_payload["filter"]["stages"] = ["DONE"]
                acrossjira_payload["across"] = "none"
                dropdown_across = create_generic_object.execute_api_call(list_url, "post",
                                                                           data=acrossjira_payload)
                dropdown_value.append(dropdown_across["count"])
                issue_payload["filter"]["statuses"] = ["DONE"]
                issue_payload["across"] = "status"
                dropdown_issue = create_generic_object.execute_api_call(list_url, "post",
                                                                         data=issue_payload)
                dropdown_value.append(dropdown_issue["count"])
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("count of dropdown values {}".format(dropdown_value))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(set(dropdown_value)) == 1, "Dropdown value is not matched {}".format(set(dropdown_value))
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(zero_list) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=4)
    @pytest.mark.parametrize("issue_type", issue_type)
    def test_leadtime_by_dashboard_onday(self, issue_type, create_dataaccuracy_object, create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        dropdown_value = []
        zero_list = []
        list_not_match = []
        not_executed_list = []
        dashboard_filter = create_generic_object.api_data["filter_widgets"]
        gt_created, lt_created = create_generic_object.get_epoc_time(value=1, type="days")
        for val in df_sprint:
            try:
                leadtime_customfield = ou_user_filter
                leadtime_customfield['github'] = ["none"]
                leadtime_payload = create_dataaccuracy_object.accuracy_payload(
                    calculation="ticket_velocity",dashboardfilter=dashboard_filter,
                    ratings=["good", "slow", "needs_attention"],
                    jira_issue_types=[issue_type],
                    velocity_config_id=create_generic_object.env["env_jira_velocity_config_id"],
                    jira_issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="velocity", ou_ids=val,
                    customfield=leadtime_customfield)

                leadtime_url = create_generic_object.connection[
                                   "base_url"] + create_generic_object.api_data["velocity"]
                leadtime_response = create_generic_object.execute_api_call(leadtime_url, "post", data=leadtime_payload)

                acrossjira_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="average_time",
                    dashboardfilter=dashboard_filter,
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="none", ou_ids=val,
                    customfield=ou_user_filter)
                acrossjira_url = create_generic_object.connection[
                                     "base_url"] + create_generic_object.api_data["issue_time_across_stages_jira"]

                acrossjira_response = create_generic_object.execute_api_call(acrossjira_url, "post",
                                                                             data=acrossjira_payload)

                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket",
                    dashboardfilter=dashboard_filter,
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="status", ou_ids=val,
                    customfield=ou_user_filter)
                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]

                issue_response = create_generic_object.execute_api_call(issue_url, "post",
                                                                        data=issue_payload)
                result_comparing1 = create_dataaccuracy_object.compare_data_in_df(leadtime_response,
                                                                                  acrossjira_response,
                                                                                  data_name='ready for prod')
                if not result_comparing1:
                    LOG.info("Not matching the ISSUE TIME ACROSS STAGES widget")
                    zero_list.append(val)
                result_comparing2 = create_dataaccuracy_object.compare_data_in_df(leadtime_response, issue_response,
                                                                                  data_name='ready for prod')
                if not result_comparing2:
                    LOG.info("Not matching the ISSUE REPORT widget")
                    zero_list.append(val)
                leadtime_payload["across"] = "values"
                velocity_url = create_generic_object.connection[
                                   "base_url"] + create_generic_object.api_data["velocity_values"]
                dropdown_leadtime = create_generic_object.execute_api_call(velocity_url, "post",
                                                                           data=leadtime_payload)
                dropdown_value.append(dropdown_leadtime["count"])
                list_url = create_generic_object.connection[
                               "base_url"] + create_generic_object.api_data["drill_down_api_url"]
                acrossjira_payload["filter"]["stages"] = ["DONE"]
                acrossjira_payload["across"] = "none"
                dropdown_across = create_generic_object.execute_api_call(list_url, "post",
                                                                         data=acrossjira_payload)
                dropdown_value.append(dropdown_across["count"])
                issue_payload["filter"]["statuses"] = ["DONE"]
                issue_payload["across"] = "status"
                dropdown_issue = create_generic_object.execute_api_call(list_url, "post",
                                                                        data=issue_payload)
                dropdown_value.append(dropdown_issue["count"])
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("count of dropdown values {}".format(dropdown_value))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(set(dropdown_value)) == 1, "Dropdown value is not matched {}".format(set(dropdown_value))
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(zero_list) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=5)
    # @pytest.mark.dataaccuracy
    @pytest.mark.parametrize("issue_type", issue_type)
    def test_leadtime_by_exclude_onday(self, issue_type, create_dataaccuracy_object, create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        dropdown_value = []
        zero_list = []
        list_not_match = []
        not_executed_list = []
        exclude = create_generic_object.api_data["filter_widgets"]
        gt_created, lt_created = create_generic_object.get_epoc_time(value=1, type="days")
        for val in df_sprint:
            try:
                leadtime_customfield = ou_user_filter
                leadtime_customfield['github'] = ["none"]
                leadtime_payload = create_dataaccuracy_object.accuracy_payload(
                    calculation="ticket_velocity",exclude=exclude,
                    ratings=["good", "slow", "needs_attention"],
                    jira_issue_types=[issue_type],
                    velocity_config_id=create_generic_object.env["env_jira_velocity_config_id"],
                    jira_issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="velocity", ou_ids=val,
                    customfield=leadtime_customfield)

                leadtime_url = create_generic_object.connection[
                                   "base_url"] + create_generic_object.api_data["velocity"]
                leadtime_response = create_generic_object.execute_api_call(leadtime_url, "post", data=leadtime_payload)

                acrossjira_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="average_time",
                    exclude=exclude,
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="none", ou_ids=val,
                    customfield=ou_user_filter)
                acrossjira_url = create_generic_object.connection[
                                     "base_url"] + create_generic_object.api_data["issue_time_across_stages_jira"]

                acrossjira_response = create_generic_object.execute_api_call(acrossjira_url, "post",
                                                                             data=acrossjira_payload)

                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket",
                    exclude=exclude,
                    issue_resolved_at={"$gt": gt_created, "$lt": lt_created}, across="status", ou_ids=val,
                    customfield=ou_user_filter)
                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]

                issue_response = create_generic_object.execute_api_call(issue_url, "post",
                                                                        data=issue_payload)
                result_comparing1 = create_dataaccuracy_object.compare_data_in_df(leadtime_response,
                                                                                  acrossjira_response,
                                                                                  data_name='ready for prod')
                if not result_comparing1:
                    LOG.info("Not matching the ISSUE TIME ACROSS STAGES widget")
                    zero_list.append(val)
                result_comparing2 = create_dataaccuracy_object.compare_data_in_df(leadtime_response, issue_response,
                                                                                  data_name='ready for prod')
                if not result_comparing2:
                    LOG.info("Not matching the ISSUE REPORT widget")
                    zero_list.append(val)
                leadtime_payload["across"] = "values"
                velocity_url = create_generic_object.connection[
                                   "base_url"] + create_generic_object.api_data["velocity_values"]
                dropdown_leadtime = create_generic_object.execute_api_call(velocity_url, "post",
                                                                           data=leadtime_payload)
                dropdown_value.append(dropdown_leadtime["count"])
                list_url = create_generic_object.connection[
                               "base_url"] + create_generic_object.api_data["drill_down_api_url"]
                acrossjira_payload["filter"]["stages"] = ["DONE"]
                acrossjira_payload["across"] = "none"
                dropdown_across = create_generic_object.execute_api_call(list_url, "post",
                                                                         data=acrossjira_payload)
                dropdown_value.append(dropdown_across["count"])
                issue_payload["filter"]["statuses"] = ["DONE"]
                issue_payload["across"] = "status"
                dropdown_issue = create_generic_object.execute_api_call(list_url, "post",
                                                                        data=issue_payload)
                dropdown_value.append(dropdown_issue["count"])
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("count of dropdown values {}".format(dropdown_value))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(set(dropdown_value)) == 1, "Dropdown value is not matched {}".format(set(dropdown_value))
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(zero_list) == 0, "Not Matching List-  {}".format(set(list_not_match))