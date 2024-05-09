import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestBounceVsIssue:
    generic_object = TGhelper()
    across_type = generic_object.get_aggregration_fields()

    @pytest.mark.run(order=1)
    @pytest.mark.dataaccuracy
    @pytest.mark.parametrize("across_type", across_type)
    def test_bounce_vs_issue_report(self, across_type, create_dataaccuracy_object, create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        dropdown_value= []
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:
                int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=val)
                custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=val)
                custom_fields = list(custom_fields_set)

                for k in custom_fields:
                    if "Sprint" in k:
                        custom_sprint = k.split("-")
                        sprint_field = custom_sprint[1]

                customfield = {"sprint": [sprint_field]}
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[1]

                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket",
                    visualization="bar_chart",
                    across=across_type,
                    sort_value=[{"id": "ticket_count", "desc": True}], ou_ids=val, customfield=customfield , integration_ids=int_ids)

                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]
                issue_response = create_generic_object.execute_api_call(issue_url, "post", data=issue_payload)

                bounce_payload = create_dataaccuracy_object.accuracy_payload(
                    across=across_type,
                    sort_value=[{"id": "bounces", "desc": True}], ou_ids=val, customfield=customfield ,integration_ids=int_ids )

                bounce_url = create_generic_object.connection[
                                 "base_url"] + create_generic_object.api_data["bounce_widget_api_url"]
                bounce_response = create_generic_object.execute_api_call(bounce_url, "post", data=bounce_payload)

                flag, zero_flag = create_dataaccuracy_object.comparing_reports(issue_response, bounce_response)
                validation_dropdown = create_dataaccuracy_object.dropdown_validation(bounce_response, across_type, val, ou_user_filter , int_ids)
                if not validation_dropdown:
                    dropdown_value.append(val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(dropdown_value) == 0, "Dropdown value is not matched {}".format(dropdown_value)
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("across_type", across_type)
    def test_comparing_dashboard_filter_bounce_vs_issue_report(self, across_type, create_dataaccuracy_object,
                                                               create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:
                int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=val)
                custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=val)
                custom_fields = list(custom_fields_set)

                for k in custom_fields:
                    if "Sprint" in k:
                        custom_sprint = k.split("-")
                        sprint_field = custom_sprint[1]

                customfield = {"sprint": [sprint_field]}
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[1]
                req_filter_names_and_value_pair = []
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[1]

                dashboard_filter = create_generic_object.api_data["filter_widgets"]
                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket", arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    visualization="bar_chart", dashboardfilter=dashboard_filter,
                    across=across_type,
                    sort_value=[{"id": "ticket_count", "desc": True}], ou_ids=val, customfield=customfield , integration_ids=int_ids)

                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]
                issue_response = create_generic_object.execute_api_call(issue_url, "post", data=issue_payload)

                bounce_payload = create_dataaccuracy_object.accuracy_payload(across=across_type,
                                                                             dashboardfilter=dashboard_filter,
                                                                             sort_value=[
                                                                                 {"id": "bounces", "desc": True}],
                                                                             ou_ids=val, customfield=customfield , integration_ids=int_ids)

                bounce_url = create_generic_object.connection[
                                 "base_url"] + create_generic_object.api_data["bounce_widget_api_url"]
                bounce_response = create_generic_object.execute_api_call(bounce_url, "post", data=bounce_payload)

                flag, zero_flag = create_dataaccuracy_object.comparing_reports(issue_response, bounce_response)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=3)
    @pytest.mark.dataaccuracy
    @pytest.mark.parametrize("across_type", across_type)
    def test_comparing_bounce_vs_issue_report_with_exclude(self, across_type, create_dataaccuracy_object,
                                                           create_generic_object):
        df_sprint = create_generic_object.env["set_ous"]
        ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:
                int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=val)
                custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=val)
                custom_fields = list(custom_fields_set)

                for k in custom_fields:
                    if "Sprint" in k:
                        custom_sprint = k.split("-")
                        sprint_field = custom_sprint[1]

                customfield = {"sprint": [sprint_field]}
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[1]
                req_filter_names_and_value_pair = []
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[1]
                req_filter_names_and_value_pair = []
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[1]

                exclude = create_generic_object.api_data["filter_widgets"]
                issue_payload = create_dataaccuracy_object.accuracy_payload(
                    metric="ticket", arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    visualization="bar_chart", exclude=exclude,
                    across=across_type,
                    sort_value=[{"id": "ticket_count", "desc": True}], ou_ids=val, customfield=customfield , integration_ids=int_ids)

                issue_url = create_generic_object.connection[
                                "base_url"] + create_generic_object.api_data["jira_tickets_report"]
                issue_response = create_generic_object.execute_api_call(issue_url, "post", data=issue_payload)

                bounce_payload = create_dataaccuracy_object.accuracy_payload(
                    across=across_type, exclude=exclude,
                    sort_value=[{"id": "bounces", "desc": True}], ou_ids=val, customfield=customfield , integration_ids=int_ids)

                bounce_url = create_generic_object.connection[
                                 "base_url"] + create_generic_object.api_data["bounce_widget_api_url"]
                bounce_response = create_generic_object.execute_api_call(bounce_url, "post", data=bounce_payload)

                flag, zero_flag = create_dataaccuracy_object.comparing_reports(issue_response, bounce_response)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))
