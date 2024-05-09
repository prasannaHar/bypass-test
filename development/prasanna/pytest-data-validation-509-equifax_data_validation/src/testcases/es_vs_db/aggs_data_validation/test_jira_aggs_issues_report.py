import json
import logging
import pytest

from copy import deepcopy
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["issues-report-across_type"]
    across_type = [x for x in across_type if not x.startswith("custom")]
    if "assignee": across_type.remove("assignee")
    if "reporter": across_type.remove("reporter")
    across_type_ss = generic_object.api_data["issues-report-across_type_ss"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    @pytest.mark.aggstcsunit
    def test_issues_report_story_points(self, across_type, drilldown_object, create_generic_object,
                                        create_customer_object):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        for val in df_sprint:
            try:
                required_filters_needs_tobe_applied = ["issue_types", "statuses", "projects"]
                filter_value = [["STORY", "REQUEST"], ["CLOSED", "DONE", "RESOLVED"],
                                create_generic_object.env["project_names"]]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]
                req_filter_names_and_value_pair2 = deepcopy(req_filter_names_and_value_pair)

                existing_flow_id, new_flow_id = (create_generic_object.env["widget_aggs_test_flag"]).split(":")
                req_filter_names_and_value_pair.append(["integration_ids", [existing_flow_id]])
                req_filter_names_and_value_pair2.append(["integration_ids", [new_flow_id]])
                ou1, ou2 = val.split(":")

                payload1 = drilldown_object.generate_issues_report_payload(
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_metric="story_point",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_across=across_type,
                    arg_ou_id=[ou1])
                payload2 = drilldown_object.generate_issues_report_payload(
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_metric="story_point",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair2,
                    arg_across=across_type,
                    arg_ou_id=[ou2])
                if len(interval_val) != 0:
                    payload1["interval"] = interval_val
                    payload2["interval"] = interval_val
                LOG.info("payload {} ".format(json.dumps(payload1)))
                LOG.info("payload {} ".format(json.dumps(payload2)))

                url = create_generic_object.connection["base_url"] + "jira_issues/story_point_report"
                es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'total_story_points',
                                                                                     'total_unestimated_tickets',
                                                                                     'total_tickets'])
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("across_type", across_type)
    @pytest.mark.aggstcsunit
    def test_issues_report(self, across_type, drilldown_object, create_generic_object, create_customer_object):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        for val in df_sprint:
            try:
                required_filters_needs_tobe_applied = ["issue_types", "statuses", "projects"]
                filter_value = [["STORY", "REQUEST"], ["CLOSED", "DONE", "RESOLVED"],
                                create_generic_object.env["project_names"]]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]
                req_filter_names_and_value_pair2 = deepcopy(req_filter_names_and_value_pair)

                existing_flow_id, new_flow_id = (create_generic_object.env["widget_aggs_test_flag"]).split(":")
                req_filter_names_and_value_pair.append(["integration_ids", [existing_flow_id]])
                req_filter_names_and_value_pair2.append(["integration_ids", [new_flow_id]])
                ou1, ou2 = val.split(":")

                payload1 = drilldown_object.generate_issues_report_payload(
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_across=across_type,
                    arg_ou_id=[ou1])
                payload2 = drilldown_object.generate_issues_report_payload(
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair2,
                    arg_across=across_type,
                    arg_ou_id=[ou2])
                if len(interval_val) != 0:
                    payload1["interval"] = interval_val
                    payload2["interval"] = interval_val
                LOG.info("payload {} ".format(json.dumps(payload1)))
                LOG.info("payload {} ".format(json.dumps(payload2)))

                url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
                es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'total_story_points',
                                                                                     'mean_story_points',
                                                                                     'total_tickets'])
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("across_type_ss", across_type_ss)
    @pytest.mark.aggstcsunit
    def test_issues_report_singleStat_es(self, across_type_ss, create_generic_object,
                                         create_customer_object):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=30, type="days")
        for val in df_sprint:
            try:
                filters = {"projects": create_generic_object.env["project_names"],
                           across_type_ss + "_at": {"$gt": gt, "$lt": lt},
                           "product_id": create_generic_object.env["project_id"]}
                filters2 = deepcopy(filters)
                existing_flow_id, new_flow_id = (create_generic_object.env["widget_aggs_test_flag"]).split(":")
                filters["integration_ids"] = [existing_flow_id]
                filters2["integration_ids"] = [new_flow_id]
                ou1, ou2 = val.split(":")
                payload1 = {"filter": filters, "across": across_type_ss, "ou_ids": [ou1]}
                payload2 = {"filter": filters2, "across": across_type_ss, "ou_ids": [ou2]}
                LOG.info("payload {} ".format(json.dumps(payload1)))
                LOG.info("payload {} ".format(json.dumps(payload2)))

                url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
                es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'total_story_points',
                                                                                     'mean_story_points',
                                                                                     'total_tickets'])

                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)
        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))
