import json
import logging
import pytest

from copy import deepcopy
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["issue-resolution-across_type"]
    across_type = [x for x in across_type if not x.startswith("custom")]
    if "assignee": across_type.remove("assignee")
    if "reporter": across_type.remove("reporter")
    across_type_ss = generic_object.api_data["issue-resolution-across_type_ss"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    @pytest.mark.aggstcsunit
    def test_issue_resolution_time_report(self, across_type, create_generic_object,
                                          create_customer_object):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        gt, lt = create_generic_object.get_epoc_time(value=4)
        for val in df_sprint:
            try:
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]
                filters = {"projects": create_generic_object.env["project_names"],
                           "statuses": ["DONE", "CLOSED", "RESOLVED"],
                           "issue_created_at": {"$gt": gt, "$lt": lt},
                           "exclude": {"issue_types": ["EPIC"]}, "product_id": create_generic_object.env["project_id"]}
                filters2 = deepcopy(filters)
                existing_flow_id, new_flow_id = (create_generic_object.env["widget_aggs_test_flag"]).split(":")
                filters["integration_ids"] = [existing_flow_id]
                filters2["integration_ids"] = [new_flow_id]
                ou1, ou2 = val.split(":")
                payload1 = {"filter": filters, "across": across_type, "across_limit": 20, "ou_ids": [ou1]}
                payload2 = {"filter": filters2, "across": across_type, "across_limit": 20, "ou_ids": [ou2]}
                if len(interval_val) != 0:
                    payload1["interval"] = interval_val
                    payload2["interval"] = interval_val
                LOG.info("payload {} ".format(json.dumps(payload1)))
                LOG.info("payload {} ".format(json.dumps(payload2)))

                url = create_generic_object.connection["base_url"] + "jira_issues/resolution_time_report"
                es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'])
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
    @pytest.mark.parametrize("across_type_ss", across_type_ss)
    @pytest.mark.aggstcsunit
    def test_issue_resolution_time_single_stat_report_es(self, across_type_ss, create_generic_object,
                                                         create_customer_object):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
        for val in df_sprint:
            for each_agg_type in agg_type:
                try:
                    filters = {"agg_type": each_agg_type, across_type_ss + "_at": {"$gt": gt, "$lt": lt},
                               "projects": create_generic_object.env["project_names"],
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

                    url = create_generic_object.connection["base_url"] + "jira_issues/resolution_time_report"
                    es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                    db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max", "mean",
                                                                                         "p90", 'total_tickets'])
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
