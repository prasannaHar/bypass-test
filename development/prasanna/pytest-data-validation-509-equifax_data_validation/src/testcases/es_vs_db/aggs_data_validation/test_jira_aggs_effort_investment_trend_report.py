import logging
import pytest
import json
from copy import deepcopy

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:

    @pytest.mark.run(order=1)
    @pytest.mark.aggstcsunit
    def test_effort_investment_trend_report(self, create_generic_object,
                                            create_customer_object):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                gt_epoc,lt_epoc = create_generic_object.get_epoc_time(value=3)
                filters = {"issue_resolved_at": {"$gt": gt_epoc, "$lt": lt_epoc},
                           "ba_attribution_mode": "current_assignee",
                           "ticket_categorization_scheme": create_generic_object.env["effort_investment_profile_id"],
                           "ticket_categories": ["Technical Tasks"]}
                filters2 = deepcopy(filters)
                existing_flow_id, new_flow_id = (create_generic_object.env["widget_aggs_test_flag"]).split(":")
                filters["integration_ids"] = [existing_flow_id]
                filters2["integration_ids"] = [new_flow_id]
                ou1, ou2 = val.split(":")
                payload1 = {"filter": filters, "across": "issue_resolved_at", "interval": "biweekly", "ou_ids": [ou1],
                            "ou_user_filter_designation": create_generic_object.env["ou_user_filter_designation"]}
                payload2 = {"filter": filters2, "across": "issue_resolved_at", "interval": "biweekly", "ou_ids": [ou2],
                            "ou_user_filter_designation": create_generic_object.env["ou_user_filter_designation"]}
                LOG.info("payload {} ".format(json.dumps(payload1)))
                LOG.info("payload {} ".format(json.dumps(payload2)))

                url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                    "effort_investment_category_api_url"]
                es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name='key',
                                                                                           columns=['key',
                                                                                                    'additional_key',
                                                                                                    'total', "fte",
                                                                                                    "effort"])
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))
