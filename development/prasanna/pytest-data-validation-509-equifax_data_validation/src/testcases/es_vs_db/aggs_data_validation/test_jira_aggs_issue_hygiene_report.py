import logging
import pytest
import json
from copy import deepcopy
import datetime
from dateutil.relativedelta import relativedelta

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()
    hygiene_type = generic_object.api_data["hygiene_types"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("hygiene_type", hygiene_type)
    @pytest.mark.aggstcsunit1
    def test_jira_aggs_issue_hygiene_report(self, hygiene_type, create_generic_object,
                                            create_customer_object):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint:
            try:
                filters = {"hideScore": False, "hygiene_types": [hygiene_type]}
                filters2 = deepcopy(filters)
                existing_flow_id, new_flow_id = (create_generic_object.env["widget_aggs_test_flag"]).split(":")
                filters["integration_ids"] = [existing_flow_id]
                filters2["integration_ids"] = [new_flow_id]
                ou1, ou2 = val.split(":")
                payload1 = {"filter": filters, "across": "project", "ou_ids": [ou1],
                            "ou_user_filter_designation": create_generic_object.env["ou_user_filter_designation"]}
                payload2 = {"filter": filters2, "across": "project", "ou_ids": [ou2],
                            "ou_user_filter_designation": create_generic_object.env["ou_user_filter_designation"]}
                LOG.info("payload {} ".format(json.dumps(payload1)))
                LOG.info("payload {} ".format(json.dumps(payload2)))

                url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                    "jira_tickets_report"]
                es_response = create_generic_object.execute_api_call(url, "post", data=payload1)
                db_response = create_generic_object.execute_api_call(url, "post", data=payload2)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name='key',
                                                                                           columns=['key',
                                                                                                    'mean_story_points',
                                                                                                    'total_tickets',
                                                                                                    "total_story_points"])
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
