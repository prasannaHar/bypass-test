import logging
import pandas as pd
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestBacklogIssuesCustomer:
    generic_object = TGhelper()
    intervel_type = generic_object.api_data["week_across"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("intervel_type", intervel_type)
    def test_backlog_trend(self, intervel_type,create_generic_object,
                                                create_customer_object, get_integration_obj):

        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        stacks = create_generic_object.api_data["issues-report-stacks_ADO"]
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint.index:
            for stacks_val in stacks:
                try:
                    filters = {"sort_xaxis":"default_old-latest",
                                "product_id": create_generic_object.env["project_id"],
                               "integration_ids": get_integration_obj}

                    payload = {"filter": filters, "across": "trends", "interval": intervel_type,
                               "ou_ids": [str(df_sprint["ou_id"][val])], "stacks":[stacks_val],
                               "ou_user_filter_designation": {"sprint": ["sprint_report "]}}

                    LOG.info("payload {} ".format(payload))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "backlog_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "backlog_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                    columns=['key', 'additional_key',
                                                                                             'sprint_id', "committed_story_points", "total_workitems", "creep_keys",
                                                                                             "total_unestimated_workitems", 'story_points_by_workitem'], unique_id="dashboard_partial with ou id: {}".format(str(intervel_type)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(str(df_sprint["ou_id"][val]))
                    if not zero_flag:
                        zero_list.append(str(df_sprint["ou_id"][val]))
                except Exception as ex:
                    not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across : " + intervel_type + " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across :" + intervel_type + "  {}".format(
            set(list_not_match))