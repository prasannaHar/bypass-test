import logging
import pandas as pd
import pytest
import itertools
import json

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestImpactOfUnestimatedTicketsCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["week_across"]
    metric_type = generic_object.api_data["sprint-impact-of-unestimated-tickets-report-metrics_ADO"]
    required_tests = list(itertools.product(across_type, metric_type))

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type,metric_type", required_tests)
    def test_sprint_impact_of_unestimated_tickets_report_ADO_es(self, across_type,metric_type, create_generic_object,
                                                create_customer_object, get_integration_obj):
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt,lt = create_generic_object.get_epoc_time(value=2)
        for val in df_sprint.index:
            try:
                filters = {"include_workitem_ids":True,
                            "completed_at":{"$gt": gt, "$lt": lt},
                            "integration_ids": get_integration_obj,
                            "workitem_statuses": ["Done", "Closed", "In Progress"],
                           }
                if(metric_type!=""): filters["metric"]=metric_type

                payload = {"filter": filters,"across": across_type,"interval": "week",
                           "ou_ids": [str(df_sprint["ou_id"][val])],
                           "ou_user_filter_designation": {"sprint": ["sprint_report "]}}

                LOG.info("payload {} ".format(json.dumps(payload)))

                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_ADO"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "sprint_metrics_report_ADO"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,sort_column_name='sprint_id',
                                                                                columns=['key', 'additional_key',
                                                                                         'sprint_id', "committed_story_points",
                                                                                         "commit_delivered_story_points","delivered_story_points",
                                                                                         "creep_story_points", "delivered_creep_story_points",
                                                                                         "committed_keys", "commit_delivered_keys", "delivered_keys",
                                                                                         "creep_keys", "delivered_creep_keys",
                                                                                         "total_workitems","total_unestimated_workitems"],
                                                                                sort_values_inside_columns=["committed_keys",
                                                                                        "commit_delivered_keys","delivered_keys",
                                                                                        "creep_keys","delivered_creep_keys" ], unique_id="dashboard_partial with ou id: {}".format(str(required_tests)+" "+ str(val))  )
                if not flag:
                    list_not_match.append(str(df_sprint["ou_id"][val]))
                if not zero_flag:
                    zero_list.append(str(df_sprint["ou_id"][val]))
            except Exception as ex:
                not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

