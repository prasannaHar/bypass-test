import json
import logging
import pandas as pd
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueCountsStatCustomer:

    @pytest.mark.run(order=1)
    def test_ticket_counts_report(self, create_generic_object,
                                  create_customer_object, get_integration_obj):
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint.index:
            try:
                filters = {"product_id": create_generic_object.env["project_id"],
                           "integration_ids": get_integration_obj}
                payload = {"filter": filters, "across": "workitem_created_at",
                           "ou_ids": [str(df_sprint["ou_id"][val])],
                           "ou_user_filter_designation": {"sprint": ["sprint_report "]}}

                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "stage_bounce_report"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + create_generic_object.api_data[
                                  "stage_bounce_report"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_ADO(es_response, db_response,
                                                                                across_type="comparing_es_vs_db_ADO",
                                                                                columns=['key', 'additional_key',
                                                                                         'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(str(df_sprint["ou_id"][val]))
                if not zero_flag:
                    zero_list.append(str(df_sprint["ou_id"][val]))
            except Exception as ex:
                not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across {}".format(
            set(list_not_match))