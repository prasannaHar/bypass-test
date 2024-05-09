import logging
import pandas as pd
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["issues-report-across_type_ADO"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    def test_issues_report_ADO_es(self, across_type, create_generic_object,
                                  create_customer_object, get_integration_obj, drilldown_object):
        project_names = create_generic_object.env["project_names"]
        stacks = create_generic_object.api_data["issues-report-stacks_ADO"]
        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        for val in df_sprint.index:
            for stacks_val in stacks:
                try:
                    required_filters_needs_tobe_applied = ["workitem_projects"]
                    filter_value = [project_names]

                    req_filter_names_and_value_pair = []
                    for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                        req_filter_names_and_value_pair.append([eachfilter, eachvalue])
                    if "-" in across_type:
                        across = across_type.split("-")
                        across_type = across[0]
                        interval_val = across[1]
                    if "custom_field" in stacks_val:
                        stack = stacks_val.split("-")
                        stack_filter = stack[1]
                        stacks_val = stack[0]
                        LOG.info("Custom Stack : " + stack_filter)

                        req_filter_names_and_value_pair.append(["custom_stacks", [stack_filter]])

                    payload = drilldown_object.generate_issues_report_payload(
                        arg_req_integration_ids=get_integration_obj,
                        arg_metric=["ticket"],
                        arg_product_id=create_generic_object.env["project_id"],
                        arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                        arg_across=across_type,
                        arg_stacks=stacks_val,
                        arg_ou_id=[str(df_sprint["ou_id"][val])]
                    )
                    if len(interval_val) != 0:
                        payload["interval"] = interval_val
                    LOG.info("payload {} ".format(payload))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "ADO_tickets_report"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "ADO_tickets_report"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db_ADO(es_response, db_response,
                                                                                    across_type,
                                                                                    columns=['key', 'additional_key',
                                                                                             'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(across_type)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(str(df_sprint["ou_id"][val]))
                    if not zero_flag:
                        zero_list.append(str(df_sprint["ou_id"][val]))
                except Exception as ex:
                    not_executed_list.append(str(df_sprint["ou_id"][val]))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- for Across : " + across_type + " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across :" + across_type + "  {}".format(
            set(list_not_match))
