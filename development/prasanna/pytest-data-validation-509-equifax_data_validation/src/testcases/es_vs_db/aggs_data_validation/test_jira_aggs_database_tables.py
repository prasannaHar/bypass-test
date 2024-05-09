import pytest
import logging
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestAggsJiraDataValidation:
    generic_object = TGhelper()
    jira_tables = generic_object.api_data["jira_database_tables"]

    @pytest.mark.run(order=1)
    @pytest.mark.aggstcsunit
    @pytest.mark.parametrize("jira_table", jira_tables)
    def test_aggs_testing_jira_issues_table(self, jira_table, create_generic_object, create_postgres_object,
                                            create_customer_object):
        tenant_name = create_generic_object.connection['tenant_name']
        jira_int_map = create_generic_object.env["aggstestintegrationids"]
        exclude_columns = create_generic_object.api_data["jira_database_exclude_columns_" + jira_table]
        sort_columns = create_generic_object.api_data["jira_database_sort_columns_" + jira_table]
        no_data_list = []
        mismatch_data_list = []
        for each_int_map in jira_int_map:
            existing_flow_id, new_flow_id = jira_int_map[0].split(":")
            try:
                ## retrieve latest ingested at
                condition_columns_new_flow = ["str:integration_id:" + new_flow_id]
                condition_columns_existing_flow = ["str:integration_id:" + existing_flow_id]
                if jira_table in ["jiraprojects", "jirafields"]:
                    condition_columns_new_flow = ["str:integrationid:" + new_flow_id]
                    condition_columns_existing_flow = ["str:integrationid:" + existing_flow_id]
                elif jira_table in ["jira_users"]:
                    condition_columns_new_flow = ["str:integ_id:" + new_flow_id]
                    condition_columns_existing_flow = ["str:integ_id:" + existing_flow_id]
                if jira_table == "jira_issues":
                    query_ingested_at = "select distinct ingested_at from " + tenant_name + \
                                        "." + jira_table + " order by ingested_at desc limit 1"
                    LOG.info("Query for ingested at {}".format(query_ingested_at))
                    latest_ingested_at = str((create_postgres_object.execute_query(query_ingested_at))[0][0])
                    condition_columns_new_flow.append("num:ingested_at:" + latest_ingested_at)
                    condition_columns_existing_flow.append("num:ingested_at:" + latest_ingested_at)
                ## retrieve integration data - existing flow
                existingflow_query_df = create_postgres_object.postgres_database_retrieve_table_data(
                    arg_table_name=tenant_name + "." + jira_table, df_flag=True,
                    arg_condition_columns=condition_columns_existing_flow)
                ## retrieve integration data - new flow
                newflow_query_df = create_postgres_object.postgres_database_retrieve_table_data(
                    arg_table_name=tenant_name + "." + jira_table, df_flag=True,
                    arg_condition_columns=condition_columns_new_flow)
                ## existing flow integration v/s new flow integration data comparion
                result_flag, execution_flag = create_customer_object.aggs_db_data_validator(
                    response_flow1=existingflow_query_df,
                    response_flow2=newflow_query_df,
                    exclude_columns=exclude_columns,
                    sort_column_name=sort_columns,
                    tc_identifier=jira_table)
                if not execution_flag:
                    no_data_list.append(each_int_map)
                if not result_flag:
                    mismatch_data_list.append(each_int_map)
            except Exception as ex:
                mismatch_data_list.append(each_int_map)

        LOG.info("integrations with no data List {}".format(set(no_data_list)))
        LOG.info("Integrations mapping with mismatch data List {}".format(set(mismatch_data_list)))
        assert len(no_data_list) == 0, "not executed List- list is {}".format(set(no_data_list))
        assert len(mismatch_data_list) == 0, "Integrations mapping with mismatch data - list is {}".format(
            set(mismatch_data_list))
