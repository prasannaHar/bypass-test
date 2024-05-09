import pytest
import logging

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TestTrellisOptimisationTesting:

    @pytest.mark.run(order=1)
    def test_trellis_org_reports_table(self, create_generic_object, create_postgres_object,
                                            create_customer_object):
        tenant_name = create_generic_object.connection['tenant_name']
        exclude_columns = create_generic_object.api_data["trellis_org_reports_table_exclude_columns"]
        sort_columns = create_generic_object.api_data["trellis_org_reports_table_sort_columns"]

        result_flag = False
        execution_flag = False
        try:
            existingflow_query_df = create_postgres_object.postgres_database_retrieve_table_data(
                arg_table_name=tenant_name + "." + "org_dev_productivity_reports", df_flag=True)
            existingflow_query_df = create_customer_object.trellis_data_df_flatenner(data_df=existingflow_query_df)
            newflow_query_df = create_postgres_object.postgres_database_retrieve_table_data(
                arg_table_name=tenant_name + "." + "org_dev_productivity_reports_ab_test", df_flag=True)
            newflow_query_df = create_customer_object.trellis_data_df_flatenner(data_df=newflow_query_df)

            result_flag, execution_flag = create_customer_object.aggs_db_data_validator(
                response_flow1=existingflow_query_df,
                response_flow2=newflow_query_df,
                exclude_columns=exclude_columns,
                sort_column_name=sort_columns,
                tc_identifier="trellis optimisation ou db tables check")

        except Exception as ex:
            LOG.info("exception occurred {}".format(ex))

        assert result_flag == True, "existing user reports and ab test database table data is not matching"
        assert execution_flag == True, "exception occured during execution"

