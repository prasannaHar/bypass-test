import pytest
import logging

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TestTrellisOptimisationTesting:

    @pytest.mark.run(order=1)
    def test_trellis_org_reports_table_es(self, create_generic_object, create_postgres_object,
                                        create_sql_query_object, create_customer_object):
        tenant_name = create_generic_object.connection['tenant_name']

        result_flag = False
        execution_flag = False
        try:            
            existingflow_query= create_sql_query_object.terllis_optimisation_query_generator_org_reports(
                                                tenant_name=tenant_name,
                                                table_name="org_dev_productivity_es_reports")
            existingflow_query_df = create_postgres_object.execute_query(query = existingflow_query, df_flag=True)            
            existingflow_query_df = create_customer_object.trellis_data_df_flatenner(data_df=existingflow_query_df)

            newflow_query= create_sql_query_object.terllis_optimisation_query_generator_org_reports(
                                                tenant_name=tenant_name,
                                                table_name="org_dev_productivity_reports_v2")
            newflow_query_df = create_postgres_object.execute_query(query = newflow_query, df_flag=True)            
            newflow_query_df = create_customer_object.trellis_data_df_flatenner(data_df=newflow_query_df)

            result_flag, execution_flag = create_customer_object.aggs_db_data_validator(
                response_flow1=existingflow_query_df,
                response_flow2=newflow_query_df,
                tc_identifier="ES - trellis optimisation ou db tables check")

        except Exception as ex:
            LOG.info("exception occurred {}".format(ex))

        assert result_flag == True, "existing user reports and ab test database table data is not matching"
        assert execution_flag == True, "exception occured during execution"

