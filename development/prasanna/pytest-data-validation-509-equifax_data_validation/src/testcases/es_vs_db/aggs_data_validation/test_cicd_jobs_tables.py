import inspect

import pandas as pd
import pytest
import logging
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from datetime import date, timedelta


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestAggrCICDJobs:
    generic_object = TGhelper()
    cicd_tables = generic_object.api_data["CICD_DATABASE_TABLES"]
    org_units = generic_object.env.get("aggstestintegrationids", [])

    @pytest.mark.parametrize("cicd_table", cicd_tables)
    @pytest.mark.parametrize("each_group", org_units)
    def test_cicd_tables(self, cicd_table, each_group, create_generic_object, create_postgres_object,
                         create_customer_object, create_sql_query_object):
        """
        Test case to check data consistency in CICD tables
        """

        gt = str(date.today() - timedelta(days = create_generic_object.env.get(
            "cicd_job_count_data_collection_span", 11
        )))
        lt = str(date.today() - timedelta(days = 1))
        tenant_name = create_generic_object.connection["tenant_name"]
        exclude_columns = create_generic_object.api_data["cicd_database_exclude_columns_" + cicd_table]
        sort_columns = create_generic_object.api_data["cicd_database_sort_columns_" + cicd_table]
        no_data_in_both_df = []
        no_data_list = []
        mismatch_data_list = []

        LOG.info(f"\n\n==== Verifying database table:  {cicd_table} - Integration IDs: {each_group}")
        try:
            (old_aggr_flow, new_aggr_flow) = each_group.split(":")

            if cicd_table == "cicd_jobs":
                old_aggr_query = create_sql_query_object.cicd_jobs_query(
                    tenant_name=tenant_name, integration_id=old_aggr_flow
                )

                new_aggr_query = create_sql_query_object.cicd_jobs_query(
                    tenant_name=tenant_name, integration_id=new_aggr_flow
                )
            elif cicd_table == "cicd_job_runs":
                old_aggr_query = create_sql_query_object.cicd_job_runs_query(
                    tenant_name=tenant_name, integration_id=old_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )

                new_aggr_query = create_sql_query_object.cicd_job_runs_query(
                    tenant_name=tenant_name, integration_id=new_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )
            elif cicd_table == "cicd_job_run_stages":
                old_aggr_query = create_sql_query_object.cicd_job_run_stages_query(
                    tenant_name=tenant_name, integration_id=old_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )

                new_aggr_query = create_sql_query_object.cicd_job_run_stages_query(
                    tenant_name=tenant_name, integration_id=new_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )
            elif cicd_table == "cicd_job_run_stage_steps":
                old_aggr_query = create_sql_query_object.cicd_job_run_stage_steps_query(
                    tenant_name=tenant_name, integration_id=old_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )

                new_aggr_query = create_sql_query_object.cicd_job_run_stage_steps_query(
                    tenant_name=tenant_name, integration_id=new_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )
            elif cicd_table == "cicd_job_run_params":
                old_aggr_query = create_sql_query_object.cicd_job_run_params_query(
                    tenant_name=tenant_name, integration_id=old_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )

                new_aggr_query = create_sql_query_object.cicd_job_run_params_query(
                    tenant_name=tenant_name, integration_id=new_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )
            elif cicd_table == "cicd_scm_mapping":
                old_aggr_query = create_sql_query_object.cicd_scm_mapping_query(
                    tenant_name=tenant_name, integration_id=old_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )

                new_aggr_query = create_sql_query_object.cicd_scm_mapping_query(
                    tenant_name=tenant_name, integration_id=new_aggr_flow,
                    time_range = {"$gt": gt, "$lt": lt}
                )

            LOG.info(f"---- old_aggr_query: {old_aggr_query}")
            LOG.info(f"---- new_aggr_query: {new_aggr_query}")
            oldflow_query_df = pd.DataFrame()
            newflow_query_df = pd.DataFrame()

            oldflow_query_df = create_postgres_object.execute_query(query=old_aggr_query, df_flag=True)
            newflow_query_df = create_postgres_object.execute_query(query=new_aggr_query, df_flag=True)

            if len(oldflow_query_df) != 0 or len(newflow_query_df) != 0:
                result_flag, execution_flag = create_customer_object.aggs_db_data_validator(
                        response_flow1=oldflow_query_df,
                        response_flow2=newflow_query_df,
                        exclude_columns=exclude_columns,
                        sort_column_name=sort_columns,
                        tc_identifier=cicd_table + "-int_ids:- " + each_group)

                if not execution_flag:
                    no_data_list.append(each_group)
                if not result_flag:
                    mismatch_data_list.append(each_group)
            else:
                no_data_in_both_df.append(each_group)
        except Exception as e:
            LOG.info(f"Error while comparing the results: {e}")
            mismatch_data_list.append(each_group)

        LOG.info("Integrations with one or both with - no data List {}".format(set(no_data_list)))
        LOG.info("Integrations mapping with mismatch data List {}".format(set(mismatch_data_list)))
        LOG.info("Integrations mapping where no data is found in both the integration -{}".format(
            set(no_data_in_both_df)))
        assert len(no_data_list) == 0, "not executed List- list is {}".format(set(no_data_list))
        assert len(mismatch_data_list) == 0, "Integrations mapping with mismatch data - list is {}".format(
            set(mismatch_data_list))

        if no_data_in_both_df:
            LOG.info("Skipping the test case as the tables have no data to check.")
            pytest.skip("Skipping the test case as the tables have no data to check.")