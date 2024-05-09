import inspect

import pandas as pd
import pytest
import logging
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestAggsJiraDataValidation:
    generic_object = TGhelper()
    scm_tables = generic_object.api_data["scm_database_tables"]

    @pytest.mark.run(order=1)
    # @pytest.mark.aggstcsunit
    @pytest.mark.parametrize("scm_table", scm_tables)
    def test_aggs_testing_scm_commit_table(self,scm_table, create_generic_object, create_postgres_object,
                                           create_customer_object, create_sql_query_object):
        """integration used in the test should be having limited data else the code might break ,
        try to use a project with limited data in the integration """
        # scm_table = "scm_issues"
        tenant_name = create_generic_object.connection['tenant_name']
        scm_int_map = create_generic_object.env["aggstestintegrationids"]
        exclude_columns = create_generic_object.api_data["scm_database_exclude_columns_" + scm_table]
        sort_columns = create_generic_object.api_data["scm_database_sort_columns_" + scm_table]
        no_data_in_both_df = []
        no_data_list = []
        mismatch_data_list = []


        for each_int_map in scm_int_map:
            existing_flow_id, new_flow_id = scm_int_map[0].split(":")
            try:

                if scm_table in ["scm_pullrequest_labels"]:
                    new_flow_query = create_sql_query_object.scm_pullrequest_labels_query(integration_id=new_flow_id,
                                                                            tenant_name=tenant_name)
                    old_flow_query = create_sql_query_object.scm_pullrequest_labels_query(integration_id=existing_flow_id,
                                                                            tenant_name=tenant_name)
                if scm_table in ["scm_pullrequest_reviews"]:
                    new_flow_query = create_sql_query_object.scm_pullrequest_reviews_query(integration_id=new_flow_id,
                                                                             tenant_name=tenant_name)
                    old_flow_query = create_sql_query_object.scm_pullrequest_reviews_query(integration_id=existing_flow_id,
                                                                             tenant_name=tenant_name)
                if scm_table in ["scm_file_commits"]:
                    new_flow_query = create_sql_query_object.scm_file_commits_query(integration_id=new_flow_id,
                                                                      tenant_name=tenant_name)
                    old_flow_query = create_sql_query_object.scm_file_commits_query(integration_id=existing_flow_id,
                                                                      tenant_name=tenant_name)



                if scm_table in ['github_project_columns']:
                    new_flow_query = create_sql_query_object.scm_github_project_cols_query(tenant_name=tenant_name,
                                                                             integration_id=new_flow_id)

                    old_flow_query = create_sql_query_object.scm_github_project_cols_query(tenant_name=tenant_name,
                                                                             integration_id=existing_flow_id)

                if scm_table in ["github_cards"]:
                    new_flow_query = create_sql_query_object.github_cards_query(tenant_name=tenant_name,
                                                                  integration_id=new_flow_id)

                    old_flow_query = create_sql_query_object.github_cards_query(tenant_name=tenant_name,
                                                                  integration_id=existing_flow_id)

                if scm_table in ['scm_commits', 'scm_files', 'scm_pullrequests', 'scm_pullrequests_jira_mappings',
                                 'scm_pullrequests_workitem_mappings', 'scm_tags', 'scm_issues',
                                 "scm_commit_jira_mappings", "scm_commit_workitem_mappings", 'github_projects',
                                 'gittechnologies', 'gitrepositories']:
                    table_name = tenant_name + "." + scm_table
                    if scm_table not in ['scm_pullrequests_jira_mappings', 'scm_pullrequests_workitem_mappings',
                                         'scm_commit_workitem_mappings', 'scm_commit_jira_mappings', 'gitrepositories']:
                        new_flow_query = create_sql_query_object.star_query(table_name=table_name, integration_id=new_flow_id)
                        old_flow_query = create_sql_query_object.star_query(table_name=table_name, integration_id=existing_flow_id)
                    elif scm_table in ["scm_commit_jira_mappings"]:
                        new_flow_query = create_sql_query_object.star_query(table_name=table_name, scm_integ_id=new_flow_id)
                        old_flow_query = create_sql_query_object.star_query(table_name=table_name, scm_integ_id=existing_flow_id)

                    elif scm_table in ['gitrepositories']:

                        new_flow_query = create_sql_query_object.scm_gitrepositories_query(table_name=table_name,
                                                                             integration_id=new_flow_id)
                        old_flow_query = create_sql_query_object.scm_gitrepositories_query(table_name=table_name,
                                                                             integration_id=existing_flow_id)

                    else:
                        new_flow_query = create_sql_query_object.star_query(table_name=table_name, scm_integration_id=new_flow_id)
                        old_flow_query = create_sql_query_object.star_query(table_name=table_name,
                                                              scm_integration_id=existing_flow_id)
                #         ## retrieve integration data - existing flow
                LOG.info("new_flow_query--{}".format(new_flow_query))
                LOG.info("old_flow_query--{}".format(old_flow_query))
                existingflow_query_df = pd.DataFrame()
                newflow_query_df = pd.DataFrame()

                # if len(create_postgres_object.execute_query(query=old_flow_query, df_flag=True)) != 0:
                existingflow_query_df = create_postgres_object.execute_query(query=old_flow_query, df_flag=True)
                #         ## retrieve integration data - new flow
                # if len(create_postgres_object.execute_query(query=new_flow_query, df_flag=True)) != 0:
                newflow_query_df = create_postgres_object.execute_query(query=new_flow_query, df_flag=True)

                """existing flow integration v/s new flow integration data comparion"""
                if len(existingflow_query_df) != 0 or len(newflow_query_df) != 0:
                    result_flag, execution_flag = create_customer_object.aggs_db_data_validator(
                        response_flow1=existingflow_query_df,
                        response_flow2=newflow_query_df,
                        exclude_columns=exclude_columns,
                        sort_column_name=sort_columns,
                        tc_identifier=scm_table + "-int_ids:- " + each_int_map)

                    if not execution_flag:
                        no_data_list.append(each_int_map)
                    if not result_flag:
                        mismatch_data_list.append(each_int_map)

                else:
                    no_data_in_both_df.append(each_int_map)
            except Exception as ex:
                mismatch_data_list.append(each_int_map)

            LOG.info("integrations with  one or both with - no data List {}".format(set(no_data_list)))
            LOG.info("Integrations mapping with mismatch data List {}".format(set(mismatch_data_list)))
            LOG.info("Integrations mapping where no data is found in both the integration -{}".format(
                set(no_data_in_both_df)))
            assert len(no_data_list) == 0, "not executed List- list is {}".format(set(no_data_list))
            assert len(no_data_in_both_df) == 0, "no data in both executed List- list is {}".format(set(no_data_list))
            assert len(mismatch_data_list) == 0, "Integrations mapping with mismatch data - list is {}".format(
                set(mismatch_data_list))

    def test_scm_commit_pullrequest_mappings(self, create_postgres_object, create_sql_query_object,
                                             create_generic_object):
        tenant_name = create_generic_object.connection['tenant_name']
        scm_int_map = create_generic_object.env["aggstestintegrationids"]

        no_data_list = []
        mismatch_data_list = []

        try:
            for each_int_map in scm_int_map:
                existing_flow_id, new_flow_id = scm_int_map[0].split(":")

                query = create_sql_query_object.scm_commit_pullrequest_mappings_except_query(tenant_name=tenant_name,
                                                                                             integration_id1=existing_flow_id,
                                                                                             integration_id2=new_flow_id)

                LOG.info("query---{}".format(query))

                result = create_postgres_object.execute_query(query=query, df_flag=True)

                LOG.info("result---{}".format(result))
                if len(result) != 0:
                    pd.DataFrame("**********", result).to_csv(
                        "log_updates/" + str(inspect.stack()[0][3]) + '.csv', header=True, index=False, mode='a')
                    mismatch_data_list.append(each_int_map)
                else:
                    no_data_list.append(each_int_map)
                    LOG.info("Test case executed with no issues successfully")

        except Exception as ex:
            LOG.info("Not executed")
            LOG.info(ex)

        if len(no_data_list) != 0:
            LOG.info(
                "---Result is having no data , both the integrations have no differences---{}".format(no_data_list))

        assert len(mismatch_data_list) == 0, "Integrations mapping with mismatch data - list is {}".format(
            set(mismatch_data_list))
