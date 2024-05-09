import logging
import pytest
from src.lib.core_reusable_functions import *

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueReport:
    @pytest.mark.run(order=1)
    def _test_issues_report_data_validation__(self, create_generic_object, widgetreusable_object, drilldown_object, create_postgres_object):

        pytest.widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        pytest.drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        pytest.jira_integration_ids = create_generic_object.get_integration_id("jenkins")
        pytest.jira_issues_table_name = create_generic_object.api_data["jira_issues_table_name"]
        LOG.info("=== generating the widget payload dynamically ===")

        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=[pytest.jira_integration_ids]
        )
        LOG.info("=== retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.widget_api_url,
            arg_req_payload=widget_payload
        )
        LOG.info('===retrieve three random records from the widget response===')

        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        LOG.info('=== generating the drill-down payload dynamically ===')
        drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
            arg_req_integration_ids=[pytest.jira_integration_ids], arg_assignees=["vinaya"]
        )

        drilldown_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.drilldown_api_url,
            arg_req_payload=drilldown_payload
        )

        LOG.info('===calling the database and retrieving the records ===')

        retrieve_db_result = create_postgres_object.postgres_database_retrieve_table_data(
            arg_table_name="foo.jira_issues",
            arg_req_columns=[],
            arg_condition_columns=["key"],
            arg_condition_column_values=["LFE-2502"]
        )

    @pytest.mark.run(order=2)
    @pytest.mark.reports
    @pytest.mark.reportswithdb
    def test_dv_issuesreport_tc2_compare_assigned_issues_count_widget_vs_database(self, widgetreusable_object,
                                                                                  drilldown_object,
                                                                                  create_generic_object,
                                                                                  create_postgres_object):
        pytest.widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        pytest.drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        pytest.jira_integration_ids = create_generic_object.get_integration_id("jenkins")
        pytest.jira_issues_table_name = create_generic_object.api_data["jira_issues_table_name"]
        LOG.info('===generating the widget payload dynamically ===')
        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=pytest.jira_integration_ids
        )
        LOG.info('===retrieving the widget response ====')
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.widget_api_url,
            arg_req_payload=widget_payload
        )
        LOG.info('===retrieve three random records from the widget response ===')
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            req_condition_columns = ['assignee', "integration_id"]
            req_condition_column_values = [each_records['additional_key'], pytest.jira_integration_ids]
            if 'key' in each_records:
                req_condition_columns.append('assignee_id')
                req_condition_column_values.append(each_records['key'])
            widget_ticket_count = each_records['total_tickets']
            latest_ingestion_date = create_postgres_object.execute_query('select distinct ingested_at from '
                                                                         'foo.jira_issues order by ingested_at desc '
                                                                         'limit 1')
            latest_ingestion_date = latest_ingestion_date[0][0]
            # LOG.info("latest_ingestion_date", latest_ingestion_date)
            sql_query_needs_to_be_executed = """
                select * from (SELECT
                    key,
                    assignee,ingested_at,assignee_id,
                    ROW_NUMBER () OVER (
                        PARTITION BY key
                        ORDER BY
                            ingested_at desc
                        )
                FROM
                    """ + create_generic_object.connection[
                "tenant_name"] + "." + pytest.jira_issues_table_name + """ where integration_id='""" + str(
                pytest.jira_integration_ids) + """') x where assignee='""" + each_records[
                                                 'additional_key'] + """'  and row_number=1 and ingested_at=""" + str(
                latest_ingestion_date)
            if 'key' in each_records:
                sql_query_needs_to_be_executed = sql_query_needs_to_be_executed + "and assignee_id='" + each_records[
                    'key'] + "'"
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            assert widget_ticket_count == len(
                required_records_from_database), "ticket count is not matching.\n widget ticket count-" + str(
                widget_ticket_count) + "\n db ticket count-" + str(len(required_records_from_database))

    @pytest.mark.run(order=3)
    @pytest.mark.reports
    @pytest.mark.reportswithoutdb
    def test_dv_issuesreport_tc1_compare_assigned_issues_count_widget_vs_drilldown(self, drilldown_object,
                                                                                   widgetreusable_object):

        LOG.info('===generating the widget payload dynamically ===')
        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=pytest.jira_integration_ids
        )
        LOG.info('===retrieving the widget response===')
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.widget_api_url,
            arg_req_payload=widget_payload
        )
        LOG.info('===retrieve three random records from the widget response ===')
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            req_condition_columns = ['assignee', "integration_id"]
            req_condition_column_values = [each_records['additional_key'], pytest.jira_integration_ids]
            if 'key' in each_records:
                req_condition_columns.append('assignee_id')
                req_condition_column_values.append(each_records['key'])
            widget_ticket_count = each_records['total_tickets']
            drilldown_payload = ""
            if "key" in each_records:
                LOG.info('===generating the drill-down ticket count payload dynamically ===')
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=pytest.jira_integration_ids,
                    arg_assignees=[each_records["key"]],
                    arg_metric="ticket"
                )
            else:

                LOG.info('===generating the drill-down ticket count payload dynamically ===')
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=pytest.jira_integration_ids,
                    arg_assignees=[each_records["additional_key"]],
                    arg_metric="ticket"
                )
            drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.drilldown_api_url,
                arg_req_payload=drilldown_payload

            )
            required_tickets_from_drilldown = (drilldown_response['_metadata'])['total_count']

            if each_records['additional_key'] != "_UNASSIGNED_":
                assert widget_ticket_count == required_tickets_from_drilldown, "ticket count is not matching for " + \
                                                                               each_records[
                                                                                   'additional_key'] + ".\n widget ticket count-" + str(
                    widget_ticket_count) + "\n db ticket count-" + str(required_tickets_from_drilldown)

    @pytest.mark.run(order=4)
    @pytest.mark.reports
    @pytest.mark.reportswithdb
    def test_dv_issuesreport_tc4_compare_assigned_issues_count_widget_vs_database(self, create_generic_object, drilldown_object,
                                                                                  widgetreusable_object,
                                                                                  create_postgres_object):

        LOG.info('===retrieve data base field names===')
        jira_issues_table_columns = create_postgres_object.postgres_retrieve_table_column_names(pytest.jira_issues_table_name)
        required_filters_needs_tobe_applied = []
        issues_created_in_filter_name = ""
        for eachcolumn in jira_issues_table_columns:
            temp_column_name = eachcolumn.lower()
            if ("project" in temp_column_name) or (("issue" in temp_column_name) and ("type" in temp_column_name)):
                # or ( ("story" in temp_column_name) and ("points" in temp_column_name) ):
                required_filters_needs_tobe_applied.append(eachcolumn)
            if ("issue" in temp_column_name) and ("created" in temp_column_name):
                issues_created_in_filter_name = eachcolumn
        required_filters_needs_tobe_applied = set(required_filters_needs_tobe_applied)
        required_filters_needs_tobe_applied = list(required_filters_needs_tobe_applied)
        LOG.info("required_filters_needs_tobe_applied : {}".format(required_filters_needs_tobe_applied))
        # required_filters_needs_tobe_applied = ["project", "issue_type"]
        req_filter_names_and_value_pair = []
        req_db_filter_names_and_value_pair = []
        for eachfilter in required_filters_needs_tobe_applied:
            temp_filter_values_can_be_used = create_postgres_object.postgres_database_retrieve_dynamic_column_values(
                arg_required_tenant_name=create_generic_object.connection["tenant_name"],
                arg_required_table_name=pytest.jira_issues_table_name,
                arg_required_column_name=eachfilter,
                arg_required_integration_id=pytest.jira_integration_ids
            )
            LOG.info(temp_filter_values_can_be_used)
            if None in temp_filter_values_can_be_used:
                temp_filter_values_can_be_used.remove(None)
            req_filter_names_and_value_pair.append(
                [create_generic_object.api_data["issues_report_payload_tags_vs_db_columns_mapping"][eachfilter], temp_filter_values_can_be_used])
            req_db_filter_names_and_value_pair.append([eachfilter, temp_filter_values_can_be_used])

        LOG.info('===generating the widget payload dynamically ===')
        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )
        LOG.info("widget_payload", widget_payload)
        LOG.info("pytest.widget_api_url", pytest.widget_api_url)

        LOG.info('===retrieving the widget response ===')
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.widget_api_url,
            arg_req_payload=widget_payload
        )
        LOG.info('===retrieve three random records from the widget response ===')
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            LOG.info("assignee", each_records['additional_key'])
            req_condition_columns = ['assignee', "integration_id"]
            req_condition_column_values = [each_records['additional_key'], pytest.jira_integration_ids]
            if 'key' in each_records:
                req_condition_columns.append('assignee_id')
                req_condition_column_values.append(each_records['key'])
            widget_ticket_count = each_records['total_tickets']
            latest_ingestion_date = create_postgres_object.execute_query(
                'select distinct ingested_at from foo.jira_issues order by ingested_at desc limit 1')
            latest_ingestion_date = latest_ingestion_date[0][0]
            sql_query_needs_to_be_executed = """
                select * from (SELECT
                    key,
                    assignee,ingested_at,assignee_id,project,issue_type,
                    ROW_NUMBER () OVER (
                        PARTITION BY key
                        ORDER BY
                            ingested_at desc
                        )
                FROM
                    foo.jira_issues where integration_id='""" + str(
                pytest.jira_integration_ids) + """') x where assignee='""" + each_records[
                                                 'additional_key'] + """'  and row_number=1 and ingested_at=""" + str(
                latest_ingestion_date)

            for each_req_db_filter in req_db_filter_names_and_value_pair:
                temp_filter_values = ""
                for eachElem in each_req_db_filter[1]:
                    temp_filter_values = temp_filter_values + " '" + eachElem + "', "
                temp_filter_values = temp_filter_values[:len(temp_filter_values) - 2]
                sql_query_needs_to_be_executed = sql_query_needs_to_be_executed + " and " + each_req_db_filter[
                    0] + " in (" + temp_filter_values + ")"
            # sql_query_needs_to_be_executed = sql_query_needs_to_be_executed[:len(sql_query_needs_to_be_executed) -4]
            if 'key' in each_records:
                sql_query_needs_to_be_executed = sql_query_needs_to_be_executed + " and assignee_id='" + each_records[
                    'key'] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            LOG.info("widget_ticket_count", widget_ticket_count)
            LOG.info("len(required_records_from_database)", len(required_records_from_database))
            assert widget_ticket_count == len(
                required_records_from_database), "ticket count is not matching.\n widget ticket count-" + str(
                widget_ticket_count) + "\n db ticket count-" + str(len(required_records_from_database))

    @pytest.mark.run(order=5)
    @pytest.mark.reports
    @pytest.mark.reportswithoutdb
    def test_dv_issuesreport_tc3_compare_assigned_issues_count_widget_vs_drilldown(self, create_generic_object, drilldown_object,
                                                                                   widgetreusable_object, create_postgres_object):

        LOG.info('===retrieve data base field names ===')
        jira_issues_table_columns = create_postgres_object.postgres_retrieve_table_column_names(pytest.jira_issues_table_name)
        required_filters_needs_tobe_applied = []
        issues_created_in_filter_name = ""
        for eachcolumn in jira_issues_table_columns:
            temp_column_name = eachcolumn.lower()
            if ("project" in temp_column_name) or (("issue" in temp_column_name) and ("type" in temp_column_name)):
                # or ( ("story" in temp_column_name) and ("points" in temp_column_name) ):
                required_filters_needs_tobe_applied.append(eachcolumn)
            if ("issue" in temp_column_name) and ("created" in temp_column_name):
                issues_created_in_filter_name = eachcolumn
        required_filters_needs_tobe_applied = set(required_filters_needs_tobe_applied)
        required_filters_needs_tobe_applied = list(required_filters_needs_tobe_applied)
        LOG.info("required_filters_needs_tobe_applied", required_filters_needs_tobe_applied)
        required_filters_needs_tobe_applied = ["project", "issue_type"]
        req_filter_names_and_value_pair = []
        req_db_filter_names_and_value_pair = []

        for eachfilter in required_filters_needs_tobe_applied:
            temp_filter_values_can_be_used = create_postgres_object.postgres_database_retrieve_dynamic_column_values(
                arg_required_tenant_name=create_generic_object.connection["tenant_name"],
                arg_required_table_name=pytest.jira_issues_table_name,
                arg_required_column_name=eachfilter,
                arg_required_integration_id=pytest.jira_integration_ids
            )
            LOG.info(temp_filter_values_can_be_used)
            if None in temp_filter_values_can_be_used:
                temp_filter_values_can_be_used.remove(None)
            req_filter_names_and_value_pair.append(
                [create_generic_object.api_data["issues_report_payload_tags_vs_db_columns_mapping"][eachfilter], temp_filter_values_can_be_used])
            req_db_filter_names_and_value_pair.append([eachfilter, temp_filter_values_can_be_used])
        LOG.info('===generating the widget payload dynamically ===')
        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )
        LOG.info("widget_payload", widget_payload)
        LOG.info("pytest.widget_api_url", pytest.widget_api_url)

        LOG.info('===retrieving the widget response ===')
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.widget_api_url,
            arg_req_payload=widget_payload
        )
        LOG.info('===retrieve three random records from the widget response ===')
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        for each_records in requried_random_records:
            LOG.info("assignee", each_records['additional_key'])
            req_condition_columns = ['assignee', "integration_id"]
            req_condition_column_values = [each_records['additional_key'], pytest.jira_integration_ids]
            if 'key' in each_records:
                req_condition_columns.append('assignee_id')
                req_condition_column_values.append(each_records['key'])
            widget_ticket_count = each_records['total_tickets']
            drilldown_payload = ""
            if "key" in each_records:
                LOG.info('===generating the drill-down ticket count payload dynamically ===')
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=pytest.jira_integration_ids,
                    arg_assignees=[each_records["key"]],
                    arg_metric="ticket",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair
                )
            else:
                LOG.info('===generating the drill-down ticket count payload dynamically ===')
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=pytest.jira_integration_ids,
                    arg_assignees=[each_records["additional_key"]],
                    arg_metric="ticket",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair
                )
            drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.drilldown_api_url,
                arg_req_payload=drilldown_payload
            )
            required_tickets_from_drilldown = (drilldown_response['_metadata'])['total_count']
            if each_records['additional_key'] != "_UNASSIGNED_":
                LOG.info("widget_ticket_count - ", widget_ticket_count)
                LOG.info("required_tickets_from_drilldown - ", required_tickets_from_drilldown)
                assert widget_ticket_count == required_tickets_from_drilldown, "ticket count is not matching for " + \
                                                                               each_records[
                                                                                   'additional_key'] + ".\n widget ticket count-" + str(
                    widget_ticket_count) + "\n db ticket count-" + str(required_tickets_from_drilldown)

    @pytest.mark.run(order=6)
    @pytest.mark.reports
    @pytest.mark.reportswithoutdb
    def test_dv_issuesreport_tc5_compare_assigned_issues_count_widget_vs_drilldown(self, drilldown_object,
                                                                                   widgetreusable_object):

        number_of_months, last_month_start_date, last_month_end_date = epoch_timeStampsGenerationForRequiredTimePeriods(
            "LAST_MONTH")
        req_filter_names_and_value_pair = [
            ["issue_created_at", {"$gt": str(last_month_start_date), "$lt": str(last_month_end_date)}],
            ["issue_resolved_at", {"$gt": str(last_month_start_date), "$lt": str(last_month_end_date)}]
        ]

        LOG.info('===generating the widget payload dynamically===')
        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )
        LOG.info("widget_payload", widget_payload)
        LOG.info("pytest.widget_api_url", pytest.widget_api_url)
        LOG.info('===retrieving the widget response ===')
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.widget_api_url,
            arg_req_payload=widget_payload
        )
        LOG.info('===retrieve three random records from the widget response ===')
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            LOG.info("assignee", each_records['additional_key'])
            req_condition_columns = ['assignee', "integration_id"]
            req_condition_column_values = [each_records['additional_key'], pytest.jira_integration_ids]
            if 'key' in each_records:
                req_condition_columns.append('assignee_id')
                req_condition_column_values.append(each_records['key'])
            widget_ticket_count = each_records['total_tickets']
            drilldown_payload = ""
            if "key" in each_records:
                LOG.info('===generating the drill-down ticket count payload dynamically ===')
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=pytest.jira_integration_ids,
                    arg_assignees=[each_records["key"]],
                    arg_metric="ticket",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair
                )
            else:
                LOG.info('===generating the drill-down ticket count payload dynamically ===')
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=pytest.jira_integration_ids,
                    arg_assignees=[each_records["additional_key"]],
                    arg_metric="ticket",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair
                )
            drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.drilldown_api_url,
                arg_req_payload=drilldown_payload
            )
            required_tickets_from_drilldown = (drilldown_response['_metadata'])['total_count']
            if each_records['additional_key'] != "_UNASSIGNED_":
                LOG.info("widget_ticket_count - ", widget_ticket_count)
                LOG.info("required_tickets_from_drilldown - ", required_tickets_from_drilldown)
                assert widget_ticket_count == required_tickets_from_drilldown, "ticket count is not matching for " + \
                                                                               each_records['additional_key'] + ".\n" + "widget ticket count-" + str(widget_ticket_count) + "\n db ticket count-" + str(required_tickets_from_drilldown)

    @pytest.mark.run(order=7)
    @pytest.mark.reports
    @pytest.mark.reportswithdb
    def test_dv_issuesreport_tc6_compare_assigned_issues_count_widget_vs_database(self, drilldown_object,
                                                                                  widgetreusable_object,
                                                                                  create_postgres_object):
        number_of_months, last_month_start_date, last_month_end_date = epoch_timeStampsGenerationForRequiredTimePeriods(
            "LAST_MONTH")
        req_filter_names_and_value_pair = [
            ["issue_created_at", {"$gt": str(last_month_start_date), "$lt": str(last_month_end_date)}],
            ["issue_resolved_at", {"$gt": str(last_month_start_date), "$lt": str(last_month_end_date)}]
        ]
        req_db_filter_names_and_value_pair = [

            ["issue_created_at", ">=", last_month_start_date],
            ["issue_created_at", "<=", last_month_end_date],
            ["issue_resolved_at", ">=", last_month_start_date],
            ["issue_resolved_at", "<=", last_month_end_date]

        ]
        LOG.info('===generating the widget payload dynamically ===')
        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )
        LOG.info("widget_payload", widget_payload)
        LOG.info("pytest.widget_api_url", pytest.widget_api_url)

        LOG.info('===retrieving the widget response ===')
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.widget_api_url,
            arg_req_payload=widget_payload
        )
        LOG.info('===retrieve three random records from the widget response===')
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            LOG.info("assignee", each_records['additional_key'])
            req_condition_columns = ['assignee', "integration_id"]
            req_condition_column_values = [each_records['additional_key'], pytest.jira_integration_ids]
            if 'key' in each_records:
                req_condition_columns.append('assignee_id')
                req_condition_column_values.append(each_records['key'])
            widget_ticket_count = each_records['total_tickets']
            latest_ingestion_date = create_postgres_object.execute_query(
                'select distinct ingested_at from foo.jira_issues order by ingested_at desc limit 1')
            latest_ingestion_date = latest_ingestion_date[0][0]
            # LOG.info("latest_ingestion_date", latest_ingestion_date)
            sql_query_needs_to_be_executed = """
            
                select * from (SELECT
                    key,
                    assignee,ingested_at,assignee_id,project,issue_type,issue_created_at,issue_resolved_at,
                    ROW_NUMBER () OVER (
                        PARTITION BY key
                        ORDER BY
                            ingested_at desc
                        )
                FROM
                    foo.jira_issues where integration_id='""" + str(pytest.jira_integration_ids) + """') x where 
                    assignee='""" + each_records['additional_key'] + """'  and row_number=1 and ingested_at=""" + str(
                latest_ingestion_date)

            for each_req_db_filter in req_db_filter_names_and_value_pair:
                temp_filter_values = ""
                sql_query_needs_to_be_executed = sql_query_needs_to_be_executed + " and " + each_req_db_filter[
                    0] + " " + str(each_req_db_filter[1]) + " " + str(each_req_db_filter[2]) + " "
            if 'key' in each_records:
                sql_query_needs_to_be_executed = sql_query_needs_to_be_executed + " and assignee_id='" + each_records[
                    'key'] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            LOG.info("widget_ticket_count", widget_ticket_count)
            LOG.info("len(required_records_from_database)", len(required_records_from_database))
            # LOG.info("widget_response", widget_response)
            assert widget_ticket_count == len(
                required_records_from_database), "ticket count is not matching.\n widget ticket count-" + str(
                widget_ticket_count) + "\n db ticket count-" + str(len(required_records_from_database))

    """  
    LOG.info('===in progress one need to handle seperately
    def test_dv_issuesreport_tc1_compare_assigned_issues_count_widget_vs_drilldown():
        LOG.info('===generating the widget payload dynamically 
        widget_payload = drilldown_object.generate_issues_report_payload(
            arg_req_integration_ids=[pytest.jira_integration_ids]
            )
        #;
        LOG.info('===retrieving the widget response
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_authorization_token=Authorization_token, 
            arg_req_api=pytest.widget_api_url, 
            arg_req_payload=widget_payload
            )
        #;
        LOG.info('===retrieve three random records from the widget response
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        #;
        for each_records in requried_random_records:
            req_condition_columns = ['assignee', "integration_id"]
            req_condition_column_values = [each_records['additional_key'], pytest.jira_integration_ids]
            if('key' in each_records):
                req_condition_columns.append('assignee_id')
                req_condition_column_values.append(each_records['key'])
            widget_ticket_count = each_records['total_tickets']
            drilldown_payload = ""
            if("key" in each_records):
                LOG.info('===generating the drill-down ticket count payload dynamically 
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=[pytest.jira_integration_ids], 
                    arg_assignees=[each_records["key"]], 
                    arg_metric="ticket"
                    )
            else:
                LOG.info('===generating the drill-down ticket count payload dynamically 
                drilldown_payload = drilldown_object.generate_issues_report_drilldown_payload(
                    arg_req_integration_ids=[pytest.jira_integration_ids], 
                    arg_assignees=[each_records["additional_key"]], 
                    arg_metric="ticket"
                    )
            # LOG.info(drilldown_payload)
            drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_authorization_token=Authorization_token, 
                arg_req_api=pytest.drilldown_api_url, 
                arg_req_payload=drilldown_payload
                )
            required_tickets_from_drilldown = (drilldown_response['_metadata'])['total_count']
            # LOG.info("pytest.drilldown_api_url", pytest.drilldown_api_url)
            # LOG.info("drilldown_response", drilldown_response)
            if( each_records['additional_key'] != "_UNASSIGNED_"):
                assert widget_ticket_count==required_tickets_from_drilldown, "ticket count is not matching for "+ each_records['additional_key'] + ".\n widget ticket count-"+str(widget_ticket_count)+"\n db ticket count-"+ str(required_tickets_from_drilldown)
    """
