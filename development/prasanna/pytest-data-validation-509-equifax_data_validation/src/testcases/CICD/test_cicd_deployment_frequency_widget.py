import json
import logging
import pytest
import random

from src.testcases.CICD.conftest import CICDDoraDF

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

cicd_env = CICDDoraDF()

class TestCICDDeploymentFrequency():

    @pytest.mark.parametrize(
            'cicd_df_pipeline_filters', cicd_env.cicd_filters.get("dora_df", {}).get("filter_combinations", [])
    )
    @pytest.mark.cicdregression
    def test_cicd_df_with_filter_combinations(self,
                                              widget_schema_validation,
                                              create_widget_helper_object,
                                              cicd_df_pipeline_filters,
                                              datastore_prep):
        """
        Test case to verify the working of CICD DF widget and drilldown with variety of filter combinations at the
        workflow profile level.

        Assertions:
        1. Record count consistency between day-wise, week-wise and month-wise job execution count grouping.
        2. CICD DF widget schema verification.
        3. Dora Deployment frequency value and grading logic verification.
        4. Day-wise drilldown and verifying the record count between DF widget and DF drilldown.
        5. Week-wise drilldown and verifying the record count between DF widget and DF drilldown.
        6. Month-wise drilldown and verifying the record count between DF widget and DF drilldown.
        """
        LOG.info(f"\n\n====== Executing test_cicd_df_with_filter_combinations - {cicd_df_pipeline_filters} ======")

        org_ids = cicd_env.org_ids
        filter_list = []

        if not org_ids:
            LOG.info("Skipping this test case as CICD integration configuration is not found for this tenant.")
            pytest.skip("Skipping this test case as CICD integration configuration is not found for this tenant.")

        # Pipeline filter combinations.
        product_application_name = cicd_df_pipeline_filters[0]
        calculation_field = cicd_df_pipeline_filters[1]
        pipeline_filter_eq = cicd_df_pipeline_filters[2]
        pipeline_filter_contains = cicd_df_pipeline_filters[3]
        pipeline_filter_not_eq = cicd_df_pipeline_filters[4]
        pipeline_filter_starts_with = cicd_df_pipeline_filters[5]

        filter_list, pipeline_filter_eq = cicd_env.cicd_job_run_helper.align_params_in_expected_format(
            params = pipeline_filter_eq, filter_list=filter_list
        )

        filter_list, pipeline_filter_contains = cicd_env.cicd_job_run_helper.align_params_in_expected_format(
            params = pipeline_filter_contains, filter_list=filter_list
        )

        filter_list, pipeline_filter_not_eq = cicd_env.cicd_job_run_helper.align_params_in_expected_format(
            params = pipeline_filter_not_eq, filter_list=filter_list
        )

        filter_list, pipeline_filter_starts_with = cicd_env.cicd_job_run_helper.align_params_in_expected_format(
            params = pipeline_filter_starts_with, filter_list=filter_list
        )

        is_ci_job = cicd_df_pipeline_filters[6]
        is_cd_job = cicd_df_pipeline_filters[7]
        pipeline_selection = cicd_df_pipeline_filters[8]

        # Getting time_range for executing widget API.
        try:
            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                LOG.info("===== Reading time range from the Harness File store =====")
                # Loading data from datastore.
                with open(datastore_prep[product_application_name]["checkpoint"], "r") as json_file:
                    time_range = json.load(json_file)

            # End date is considered as now() - 2d.
            else:
                LOG.info("===== Datastore disabled. Calculating time range manually =====")
                time_range = cicd_env.api_helper_obj.cicd_fetch_time_range()
        except Exception as e:
            LOG.error(f" ===== Error occurred while calculating time range. Error: {e}")
            pytest.skip(f"Error occurred while calculating time range. Error: {e}")

        total_days_in_time_range = round(
            (int(time_range["$lt"]) - int(time_range["$gt"])) / 86400
        )
        LOG.info(f"Time_range for DF widget: {time_range}, total_days: {total_days_in_time_range}")

        # Checking if record match check and datastore check needs to be skipped.
        # These checks will be skipped for filters whose data is available through Pipeline
        # detail API call.
        skip_datastore_check = any(skip_filter for skip_filter in \
                                   cicd_env.filters_to_skip_for_datastore_check \
                                    if skip_filter in filter_list)
        LOG.info(f"===== skip_datastore_check: {skip_datastore_check}")

        tests_run = False
        for org_id in org_ids:
            # Fetching integration IDs.
            product_integration_id = cicd_env.integration_details[org_id].get(product_application_name)
            if not product_integration_id:
                continue

            tests_run = True

            df_widget_data_present = False
            df_widget_data_collection_trial = 1
            selected_pipelines = {}

            # Attempting 5 retries to select filter combinations with non-null values in the widget.
            while not df_widget_data_present and df_widget_data_collection_trial < 6:
                LOG.info(f"Trial {df_widget_data_collection_trial} for selecting filter values "
                         "for workflow profile.")

                # Getting CICD DF pipeline filters payload.
                df_pipeline_filters = cicd_env.cicd_job_run_helper.generate_cicd_dora_filters_payload(
                    integration_ids=product_integration_id,
                    pipeline_filter_eq = pipeline_filter_eq,
                    pipeline_filter_not_eq = pipeline_filter_not_eq,
                    pipeline_filter_contains = pipeline_filter_contains,
                    pipeline_filter_starts_with = pipeline_filter_starts_with
                )
                if not df_pipeline_filters:
                    df_widget_data_collection_trial = df_widget_data_collection_trial + 1
                    continue

                if pipeline_selection == "all":
                    selected_pipelines_id = []
                    LOG.info("All pipelines selected")
                else:
                    selected_pipelines = cicd_env.mapper_lib.dora_fetch_selected_pipelines(
                        integration_ids=product_integration_id
                    )
                    selected_pipelines_id = selected_pipelines["job_ids"]
                    LOG.info(f"Workflow profile pipelines count: {len(selected_pipelines_id)}."
                                f" Selected Pipelines: {selected_pipelines}")

                #### Workflow profile creation ####
                LOG.info(f"Creating workflow profile with DF filters: {df_pipeline_filters}")

                # Creating the workflow profile.
                workflow_profile = cicd_env.mapper_lib.create_cicd_workflow_profile(
                    profile_filters={
                        "integration_ids": product_integration_id,
                        "ou_id": org_id,
                        "application": product_application_name
                    },
                    df_filters={
                        "calculation_field": calculation_field,
                        "filters": df_pipeline_filters,
                        "values_list": selected_pipelines_id
                    }
                )
                LOG.info(f"Workflow profile created: {workflow_profile}")
                #### Workflow profile created ####

                #### CICD Deployment frequency widget Start ####
                LOG.info("Executing the CICD deployment frequency widget API call")

                arg_ou_ids = [org_id]

                # Executing CFR widget API call.
                resp = cicd_env.mapper_lib.execute_dora_profile_widget_api(
                    widget_url = cicd_env.dora_df_widget_url,
                    org_id = arg_ou_ids, time_range = time_range
                )

                day = resp['time_series']['day']
                week = resp['time_series']['week']
                month = resp['time_series']['month']
                total_deployment = resp['stats']["total_deployment"]
                actual_count_per_day = resp['stats']['count_per_day']
                actual_count_per_week = resp['stats']['count_per_week']
                actual_count_per_month = resp['stats']['count_per_month']
                band = resp["stats"]["band"]

                # Widget does not have data with the select filter values.
                if total_deployment == 0 or actual_count_per_day == 0.0:
                    df_widget_data_collection_trial = df_widget_data_collection_trial + 1
                    LOG.info("Filter values selected does not have enough data to validate. Re-trying "
                             "another filter value combination to resume the test case execution.")
                    # Delete Workflow profile
                    cicd_env.mapper_lib.delete_workflow_profile(workflow_id=workflow_profile["id"])
                else:
                    df_widget_data_present = True

            if not df_widget_data_present or df_widget_data_collection_trial == 6:
                LOG.info("Skipping this test case as the filters does not have data to validate.")
                pytest.skip("Skipping this test case as the filters does not have data to validate.")

            # Calculating job execution count from day list, week list and month list.
            # DF API call returns job execution count by day, week and month.
            day_job_exec_count, week_job_exec_count, month_job_exec_count = \
                cicd_env.mapper_lib.calculate_record_cnt_by_group(
                    application=product_application_name,
                    day_wise_records=day,
                    week_wise_records=week,
                    month_wise_records=month,
                )

            record_count_consistency_across_groups = (day_job_exec_count == week_job_exec_count and \
                    week_job_exec_count == month_job_exec_count)

            #### CICD DF Drill-down verification Start ####
            # Day wise verification.
            LOG.info("Verifying day-wise records consistency with drilldown widget.")
            df_widget_drilldown_day_wise_discrepancy, day_random_record = cicd_env.mapper_lib.dora_widget_drill_down_verification(
                records = day,
                org_ids = [org_id],
                application = product_application_name,
                group_type = "day",
                filters = df_pipeline_filters,
                values_list = selected_pipelines_id,
                skip_record_match_check=skip_datastore_check
            )

            LOG.info("Verifying week-wise records consistency with drilldown widget.")
            # Week wise verification.
            df_widget_drilldown_week_wise_discrepancy, week_random_record = cicd_env.mapper_lib.dora_widget_drill_down_verification(
                records = week,
                org_ids = [org_id],
                application = product_application_name,
                widget_time_range = time_range,
                group_type = "week",
                filters = df_pipeline_filters,
                values_list = selected_pipelines_id,
                skip_record_match_check=skip_datastore_check
            )

            LOG.info("Verifying month-wise records consistency with drilldown widget.")
            # Month wise verification.
            df_widget_drilldown_month_wise_discrepancy, month_random_record = cicd_env.mapper_lib.dora_widget_drill_down_verification(
                records = month,
                org_ids = [org_id],
                application = product_application_name,
                widget_time_range = time_range,
                group_type = "month",
                filters = df_pipeline_filters,
                values_list = selected_pipelines_id,
                skip_record_match_check=skip_datastore_check
            )

            # If datastore check is not disabled, verify the widget records with the datastore.
            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                skip_fields_from_datastore_check = cicd_env.integration_details[org_id].get(
                        "skip_fields_from_datastore_check", []
                    )
                # Checking if datastore check needs to be skipped due to unsupported filter.
                if skip_datastore_check:
                    LOG.info(f"Skipped datastore check for filters: '{df_pipeline_filters}' as it contains"
                            f" filter not yet supported for datastore check.")
                else:
                    # Fetching records from the datastore.
                    with open(datastore_prep[product_application_name]["datastore"], "r") as json_file:
                        data = json.load(json_file)

                    filtered_job_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                            datastore=data, filters=df_pipeline_filters,
                            time_range=time_range, calculation_field=calculation_field,
                            values_list=selected_pipelines.get("job_names", []),
                            skip_fields=skip_fields_from_datastore_check
                    )

            #### Delete Workflow profile Start####
            LOG.info("Deleting Workflow Profile.")
            cicd_env.mapper_lib.delete_workflow_profile(workflow_id=workflow_profile["id"])
            LOG.info(f"Workflow Profile '{workflow_profile['id']}' deleted.")
            #### Delete Workflow profile End ####

            #### Assertions ####

            LOG.info("DF Widget Schema Verification start...")
            create_widget_helper_object.schema_validations(
                widget_schema_validation.create_deployment_frequency(application = product_application_name),
                resp,
                "Schema validation failed for CICD Deployment Frequenct widget ",
            )
            LOG.info("DF Widget Schema Verification completed...")

            LOG.info("[Data Consistency Check] Verifying job execution count in"
                     " day wise, week wise and month wise records.")
            LOG.info(f"day_job_exec_count: {day_job_exec_count}, week_job_exec_count: {week_job_exec_count},"
                     f" month_job_exec_count: {month_job_exec_count} ")
            assert record_count_consistency_across_groups, "day_count / week_count / month_count not matching."
            LOG.info("[Data Consistency Check] Job execution count verification complete.")

            LOG.info("[Data Consistency Check - Day wise] DF Widget -> DF Drilldown verification start...")
            LOG.info(f"df_widget_drilldown_day_wise_discrepancy: {df_widget_drilldown_day_wise_discrepancy}")
            assert len(df_widget_drilldown_day_wise_discrepancy) == 0, "Records mis-match between CICD Deployment" \
                "  Frequency widget and CICD Deployment Frequency Drilldown.\n Unmatched" \
                f" records: {df_widget_drilldown_day_wise_discrepancy}"
            LOG.info("[Data Consistency Check - Day wise] DF Widget -> DF Drilldown verification complete...")

            LOG.info("[Data Accuracy Check] Verifying CICD DF band calculation logic.")
            # Asserting deployment frequency rate.
            if total_deployment / total_days_in_time_range == 0:
                assert total_deployment == actual_count_per_day, \
                        f"Deployment frequency rate assertion failed. Total " \
                        f"deployment: {total_deployment} , actual_count_per_day: " \
                        f"{actual_count_per_day}"
            else:
                expected_count_per_day = total_deployment / total_days_in_time_range
                expected_count_per_week = expected_count_per_day * 7
                expected_count_per_month = expected_count_per_day * 30
                assert round(expected_count_per_day, 2) == round(actual_count_per_day, 2), \
                    f"Deployment frequency rate assertion failed. expected_count_per_day:" \
                    f" {expected_count_per_day} , actual_count_per_day: " \
                    f"{actual_count_per_day}"
                assert round(expected_count_per_week, 2) == round(actual_count_per_week, 2), \
                    f"Deployment frequency rate assertion failed. expected_count_per_week:" \
                    f" {expected_count_per_week} , actual_count_per_week: " \
                    f"{actual_count_per_week}"
                assert round(expected_count_per_month, 2) == round(actual_count_per_month, 2), \
                    f"Deployment frequency rate assertion failed. expected_count_per_month:" \
                    f" {expected_count_per_month} , actual_count_per_month: " \
                    f"{actual_count_per_month}"
            LOG.info("[Data Accuracy Check] CICD DF Grading calculation logic verification complete.")

            if cicd_env.disable_datastore_check.get(product_application_name) != True and not skip_datastore_check:
                LOG.info("DF widget -> data store verification start...")
                LOG.info(f"df_widget_total_deployment: {total_deployment}, filtered_job_cnt_from_datastore" \
                        f": {filtered_job_cnt_from_datastore}")
                assert total_deployment == filtered_job_cnt_from_datastore, "CICD DF widget record count did " \
                    "not match the data store count."
                LOG.info("DF widget -> data store verification complete...")

            LOG.info("[Data Accuracy Check] Verifying CICD DF Band allocation logic.")
            LOG.info(f"Total deployment record count: {actual_count_per_day}")
            # Asserting Deployment frequency band value.
            if actual_count_per_day > 1:
                assert band.lower() == "ELITE".lower(), \
                    f"Deployment frequency band should be 'Elite' but found as {band}"
            elif actual_count_per_day <= 1 and (actual_count_per_day * 7) >= 1:
                assert band.lower() == "HIGH".lower(), \
                    f"Deployment frequency band should be 'High' but found as {band}"
            elif (actual_count_per_day * 7) < 1 and (actual_count_per_day * 30) >= 1:
                assert band.lower() == "MEDIUM".lower(), \
                    f"Deployment frequency band should be 'Medium' but found as {band}"
            else:
                assert band.lower() == "LOW".lower(), \
                    f"Deployment frequency band should be 'Low' but found as {band}"
            LOG.info("[Data Accuracy Check] CICD DF Band allocation logic verification complete.")

            LOG.info("[Data Consistency Check - Week wise] DF Widget -> DF Drilldown start...")
            assert len(df_widget_drilldown_week_wise_discrepancy) == 0, "Records mis-match between CICD Deployment" \
                "  Frequency widget and CICD Deployment Frequency Drilldown.\n Unmatched" \
                f" records: {df_widget_drilldown_week_wise_discrepancy}"
            LOG.info("[Data Consistency Check - Week wise] DF Widget -> DF Drilldown complete...")

            LOG.info("[Data Consistency Check - Month wise] DF Widget -> DF Drilldown start...")
            assert len(df_widget_drilldown_month_wise_discrepancy) == 0, "Records mis-match between CICD Deployment" \
                "  Frequency widget and CICD Deployment Frequency Drilldown.\n Unmatched" \
                f" records: {df_widget_drilldown_month_wise_discrepancy}"
            LOG.info("[Data Consistency Check - Month wise] DF Widget -> DF Drilldown complete...")

        if not tests_run:
            LOG.info(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")
            pytest.skip(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")

    @pytest.mark.cicdregression
    @pytest.mark.cicdsanity
    @pytest.mark.parametrize(
        'product_application_name', cicd_env.product_application_names
    )
    def test_cicd_df_verify_basic_functionality_check(self,
                                                      product_application_name,
                                                      widget_schema_validation,
                                                      create_widget_helper_object,
                                                      datastore_prep):
        """
        Test case to verify the working of CICD DF widget and drilldown with 'end_time' and 'project' pipeline filters.
        It also verifies the pagination logic of DF drilldown widget.

        Assertions:
        1. CICD DF widget schema verification.
        2. CICD DF grading value and band allocation logic check.
        3. CICD DF drilldown widget schema verification.
        4. Month-wise drilldown and verifying the record count between DF widget and DF drilldown.
        5. CICD DF drilldown pagination logic check.
        """
        LOG.info(f"\n\n====== Executing test_cicd_df_verify_basic_functionality_check for integration: {product_application_name} ======")

        org_ids = cicd_env.org_ids
        if not org_ids:
            LOG.info("Skipping this test case as CICD integration configuration is not found for this tenant.")
            pytest.skip("Skipping this test case as CICD integration configuration is not found for this tenant.")

        # Getting time_range for executing widget API.
        try:
            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                LOG.info("===== Reading time range from the Harness File store =====")
                # Loading data from datastore.
                with open(datastore_prep[product_application_name]["checkpoint"], "r") as json_file:
                    time_range = json.load(json_file)

            # End date is considered as now() - 2d.
            else:
                LOG.info("===== Datastore disabled. Calculating time range manually =====")
                time_range = cicd_env.api_helper_obj.cicd_fetch_time_range()
        except Exception as e:
            LOG.error(f" ===== Error occurred while calculating time range. Error: {e}")
            pytest.skip(f"Error occurred while calculating time range. Error: {e}")

        total_days_in_time_range = round(
            (int(time_range["$lt"]) - int(time_range["$gt"])) / 86400
        )
        LOG.info(f"Time_range for DF widget: {time_range}, total_days: {total_days_in_time_range}")

        # Basic Pipeline filter combinations.
        calculation_field = "end_time"
        pipeline_filter_eq = [{
            "field": "cicd_user_ids"
        }]

        tests_run = False
        for org_id in org_ids:
            # Fetching integration IDs.
            product_integration_id = cicd_env.integration_details[org_id].get(product_application_name)
            if not product_integration_id:
                continue

            tests_run = True

            df_widget_data_present = False
            df_widget_data_collection_trial = 1

             # Attempting 5 retries to select filter combinations with non-null values in the widget.
            while not df_widget_data_present and df_widget_data_collection_trial < 6:
                LOG.info(f"Trial {df_widget_data_collection_trial} for selecting filter values "
                         "for workflow profile.")

                # Getting CICD DF pipeline filters payload.
                df_pipeline_filters = cicd_env.cicd_job_run_helper.generate_cicd_dora_filters_payload(
                    integration_ids=product_integration_id,
                    pipeline_filter_eq = pipeline_filter_eq
                )
                if not df_pipeline_filters:
                    df_widget_data_collection_trial = df_widget_data_collection_trial + 1
                    continue

                #### Workflow profile creation ####
                LOG.info(f"Creating workflow profile with DF filters: {df_pipeline_filters}")

                # Creating the workflow profile.
                workflow_profile = cicd_env.mapper_lib.create_cicd_workflow_profile(
                    profile_filters={
                        "integration_ids": product_integration_id,
                        "ou_id": org_id,
                        "application": product_application_name
                    },
                    df_filters={
                        "calculation_field": calculation_field,
                        "filters": df_pipeline_filters
                    }
                )
                LOG.info(f"Workflow profile created: {workflow_profile}")
                #### Workflow profile created ####

                #### CICD Deployment frequency widget Start ####
                LOG.info("Executing the CICD deployment frequency widget API call")

                arg_ou_ids = [org_id]

                # DF Post API call payload.
                payload = cicd_env.mapper_lib.dora_deployment_freq_filter(
                    arg_ou_ids=arg_ou_ids,
                    time_range=time_range
                )
                LOG.info("CICD DF widget payload----{}".format(json.dumps(payload)))

                try:
                    resp = cicd_env.generic_obj.execute_api_call(
                        cicd_env.dora_df_widget_url + "?there_is_no_cache=true", "post", data=payload
                    )
                except Exception as ex:
                    LOG.warning("Error occurred while executing Dora DF API."
                                f"exception---{ex}")
                    raise Exception(ex)

                month = resp['time_series']['month']
                total_deployment = resp['stats']["total_deployment"]
                actual_count_per_day = resp['stats']['count_per_day']
                band = resp["stats"]["band"]

                # Widget does not have data with the select filter values.
                if total_deployment == 0 or actual_count_per_day == 0.0:
                    df_widget_data_collection_trial = df_widget_data_collection_trial + 1
                    LOG.info("Filter values selected does not have enough data to validate. Re-trying "
                                "another filter value combination to resume the test case execution.")
                    # Delete Workflow profile
                    cicd_env.mapper_lib.delete_workflow_profile(workflow_id=workflow_profile["id"])
                else:
                    df_widget_data_present = True

            if not df_widget_data_present or df_widget_data_collection_trial == 6:
                LOG.info("Skipping this test case as the filters does not have data to validate.")
                pytest.skip("Skipping this test case as the filters does not have data to validate.")

            #### CICD DF Drill-down verification Start ####
            stack_to_drilldown, record_index = cicd_env.mapper_lib.select_key_for_drilldown(
                application = product_application_name,
                records = month,
                already_parsed_records = []
            )

            if not stack_to_drilldown.get("key"):
                LOG.info("Skipping this test case as no stack found to check the drilldown.")
                pytest.skip("Skipping this test case as no stack found to check the drilldown.")

            time_range_for_drilldown = cicd_env.mapper_lib.calculate_time_range_for_drilldown_widget(
                record = stack_to_drilldown,
                record_index = record_index,
                group_type = "month",
                widget_time_range = time_range
            )

            # If datastore check is not disabled, verify the widget records with the datastore.
            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                skip_fields_from_datastore_check = cicd_env.integration_details[org_id].get(
                        "skip_fields_from_datastore_check", []
                    )
                with open(datastore_prep[product_application_name]["datastore"], "r") as json_file:
                    data = json.load(json_file)

                filtered_job_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                        datastore = data,
                        filters = df_pipeline_filters, time_range = time_range_for_drilldown,
                        skip_fields=skip_fields_from_datastore_check
                    )

            # Calculating count of records in the stack selected from CICD DF widget.
            df_widget_record_count = 0
            for stack in stack_to_drilldown.get("stacks", []):
                df_widget_record_count = df_widget_record_count + stack["count"]

            drilldown_time_range = cicd_env.mapper_lib.calculate_time_range_for_drilldown_widget(
                record = stack_to_drilldown, record_index = record_index,
                 group_type = "month", widget_time_range = time_range_for_drilldown
            )
            LOG.info("Executing DF drilldown widget..")
            has_next = True
            drilldown_record_count = 0
            if df_widget_record_count > 2:
                page_size = int(df_widget_record_count / 2)
            else:
                page_size = df_widget_record_count
            record_count_per_api_call = []
            page = 0
            while has_next:
                drilldown_resp = cicd_env.mapper_lib.cicd_drilldown_widget(
                    org_ids = [org_id],
                    time_range = drilldown_time_range,
                    page = page,
                    page_size = page_size
                )

                if drilldown_resp:
                    record_count = len(drilldown_resp["records"])
                    has_next = drilldown_resp.get("_metadata", {}).get("has_next", False)
                    drilldown_record_count = drilldown_record_count + record_count
                    if (record_count < page_size or record_count > page_size) and has_next:
                        record_count_per_api_call.append({
                            "page": page,
                            "page_size": page_size,
                            "records_count": record_count,
                            "has_next": has_next
                        })
                else:
                    has_next = False
                LOG.info(f"Records received in the API call: {record_count}, has_next: {has_next}")

                page = page + 1

            #### Delete Workflow profile Start####
            LOG.info("Deleting Workflow Profile.")
            cicd_env.mapper_lib.delete_workflow_profile(workflow_id=workflow_profile["id"])
            LOG.info(f"Workflow Profile '{workflow_profile['id']}' deleted.")
            #### Delete Workflow profile End ####

            #### Assertions ####
            LOG.info("DF Widget Schema Verification start...")
            create_widget_helper_object.schema_validations(
                widget_schema_validation.create_deployment_frequency(application = product_application_name),
                resp,
                "Schema validation failed for CICD Deployment Frequency widget ",
            )
            LOG.info("DF Widget Schema Verification completed...")

            LOG.info("DF Drilldown Widget Schema Verification start...")
            create_widget_helper_object.schema_validations(
                widget_schema_validation.cicd_deployment_frequency_drilldown(),
                drilldown_resp,
                "Schema validation failed for CICD Deployment Frequenct widget ",
            )
            LOG.info("DF Drilldown Widget Schema Verification completed...")

            LOG.info(f"df_widget_record_count: {df_widget_record_count}, drilldown_record_count: {drilldown_record_count}")
            assert df_widget_record_count == drilldown_record_count, "Record count mismatch between DF widget and Drilldown."

            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                LOG.info("DF widget drilldown -> data store verification start...")
                LOG.info(f"drilldown_record_count: {drilldown_record_count}, filtered_job_cnt_from_datastore" \
                        f": {filtered_job_cnt_from_datastore}")
                assert drilldown_record_count == filtered_job_cnt_from_datastore, "Drilldown record count did " \
                    "not match the data store count."
                LOG.info("DF widget drilldown -> data store verification complete...")
            else:
                LOG.info(f"Data store check disabled. disable_datastore_check: {cicd_env.disable_datastore_check}")

            LOG.info(f"Mis-matched records count with the page size: {record_count_per_api_call}")
            assert len(record_count_per_api_call) == 0, "Records received in drilldown widget did not align with the pagination logic."

            LOG.info("[Data Accuracy Check] Verifying CICD DF band calculation logic.")
            # Asserting deployment frequency rate.
            if total_deployment / total_days_in_time_range == 0:
                assert total_deployment == actual_count_per_day, \
                        f"Deployment frequency rate assertion failed. Total " \
                        f"deployment: {total_deployment} , actual_count_per_day: " \
                        f"{actual_count_per_day}"
            else:
                expected_count_per_day = round((total_deployment / total_days_in_time_range), 2)
                assert expected_count_per_day == round(actual_count_per_day, 2), \
                    f"Deployment frequency rate assertion failed. Total " \
                    f"deployment: {expected_count_per_day} , actual_count_per_day: " \
                    f"{actual_count_per_day}"
            LOG.info("[Data Accuracy Check] CICD DF Grading calculation logic verification complete.")

            LOG.info("[Data Accuracy Check] Verifying CICD DF Band allocation logic.")
            LOG.info(f"Total deployment record count: {actual_count_per_day}")
            # Asserting Deployment frequency band value.
            if actual_count_per_day > 1:
                assert band.lower() == "ELITE".lower(), \
                    f"Deployment frequency band should be 'Elite' but found as {band}"
            elif actual_count_per_day <= 1 and (actual_count_per_day * 7) >= 1:
                assert band.lower() == "HIGH".lower(), \
                    f"Deployment frequency band should be 'High' but found as {band}"
            elif (actual_count_per_day * 7) < 1 and (actual_count_per_day * 30) >= 1:
                assert band.lower() == "MEDIUM".lower(), \
                    f"Deployment frequency band should be 'Medium' but found as {band}"
            else:
                assert band.lower() == "LOW".lower(), \
                    f"Deployment frequency band should be 'Low' but found as {band}"
            LOG.info("[Data Accuracy Check] CICD DF Band allocation logic verification complete.")

        if not tests_run:
            LOG.info(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")
            pytest.skip(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")