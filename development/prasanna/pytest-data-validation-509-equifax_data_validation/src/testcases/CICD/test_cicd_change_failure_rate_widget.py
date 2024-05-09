import json
import logging
import pytest
import copy

from src.testcases.CICD.conftest import CICDDoraCFR

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

cicd_env = CICDDoraCFR()

class TestCICDChangeFailureRate():

    @pytest.mark.cicdregression
    @pytest.mark.parametrize(
        'cicd_cfr_pipeline_filters', cicd_env.cicd_filters.get("dora_cfr", {}).get("filter_combinations", [])
    )
    def test_cicd_cfr_with_filter_combinations(self,
                                               cicd_cfr_pipeline_filters,
                                               widget_schema_validation,
                                               create_widget_helper_object,
                                               datastore_prep):
        """
        This test cases tests the CFR widget with multiple filter combinations.
        It verifies the behavior of the test case under various scenarios, asserts the
        drilldown widget and also verifies the count with the datastore.

        Assertions:
        1. CICD CFR widget schema verification.
        2. CICD CFR band value and failure rate calculation logic check.
        3. Day-wise, week-wise and month-wise record count and drilldown verification.
        4. CICD CFR drilldown widget record count with the datastore.
        """
        LOG.info(f"\n\n====== Executing test_cicd_cfr_with_filter_combinations - {cicd_cfr_pipeline_filters} ======")
        if not cicd_env.org_ids:
            LOG.info("Skipping this test case as CICD integration configuration is not found for this tenant.")
            pytest.skip("Skipping this test case as CICD integration configuration is not found for this tenant.")

        product_application_name = cicd_cfr_pipeline_filters[0]

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
        LOG.info(f"Time_range for CFR widget: {time_range}, total_days: {total_days_in_time_range}")
        filter_list = []

        calculation_field = cicd_cfr_pipeline_filters[1]
        deployment_eq = cicd_cfr_pipeline_filters[2]
        deployment_not_eq = cicd_cfr_pipeline_filters[3]
        pipeline_selection = cicd_cfr_pipeline_filters[4]

        filter_list, deployment_eq = cicd_env.cicd_job_run_helper.align_params_in_expected_format(
            params = deployment_eq, filter_list=filter_list
        )

        filter_list, deployment_not_eq = cicd_env.cicd_job_run_helper.align_params_in_expected_format(
            params = deployment_not_eq, filter_list=filter_list
        )

        pipeline_selection = cicd_cfr_pipeline_filters[4]

        # Checking if record match check and datastore check needs to be skipped.
        # These checks will be skipped for filters whose data is available through Pipeline
        # detail API call.
        skip_datastore_check = any(skip_filter for skip_filter in \
                                   cicd_env.filters_to_skip_for_datastore_check \
                                    if skip_filter in filter_list)
        LOG.info(f"===== filters_supported: {skip_datastore_check}")

        tests_run = False
        for org_id in cicd_env.org_ids:
            # Fetching integration IDs.
            product_integration_id = cicd_env.integration_details[org_id].get(product_application_name)
            failure_statues = cicd_env.integration_details[org_id].get(
                    "failed_status_values"
                )
            if not product_integration_id:
                LOG.info(f"Integration '{product_application_name} not found with Org ID: {org_id}")
                continue

            tests_run = True

            cfr_widget_data_present = False
            cfr_widget_data_collection_trial = 1
            selected_pipelines = {}

            # Attempting 5 retries to select filter combinations with non-null values in the widget.
            while not cfr_widget_data_present and cfr_widget_data_collection_trial < 6:
                LOG.info(f"Trial {cfr_widget_data_collection_trial} for selecting filter values "
                         "for workflow profile.")

                # Fetching filter values.
                total_deployment_pipeline_filters = cicd_env.cicd_job_run_helper.generate_cicd_dora_filters_payload(
                    integration_ids=product_integration_id,
                    pipeline_filter_eq = deployment_eq,
                    pipeline_filter_not_eq = deployment_not_eq
                )
                if not total_deployment_pipeline_filters:
                    cfr_widget_data_collection_trial = cfr_widget_data_collection_trial + 1
                    continue

                failed_deployment_pipeline_filters = copy.deepcopy(total_deployment_pipeline_filters)
                failed_deployment_pipeline_filters["job_statuses"] = failure_statues

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
                LOG.info(f"workflow profile filters ------- Failed Deployment: {failed_deployment_pipeline_filters}"
                        f" ------- Total Deployment: {total_deployment_pipeline_filters}")

                # Creating the workflow profile.
                workflow_profile = cicd_env.mapper_lib.create_cicd_workflow_profile(
                    profile_filters={
                        "integration_ids": product_integration_id,
                        "ou_id": org_id,
                        "application": product_application_name
                    },
                    cfr_filters={
                        "calculation_field": calculation_field,
                        "failed_deployment_filters": failed_deployment_pipeline_filters,
                        "total_deployment_filters": total_deployment_pipeline_filters,
                        "values_list": selected_pipelines_id
                    }
                )
                LOG.info(f"Workflow profile created: {workflow_profile}")
                #### Workflow profile created ####

                arg_ou_ids = [org_id]

                # Executing CFR widget API call.
                resp = cicd_env.mapper_lib.execute_dora_profile_widget_api(
                    widget_url = cicd_env.dora_cfr_widget_url,
                    org_id = arg_ou_ids, time_range = time_range
                )

                day = resp['time_series']['day']
                week = resp['time_series']['week']
                month = resp['time_series']['month']
                total_deployment = resp['stats']["total_deployment"]
                failure_rate = resp['stats']['failure_rate']
                band = resp["stats"]["band"]

                # Widget does not have data with the select filter values.
                if total_deployment == 0 or failure_rate == 0.0:
                    cfr_widget_data_collection_trial = cfr_widget_data_collection_trial + 1
                    LOG.info("Filter values selected does not have enough data to validate. Re-trying "
                             "another filter value combination to resume the test case execution.")
                    # Delete Workflow profile
                    cicd_env.mapper_lib.delete_workflow_profile(workflow_id=workflow_profile["id"])
                else:
                    cfr_widget_data_present = True

            if not cfr_widget_data_present or cfr_widget_data_collection_trial == 6:
                LOG.info("Skipping this test case as the filters does not have data to validate.")
                pytest.skip("Skipping this test case as the filters does not have data to validate.")

            # Calculating job execution count from day list, week list and month list.
            # CFR API call returns job execution count by day, week and month.
            day_job_exec_count, week_job_exec_count, month_job_exec_count = \
                cicd_env.mapper_lib.calculate_record_cnt_by_group(
                    application=product_application_name,
                    day_wise_records=day,
                    week_wise_records=week,
                    month_wise_records=month,
                )

            record_count_consistency_across_groups = (day_job_exec_count == week_job_exec_count and \
                    week_job_exec_count == month_job_exec_count)

            #### CICD CFR Drill-down verification Start ####
            # Day wise verification.
            LOG.info("Verifying day-wise records consistency with drilldown widget.")
            cfr_widget_drilldown_day_wise_discrepancy, day_random_record = cicd_env.mapper_lib.dora_widget_drill_down_verification(
                records = day,
                org_ids = [org_id],
                application = product_application_name,
                group_type = "day",
                filters = failed_deployment_pipeline_filters,
                values_list = selected_pipelines_id,
                widget = "change_failure_rate",
                skip_record_match_check=skip_datastore_check
            )

            # Week wise verification.
            LOG.info("Verifying week-wise records consistency with drilldown widget.")
            cfr_widget_drilldown_week_wise_discrepancy, week_random_record = cicd_env.mapper_lib.dora_widget_drill_down_verification(
                records = week,
                org_ids = [org_id],
                application = product_application_name,
                widget_time_range = time_range,
                group_type = "week",
                filters = failed_deployment_pipeline_filters,
                values_list = selected_pipelines_id,
                widget = "change_failure_rate",
                skip_record_match_check=skip_datastore_check
            )

            # Month wise verification.
            LOG.info("Verifying month-wise records consistency with drilldown widget.")
            cfr_widget_drilldown_month_wise_discrepancy, month_random_record = cicd_env.mapper_lib.dora_widget_drill_down_verification(
                records = month,
                org_ids = [org_id],
                application = product_application_name,
                widget_time_range = time_range,
                group_type = "month",
                filters = failed_deployment_pipeline_filters,
                values_list = selected_pipelines_id,
                widget = "change_failure_rate",
                skip_record_match_check=skip_datastore_check
            )

            #### Delete Workflow profile Start####
            cicd_env.mapper_lib.delete_workflow_profile(workflow_id=workflow_profile["id"])
            LOG.info(f"Workflow Profile '{workflow_profile['id']}' deleted.")
            #### Delete Workflow profile End ####

            # If datastore check is not disabled, verify the widget records with the datastore.
            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                # Checking if datastore check needs to be skipped due to unsupported filter.
                if skip_datastore_check:
                    LOG.info(f"Skipped datastore check for filters: '{failed_deployment_pipeline_filters}' "
                             "as it contains filter not yet supported for datastore check.")
                else:
                    skip_fields_from_datastore_check = cicd_env.integration_details[org_id].get(
                        "skip_fields_from_datastore_check", []
                    )
                    # Fetching records from the datastore.
                    with open(datastore_prep[product_application_name]["datastore"], "r") as json_file:
                        data = json.load(json_file)

                    failed_deployment_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                            datastore=data, filters=failed_deployment_pipeline_filters,
                            time_range=time_range, calculation_field=calculation_field,
                            values_list=selected_pipelines.get("job_names", []),
                            skip_fields=skip_fields_from_datastore_check
                    )
                    total_deployment_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                            datastore=data, filters=total_deployment_pipeline_filters,
                            time_range=time_range, calculation_field=calculation_field,
                            values_list=selected_pipelines.get("job_names", []),
                            skip_fields=skip_fields_from_datastore_check
                    )

            #### Assertions ####
            create_widget_helper_object.schema_validations(
                widget_schema_validation.create_failure_rate_frequency(application = product_application_name),
                resp,
                "Schema validation failed for CICD CFR widget ",
            )
            LOG.info("[SCHEMA VERIFICATION] CFR Widget schema verified.")

            LOG.info(f"day_count -- {day_job_exec_count}, week_count -- {week_job_exec_count}"
                     f", month_count -- {month_job_exec_count}")
            assert record_count_consistency_across_groups, "day_count / week_count / month_count not matching."
            LOG.info("[DATA CONSISTENCY] Job count check across day, week and month wise group complete.")

            assert len(cfr_widget_drilldown_day_wise_discrepancy) == 0, "Records mis-match between CICD Deployment" \
                "  Frequency widget and CICD Deployment Frequency Drilldown.\n Unmatched" \
                f" records: {cfr_widget_drilldown_day_wise_discrepancy}"
            LOG.info("[DATA CONSISTENCY - Day wise] cfr Widget -> CFR Drilldown verification complete.")

            assert len(cfr_widget_drilldown_week_wise_discrepancy) == 0, "Records mis-match between CICD Deployment" \
                "  Frequency widget and CICD Deployment Frequency Drilldown.\n Unmatched" \
                f" records: {cfr_widget_drilldown_week_wise_discrepancy}"
            LOG.info("[DATA CONSISTENCY - Week wise] CFR Widget -> CFR Drilldown complete.")

            assert len(cfr_widget_drilldown_month_wise_discrepancy) == 0, "Records mis-match between CICD Deployment" \
                "  Frequency widget and CICD Deployment Frequency Drilldown.\n Unmatched" \
                f" records: {cfr_widget_drilldown_month_wise_discrepancy}"
            LOG.info("[DATA CONSISTENCY - Month wise] CFR Widget -> CFR Drilldown complete.")

            if cicd_env.disable_datastore_check.get(product_application_name) != True and not skip_datastore_check:
                LOG.info(f"widget_total_deployment: {total_deployment}, "
                        f"datastore_total_count: {total_deployment_cnt_from_datastore}")
                assert total_deployment == total_deployment_cnt_from_datastore, "Total deployment count " \
                    "did not match with the datastore."
                LOG.info("[DATA ACCURACY] Total deployment count check with datastore complete.")

                expected_failure_rate = 0
                if total_deployment_cnt_from_datastore > 0:
                    expected_failure_rate = (
                        failed_deployment_cnt_from_datastore * 100) / total_deployment_cnt_from_datastore
                LOG.info(f"widget_failure_rate: {failure_rate}, datastore_failure_Rate: {expected_failure_rate}")
                assert failure_rate == expected_failure_rate
                LOG.info("[DATA ACCURACY] Change Failure Rate value validated with the datastore records.")

                expected_band = cicd_env.mapper_lib.cfr_band_calculation(expected_failure_rate)
                LOG.info(f"widget_band: {band}, datastore_band: {expected_band}")
                assert band == expected_band
                LOG.info("[DATA ACCURACY] CFR Band Allocation validated with the datastore records.")

        if not tests_run:
            LOG.info(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")
            pytest.skip(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")

    @pytest.mark.cicdregression
    @pytest.mark.cicdsanity
    @pytest.mark.parametrize(
        'product_application_name', cicd_env.product_application_names
    )
    def test_cicd_cfr_verify_basic_functionality_check(self,
                                                       product_application_name,
                                                       widget_schema_validation,
                                                       create_widget_helper_object,
                                                       datastore_prep):
        """
        This is a sanity test case which verifies the basic functionalities of the CFR widget.
        It verifies the widget data with default filters, asserts the pagination check and
        validates the count with the datastore.

        Assertions:
        1. CICD CFR widget schema verification.
        2. CICD CFR Drilldown schema verification
        3. CICD CFR band value and failure rate calculation logic check.
        4. Month-wise drilldown and verifying the record count between CFR widget and CFR drilldown.
        5. CICD CFR drilldown pagination logic check.
        6. CICD CFR widget records with the datastore check.
        """
        LOG.info(f"\n\n====== Executing test_cicd_cfr_verify_basic_functionality_check ======")
        if not cicd_env.org_ids:
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
        LOG.info(f"Time_range for CFR widget: {time_range}, total_days: {total_days_in_time_range}")

        calculation_field = "end_time"

        # Total Deployment Pipeline filter combinations.
        total_delployment_filters_eq = [{
            "field": "cicd_user_ids"
        }]

        tests_run = False
        for org_id in cicd_env.org_ids:
            # Fetching integration IDs.
            product_integration_id = cicd_env.integration_details[org_id].get(product_application_name)
            failure_statues = cicd_env.integration_details[org_id].get(
                    "failed_status_values"
                )
            if not product_integration_id:
                LOG.info(f"Integration '{product_application_name} not found with Org ID: {org_id}")
                continue

            tests_run = True

            cfr_widget_data_present = False
            cfr_widget_data_collection_trial = 1

            # Attempting 5 retries to select filter combinations with non-null values in the widget.
            while not cfr_widget_data_present and cfr_widget_data_collection_trial < 6:
                LOG.info(f"Trial {cfr_widget_data_collection_trial} for selecting filter values "
                         "for workflow profile.")
                # Getting CICD CFR pipeline filters payload.
                total_deployment_filters = cicd_env.cicd_job_run_helper.generate_cicd_dora_filters_payload(
                    integration_ids=product_integration_id,
                    pipeline_filter_eq = total_delployment_filters_eq
                )
                if not total_deployment_filters:
                    cfr_widget_data_collection_trial = cfr_widget_data_collection_trial + 1
                    continue

                failed_deployment_filters = copy.deepcopy(total_deployment_filters)
                failed_deployment_filters["job_statuses"] = failure_statues

                #### Workflow profile creation ####
                LOG.info(f"Creating Workflow profile with filters ------ Failed Deployment: {failed_deployment_filters}"
                        f"------- Total Deployment: {total_deployment_filters}")

                # Creating the workflow profile.
                workflow_profile = cicd_env.mapper_lib.create_cicd_workflow_profile(
                    profile_filters={
                        "integration_ids": product_integration_id,
                        "ou_id": org_id,
                        "application": product_application_name
                    },
                    cfr_filters = {
                        "calculation_field": calculation_field,
                        "failed_deployment_filters": failed_deployment_filters,
                        "total_deployment_filters": total_deployment_filters
                    }
                )
                LOG.info(f"Workflow profile created ----: {workflow_profile}")

                arg_ou_ids = [org_id]

                # Executing CFR widget API call.
                resp = cicd_env.mapper_lib.execute_dora_profile_widget_api(
                    widget_url = cicd_env.dora_cfr_widget_url,
                    org_id = arg_ou_ids, time_range = time_range
                )

                month = resp['time_series']['month']
                total_deployment = resp['stats']["total_deployment"]
                failure_rate = resp['stats']['failure_rate']
                band = resp["stats"]["band"]

                # Widget does not have data with the select filter combination.
                if total_deployment == 0 or failure_rate == 0.0:
                    cfr_widget_data_collection_trial = cfr_widget_data_collection_trial + 1
                    LOG.info("Filter values selected does not have enough data to validate. Re-trying "
                             "another filter value combination to resume the test case execution.")
                    # Delete Workflow profile
                    cicd_env.mapper_lib.delete_workflow_profile(workflow_id=workflow_profile["id"])
                else:
                    cfr_widget_data_present = True

            if not cfr_widget_data_present or cfr_widget_data_collection_trial == 6:
                LOG.info("Skipping this test case as the filters does not have data to validate.")
                pytest.skip("Skipping this test case as the filters does not have data to validate.")

            #### CICD CFR Drill-down verification Start ####
            stack_to_drilldown, record_index = cicd_env.mapper_lib.select_key_for_drilldown(
                application = product_application_name,
                records = month,
                already_parsed_records = []
            )

            if not stack_to_drilldown.get("key"):
                LOG.info("Skipping this test case as no stack found to check the drilldown.")
                pytest.skip("Skipping this test case as no stack found to check the drilldown.")

            # Calculating count of records in the stack selected from CICD CFR widget.
            cfr_widget_record_count = 0
            for stack in stack_to_drilldown.get("stacks", []):
                cfr_widget_record_count = cfr_widget_record_count + stack["count"]

            drilldown_time_range = cicd_env.mapper_lib.calculate_time_range_for_drilldown_widget(
                record = stack_to_drilldown, record_index = record_index,
                 group_type = "month", widget_time_range = time_range
            )

            # If datastore check is not disabled, verify the widget records with the datastore.
            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                skip_fields_from_datastore_check = cicd_env.integration_details[org_id].get(
                        "skip_fields_from_datastore_check", []
                    )
                with open(datastore_prep[product_application_name]["datastore"], "r") as json_file:
                    data = json.load(json_file)

                failed_deployment_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                    datastore = data,
                    filters = failed_deployment_filters, time_range = time_range,
                    skip_fields=skip_fields_from_datastore_check
                )
                total_deployment_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                    datastore = data,
                    filters = total_deployment_filters, time_range = time_range,
                    skip_fields=skip_fields_from_datastore_check
                )
                drilldown_job_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                    datastore = data,
                    filters = failed_deployment_filters, time_range = drilldown_time_range,
                    skip_fields=skip_fields_from_datastore_check
                )

            LOG.info("Executing CFR drilldown widget..")
            has_next = True
            drilldown_record_count = 0
            if cfr_widget_record_count > 2:
                page_size = int(cfr_widget_record_count / 2)
            else:
                page_size = cfr_widget_record_count
            record_count_per_api_call = []
            page = 0
            data_consistency_failure_records = []

            while has_next:
                drilldown_resp = cicd_env.mapper_lib.cicd_drilldown_widget(
                    org_ids = [org_id],
                    time_range = drilldown_time_range,
                    page = page,
                    page_size = page_size,
                    widget = "change_failure_rate"
                )

                if drilldown_resp:
                    record_count = len(drilldown_resp["records"])
                    for data in drilldown_resp["records"]:
                        if data["status"] not in failure_statues:
                            data_consistency_failure_records.append(data)
                            has_next = False
                            break

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
            create_widget_helper_object.schema_validations(
                widget_schema_validation.create_failure_rate_frequency(application = product_application_name),
                resp,
                "Schema validation failed for CICD CFR widget ",
            )
            LOG.info("[SCHEMA VERIFICATION] CFR Widget schema verified.")

            create_widget_helper_object.schema_validations(
                widget_schema_validation.cicd_deployment_frequency_drilldown(),
                drilldown_resp,
                "Schema validation failed for CICD Deployment Frequenct widget ",
            )
            LOG.info("[SCHEMA VERIFICATION] Drilldown Widget schema verified.")

            assert len(data_consistency_failure_records) == 0, "Records not matching the failed deployment pipeline filters" \
                f"----- data_consistency_failure_records: {data_consistency_failure_records}"
            LOG.info("[DATA ACCURACY] - Pipeline filters drilldown records verified.")

            assert cfr_widget_record_count == drilldown_record_count, f"CFR widget count {cfr_widget_record_count} " \
                f"--------- drilldown_record_count: {drilldown_record_count} ---- Records not matched"
            LOG.info("[DATA CONSISTENCY] Widget record count -> Drilldown record count verification done.")

            assert len(record_count_per_api_call) == 0, "Records received in drilldown widget did not align with the pagination logic."
            LOG.info(f"[DATA CONSISTENCY] Pagination logic verified.")

            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                LOG.info(f"drilldown_record_count: {drilldown_record_count}, datastore_record_count: {drilldown_job_cnt_from_datastore}")
                assert drilldown_record_count == drilldown_job_cnt_from_datastore, "Drilldown record count did not " \
                    "match with the datastore records count."
                LOG.info("[DATA CONSISTENCY] Drilldown records count validation with the datastore records complete.")

                expected_failure_rate = (failed_deployment_cnt_from_datastore * 100) / total_deployment_cnt_from_datastore
                LOG.info(f"actual_failure_rate: {failure_rate} ------- expected_failure_rate: {expected_failure_rate}")
                assert failure_rate == expected_failure_rate, "Widget Failure rate calculation did not match " \
                    "the expected failure rate."
                LOG.info("[DATA ACCURACY] Change Failure rate value calculation verified.")

                LOG.info(f"total_deployment_count_from_widget: {total_deployment} ------- "
                        f"total_deployment_count_from_datastore: {total_deployment_cnt_from_datastore}")
                assert total_deployment == total_deployment_cnt_from_datastore, "Total deployment count did not match " \
                    "the expected count from the datastore."
                LOG.info("[DATA ACCURACY] Total deployments count in datastore and CFR widget verified.")

                expected_band = cicd_env.mapper_lib.cfr_band_calculation(expected_failure_rate)
                LOG.info(f"actual_band: {band} ------- expected_band: {expected_band}")
                assert band == expected_band, "CFR band did not match with the expected band value."
                LOG.info("[DATA ACCURACY] CFR band calculation verified.")

        if not tests_run:
            LOG.info(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")
            pytest.skip(f"Skipping this test case as '{product_application_name}' is not configured in any CICD OUs of this tenant.")
