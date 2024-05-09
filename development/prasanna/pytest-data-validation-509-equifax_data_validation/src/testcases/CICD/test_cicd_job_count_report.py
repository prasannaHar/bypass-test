import json
import logging
import pytest

from src.utils.cicd_payload_generator import (
    generate_cicd_job_count_payload
)
from src.testcases.CICD.conftest import CICDJobCount

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

cicd_env = CICDJobCount()

class TestCICDJobCount():

    @pytest.mark.cicdregression
    @pytest.mark.parametrize(
        "aggr_details", cicd_env.cicd_filters.get("job_count", {}).get("x_axis_field_names", [])
    )
    def test_job_count_widget_with_only_x_axis_filters(
        self,
        widget_schema_validation,
        create_widget_helper_object,
        aggr_details,
        datastore_prep,
    ):
        """
        Test case to assert the Job count widget with the Drill-down feature.
        This test case verifies the count based on Project field on X-Axis.

        Assertions:
        1. Assert response received is in the expected format.
        2. Assert Job count received in CICD Job count widget is identical to count received in Drill-down widget.
        3. Assert rows received in Drill-down widget is accurate and matching the filters.
        """

        LOG.info("====== Test case execution - test_job_count_widget_with_only_x_axis_filters for "
                 f": '{aggr_details}' =====")

        product_application_name = aggr_details[0]
        x_axis_field = aggr_details[1]

        org_ids = cicd_env.org_ids

        if not org_ids:
            LOG.info("Skipping this test case as CICD integration configuration is not found for this tenant.")
            pytest.skip("Skipping this test case as CICD integration configuration is not found for this tenant.")

        for org_id in org_ids:
            # Fetching integration IDs.
            integration_ids = cicd_env.integration_details[org_id].get(product_application_name)
            if not integration_ids:
                continue

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

            LOG.info("===== Executing CICD job count report widget endpoint =====")
            cicd_job_count_widget_url = cicd_env.generic_obj.connection["base_url"] + \
                cicd_env.generic_obj.api_data["CICD_JOB_COUNT_WIDGET_URL"]

            payload = generate_cicd_job_count_payload(
                arg_across = x_axis_field,
                arg_end_time = time_range,
                arg_req_integration_ids = integration_ids,
                arg_ou_id = org_id,
                arg_sort = [{"id": "count", "desc": True}]
            )

            LOG.info(f"CICD Job count widget payload ---- {json.dumps(payload)}")

            # Executing CICD Job count widget API.
            widget_response = cicd_env.generic_obj.execute_api_call(
                url=cicd_job_count_widget_url,
                request_type="post",
                data=payload,
                status_code_info=True,
            )

            widget_response = widget_response.json()
            widget_record = widget_response["records"]

            widget_record_cnt = sum(record["count"] for record in widget_record)

            if widget_record_cnt == 0:
                LOG.info("Not enough data in the given time range.")
                continue

            # Fetching the value of X-Axis field to send in Drilldown widget.
            filter_name = widget_record[0]["key"]
            widget_record_drill_down_cnt = widget_record[0]["count"]

            drilldown_url = cicd_env.generic_obj.connection["base_url"] + \
                cicd_env.generic_obj.api_data["CICD_JOB_COUNT_WIDGET_LIST_URL"]

            x_axis_filters = cicd_env.generic_obj.api_data["cicd_job_count_config"][x_axis_field]["filters"]
            dynamic_filter_list = [(x_axis_filters, [filter_name])]
            LOG.info(f"Executing Drilldown API call with filter: {dynamic_filter_list}")

            payload = generate_cicd_job_count_payload(
                arg_across=x_axis_field,
                arg_end_time=time_range,
                arg_req_integration_ids=integration_ids,
                arg_req_dynamic_filters=dynamic_filter_list,
                arg_ou_id=org_id,
            )

            LOG.info(f"CICD Job count drilldown payload ---- {json.dumps(payload)}")

            LOG.info(
                "===== Executing cicd job count report widget drill down endpoint ====="
            )
            # Executing drill-down feature API.
            drilldown_response = cicd_env.generic_obj.execute_api_call(
                url=drilldown_url, request_type="post", data=payload, status_code_info=True
            )

            drilldown_response = drilldown_response.json()
            drilldown_response_record = drilldown_response["records"]

            LOG.info("===== Asserting CICD job count widget API response =====")
            create_widget_helper_object.schema_validations(
                widget_schema_validation.cicd_job_count_report_drilldown_list(),
                drilldown_response,
                "Schema validation failed for CICD job count report drilldown "
                "list endpoint",
            )

            LOG.info("===== Asserting CICD job count widget list API response =====")
            create_widget_helper_object.schema_validations(
                widget_schema_validation.cicd_job_count_report(),
                widget_response,
                "Schema validation failed for CICD job count Report widget " "endpoint",
            )

            count_verification_with_drilldown = (
                len(drilldown_response_record) == widget_record_drill_down_cnt
            )

            data_accuracy_check_with_drilldown = True
            for record in drilldown_response_record:
                if record[cicd_env.generic_obj.api_data["cicd_job_count_config"][x_axis_field]["response"]] != filter_name:
                    data_accuracy_check_with_drilldown = False
                    LOG.info(
                        f"Data accuracy check with drilldown widget failed. Drilldown widget value: "
                        f"{record[cicd_env.generic_obj.api_data['cicd_job_count_config'][x_axis_field]['response']]}, Filter Name: {filter_name}"
                    )
                    break

            LOG.info(
                f"Count Verification with Drilldown widget: {count_verification_with_drilldown}"
            )
            LOG.info(
                f"Data Accuracy check with Drilldown widget: {data_accuracy_check_with_drilldown}"
            )

            LOG.info("(CICD Job count - Total records) widget_record_cnt: "
                     f"{widget_record_cnt}")
            LOG.info(f"(CICD Job count (Drilldown field '{x_axis_field} = {filter_name}' count) "
                     f"widget_record_drill_down_cnt: {widget_record_drill_down_cnt}")
            LOG.info("(CICD Job count Drilldown count) drilldown_response_count: "
                     f"{len(drilldown_response_record)}")

            # If datastore check is not disabled, verify the widget records with the datastore.
            if cicd_env.disable_datastore_check.get(product_application_name) != True:
                LOG.info("===== Loading job execution data from datastore =====")
                # Loading data from datastore.
                with open(datastore_prep[product_application_name]["datastore"], "r") as json_file:
                    data = json.load(json_file)

                filtered_job_cnt_from_datastore = len(data)
                count_verification_with_datastore = (
                    filtered_job_cnt_from_datastore == widget_record_cnt
                )
                LOG.info(f"(CICD Job count data store count) filtered_job_cnt_from_datastore: {filtered_job_cnt_from_datastore}")
                LOG.info(
                    f"Count Verification with Data store: {count_verification_with_datastore}"
                )
                assert count_verification_with_datastore

            assert count_verification_with_drilldown
            assert data_accuracy_check_with_drilldown

    @pytest.mark.cicdregression
    @pytest.mark.parametrize(
        "cicd_job_count_filters", cicd_env.cicd_filters.get("job_count", {}).get("filter_combinations", [])
    )
    def test_job_count_widget_with_all_filter_combinations(
        self,
        cicd_job_count_filters,
        create_widget_helper_object,
        widget_schema_validation,
        datastore_prep
    ):
        """
        Test case to assert the behavior of the Job count widget with Filter, X-Axis and
        Stack combinations.
        This test case asserts the result of Job count widget with the Drill-down feature.

        Assertions:
        1. Assert response received is in the expected format.
        2. Assert Job count received in CICD Job count widget is identical to count received in Drill-down widget.
        3. Assert data received in Drill-down widget is accurate and matching the widget data.
        """

        product_application_name = cicd_job_count_filters[0]
        filters = cicd_job_count_filters[1]
        x_axis = cicd_job_count_filters[2]
        stacks = [cicd_job_count_filters[3]]
        exclude_filters = cicd_job_count_filters[4]

        org_ids = cicd_env.org_ids
        if not org_ids:
            LOG.info("Skipping this test case as CICD integration configuration is not found for this tenant.")
            pytest.skip("Skipping this test case as CICD integration configuration is not found for this tenant.")

        verification_failure = []
        skip_scenarios = 0

        LOG.info(f"\n\n====== Test case execution - test_job_count_widget_with_all_filter_combinations for "
                 f"filter combinations: X-Axis: {x_axis}, Filters: {filters}, "
                 f"Stacks: {stacks} ======")

        for org_id in org_ids:
            # Fetching integration IDs.
            integration_ids = cicd_env.integration_details[org_id].get(product_application_name)
            if not integration_ids:
                continue

            job_run_filter_name = filters
            parsed_values = []
            for field, values in cicd_env.generic_obj.api_data["cicd_job_count_config"].items():
                if values.get("filters") == filters:
                    job_run_filter_name = field
                    break

            verify_round = 0
            while verify_round < 5:
                cicd_job_count_filter = dict()
                dynamic_filter_list = []
                verify_round = verify_round + 1

                LOG.info(f"Verification round: {verify_round}")
                cicd_job_run_helper_obj = cicd_env.cicd_job_run_helper
                cicd_job_count_filter = cicd_job_run_helper_obj.get_job_run_filter_values(
                    integration_ids=integration_ids, filter_name=job_run_filter_name,
                    skip_values=parsed_values
                )
                LOG.info(f"Random value selected for filter '{job_run_filter_name}': {cicd_job_count_filter}")

                if cicd_job_count_filter.get("field_value") == "":
                    LOG.info(f"Skipping the 'Filter' = '{filters}' combination from the test case as no " \
                            "value is present for this field.")
                    filters = ""
                    # Jumping this round as the last round as the widget will be checked only
                    # for Stack and X-Axis combination. Hence, one combination will suffice the scenario
                    # verification.
                    verify_round = 5
                else:
                    if filters in ["stage_status", "step_status"]:
                        parsed_values.append(cicd_job_count_filter["parent_value"])
                    else:
                        parsed_values.append(cicd_job_count_filter["field_value"])
                    # Refine dynamic filter list to send it in the Payload.
                    if filters == "rollback":
                        dynamic_filter_list.append(
                            (
                                filters,
                                cicd_job_count_filter["field_value"],
                            )
                        )
                    elif filters != "end_time":
                        if exclude_filters == "yes":
                            dynamic_filter_list.append(
                                (
                                    "exclude",
                                    {filters: [cicd_job_count_filter["field_value"]]},
                                )
                            )
                        else:
                            dynamic_filter_list.append(
                                (
                                    filters,
                                    [cicd_job_count_filter["field_value"]],
                                )
                            )
                        # For stage_status filter, stage name is the parent filter in the UI. Hence, adding
                        # stage name in the filter list.
                        if filters == "stage_status":
                            dynamic_filter_list.append(
                                ("stage_name", [cicd_job_count_filter["parent_value"]])
                            )
                        # Step name is the parent filter for 'step_status' filter in the UI. Hence, adding
                        # step name in the filter list.
                        elif filters == "step_status":
                            dynamic_filter_list.append(
                                ("step_name", [cicd_job_count_filter["parent_value"]])
                            )

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

                # Create base URL and generate payload
                LOG.info("===== Executing CICD job count report widget endpoint =====")
                cicd_job_count_widget_url = cicd_env.generic_obj.connection["base_url"] + \
                    cicd_env.generic_obj.api_data["CICD_JOB_COUNT_WIDGET_URL"]

                ## CICD Job Count Widget ##
                payload = generate_cicd_job_count_payload(
                    arg_across=x_axis,
                    arg_end_time=time_range,
                    arg_req_integration_ids=integration_ids,
                    arg_req_dynamic_filters=dynamic_filter_list,
                    arg_stacks=stacks,
                    arg_sort_xaxis="value_high-low",
                    arg_ou_id=org_id,
                    arg_sort = [{"id": "count", "desc": True}]
                )
                LOG.info(f"CICD Job count payload: {json.dumps(payload)}")

                # Executing CICD Job count widget API.
                widget_response = cicd_env.generic_obj.execute_api_call(
                    url=cicd_job_count_widget_url,
                    request_type="post",
                    data=payload,
                )

                widget_record = widget_response.get("records", [])
                if not widget_record:
                    LOG.info(f"Skipping this round due to not enough records for Filters: {cicd_job_count_filter}.")
                    skip_scenarios = skip_scenarios + 1
                    continue

                widget_record_count = sum(item.get("count", 0) for item in widget_record)

                # Fetching the value of X-Axis field to send in Drilldown widget.
                filter_name = widget_record[0]["key"]
                widget_record_drill_down_cnt = widget_record[0]["count"]

                ## CICD Job Count Drilldown Widget ##
                drilldown_url = cicd_env.generic_obj.connection["base_url"] + \
                    cicd_env.generic_obj.api_data["CICD_JOB_COUNT_WIDGET_LIST_URL"]

                x_axis_filter = cicd_env.generic_obj.api_data["cicd_job_count_config"][x_axis]["filters"]
                dynamic_filter_list.append(
                    (x_axis_filter, [filter_name])
                )
                LOG.info(f"Dynamic filter list in Drilldown payload: {dynamic_filter_list}")

                ## CICD Job Count drilldown widget ##
                payload = generate_cicd_job_count_payload(
                    arg_across=x_axis,
                    arg_end_time=time_range,
                    arg_req_integration_ids=integration_ids,
                    arg_req_dynamic_filters=dynamic_filter_list,
                    arg_ou_exclusions=["projects"],
                    arg_ou_id=org_id
                )

                LOG.info(f"CICD job count drilldown payload: {json.dumps(payload)}")

                # Executing drill-down feature API.
                drilldown_response = cicd_env.generic_obj.execute_api_call(
                    url=drilldown_url, request_type="post", data=payload
                )

                drilldown_response_record = drilldown_response.get("records", [])

                data_accuracy_job_count_vs_drilldown = True
                for record in drilldown_response_record:
                    if record[cicd_env.generic_obj.api_data[
                        "cicd_job_count_config"][x_axis]["response"]] != filter_name:
                        data_accuracy_job_count_vs_drilldown = False
                        LOG.info(
                            f"Data accuracy check with drilldown widget failed. Drilldown widget value: "
                            f"{record[cicd_env.generic_obj.api_data['cicd_job_count_config'][x_axis]['response']]},"
                            f" Filter Name: {filter_name}"
                        )
                        verification_failure.append((
                                f"Verify Round: {verify_round}",
                                f"data_accuracy_job_count_vs_drilldown: {data_accuracy_job_count_vs_drilldown}",
                                f"record: {record}",
                                f"dynamic_filter_list: {dynamic_filter_list}"
                            ))
                        break

                count_verify_job_count_vs_drilldown = (
                    len(drilldown_response_record) == widget_record_drill_down_cnt
                )

                LOG.info(
                    "Assert: count check CICD Job Count -> Drilldown widget: "
                    f"{count_verify_job_count_vs_drilldown}"
                )
                LOG.info(
                    "Assert: data accuracy check CICD Job Count -> Drilldown "
                    f"widget: {data_accuracy_job_count_vs_drilldown}"
                )
                LOG.info("(CICD Job count - Total records) widget_record_count: "
                        f"{widget_record_count}")

                if filters in cicd_env.filters_to_skip_for_datastore_check:
                    LOG.info(f"Filter '{filters}' is currently skipped from the automation pipeline"
                            " for the data store check.")
                # If datastore check is not disabled, verify the widget records with the datastore.
                elif cicd_env.disable_datastore_check.get(product_application_name) != True:
                    LOG.info(
                        f"Fetching count of records from the data store for filters: {cicd_job_count_filter}"
                    )
                    skip_fields_from_datastore_check = cicd_env.integration_details[org_id].get(
                        "skip_fields_from_datastore_check", []
                    )
                    with open(datastore_prep[product_application_name]["datastore"], "r") as json_file:
                        data = json.load(json_file)

                    datastore_filter = {}
                    if cicd_job_count_filter["field_value"] != "":
                        if exclude_filters == "yes":
                            datastore_filter = {
                                "exclude": {
                                    filters: [cicd_job_count_filter["field_value"]]
                                }
                            }
                        else:
                            datastore_filter = {
                                filters: [cicd_job_count_filter["field_value"]]
                            }
                    filtered_job_cnt_from_datastore = cicd_env.api_helper_obj.fetch_record_count_from_datastore(
                        datastore=data, filters=datastore_filter, time_range=time_range,
                        skip_fields=skip_fields_from_datastore_check
                    )
                    count_verify_job_count_vs_datastore = (
                        filtered_job_cnt_from_datastore == widget_record_count
                    )
                    LOG.info(
                        "Assert: data accuracy check CICD Job Count -> Data "
                        f"store: {count_verify_job_count_vs_datastore}"
                    )
                    LOG.info(f"(Data store count) filtered_job_cnt_from_datastore: {filtered_job_cnt_from_datastore}")
                    # assert count_verify_job_count_vs_datastore
                    if not count_verify_job_count_vs_datastore:
                        verification_failure.append((
                            f"Verify Round: {verify_round}",
                            f"count_verify_job_count_vs_datastore: {count_verify_job_count_vs_datastore}",
                            f"filtered_job_cnt_from_datastore: {filtered_job_cnt_from_datastore}",
                            f"widget_record_count: {widget_record_count}"
                        ))

                #### Assertions ####
                create_widget_helper_object.schema_validations(
                    widget_schema_validation.cicd_job_count_report(),
                    widget_response,
                    "Schema validation failed for CICD job count Report widget " "endpoint",
                )
                LOG.info("[DATA CONSISTENCY] Widget schema verification complete.")

                create_widget_helper_object.schema_validations(
                    widget_schema_validation.cicd_job_count_report_drilldown_list(),
                    drilldown_response,
                    "Schema validation failed for CICD job count report drilldown "
                    "list endpoint",
                )
                LOG.info("[DATA CONSISTENCY] Widget Drilldown schema verification complete.")

                LOG.info(f"(CICD Job count (Drilldown field '{x_axis} = {filter_name}' count) "
                        f"widget_record_drill_down_cnt: {widget_record_drill_down_cnt}")
                LOG.info("(CICD Job count Drilldown count) drilldown_response_count: "
                        f"{len(drilldown_response_record)}")

                # assert count_verify_job_count_vs_drilldown
                if not count_verify_job_count_vs_drilldown:
                    verification_failure.append((
                            f"Verify Round: {verify_round}",
                            f"count_verify_job_count_vs_drilldown: {count_verify_job_count_vs_drilldown}",
                            f"drilldown_response_record: {drilldown_response_record}",
                            f"widget_record_drill_down_cnt: {widget_record_drill_down_cnt}"
                        ))

            if skip_scenarios == 5:
                LOG.info(f"Skipping the test case with filters: '{filters} due to not records to test.")
                pytest.skip(f"Skipping the test case with filters: '{filters} due to not records to test.")

            assert len(verification_failure) == 0