import random
from src.utils.cicd_payload_generator import generate_cicd_job_runs_values_payload


class CICDJobRunHelper:
    def __init__(self, generic_obj):
        self.generic_obj = generic_obj

    def get_job_run_filter_values(self, integration_ids, filter_name, skip_values=[]):
        """
        Helper function to fetch the values for the filter.
        Args:
        integration_ids (list): List of Integration IDs.
        filter_name (str): Name of the field used to filter the data.
        skip_values (list): Values to skip from the response.

        Returns:
        dict(field_name, field_value): Field name and the value of the filter.
        """
        filter_value = {
                "field_name": filter_name,
                "field_value": ""
            }
        selected_filter_value = ""

        if filter_name == "end_time":
            gt, lt = self.generic_obj.get_epoc_utc(
                self.generic_obj.env["cicd_job_count_data_collection_span"], "days", lt_time_delta=2
            )
            filter_value = {
                "field_name": filter_name,
                "field_value": {"$gt": gt, "$lt": lt}
            }
            return filter_value

        # Generate the payload to be used for executing the `/job_runs` API.
        job_run_value_payload = generate_cicd_job_runs_values_payload(
            arg_integration_ids=integration_ids,
            arg_fields=[filter_name],
            arg_req_dynamic_filters=[("integration_ids", integration_ids)],
        )

        # Stage name is required in the payload of stage status field.
        if filter_name == "stage_status":
            stage_name = self.get_job_run_filter_values(integration_ids, "stage_name", skip_values=skip_values)
            stage_name = stage_name["field_value"]

            job_run_value_payload["filter"]["stage_name"] = [stage_name]
            filter_value.update({
                "parent_field": "stage_name",
                "parent_value": stage_name
            })

        # Step name is required in the payload of the step status field.
        elif filter_name == "step_status":
            step_name = self.get_job_run_filter_values(integration_ids, "step_name", skip_values=skip_values)
            step_name = step_name["field_value"]

            job_run_value_payload["filter"]["step_name"] = [step_name]
            filter_value.update({
                "parent_field": "step_name",
                "parent_value": step_name
            })

        # URL preparation for `/job_runs` API call.
        url = (
            self.generic_obj.connection["base_url"] + \
                self.generic_obj.api_data["CICD_JOB_COUNT_WIDGET_JOB_RUN_VALUES"]
        )

        # URL preparation for `/dora/cicd-job-params` API call.
        if filter_name == "parameters":
            url = (
                self.generic_obj.connection["base_url"] + \
                    self.generic_obj.api_data["CICD_JOB_COUNT_DORA_CICD_JOB_PARAMS"]
            )

        # Executing the API call to fetch the filter values.
        filters_value_data = self.generic_obj.execute_api_call(
            url=url, request_type="POST", data=job_run_value_payload
        )

        # Handling response for `dora/cicd-job-params` API call.
        if filter_name == "parameters":
            if filters_value_data:
                selected_parameter = random.choice(list(filters_value_data.keys()))
                selected_filter_value = {"name": selected_parameter, "values": filters_value_data[selected_parameter]}
            else:
                selected_filter_value = ""

            filter_value = {
                "field_name": filter_name,
                "field_value": selected_filter_value
            }
            return filter_value

        # If filter has no values, return empty value in the filter.
        if not filters_value_data or (
            isinstance(filters_value_data, dict) and len(filters_value_data.get("records", []))
            and not filters_value_data["records"][0].get(filter_name)
        ):
            return filter_value

        filter_values = filters_value_data["records"][0][filter_name]

        # Selecting random value from the filter value list.
        selected_filter_value = random.choice(filter_values)
        retry_count = 1
        while retry_count < 10:
            # There is a possibility that filter value payload might contain metadata in the response.
            # Hence, iterating over the response to fetch a random filter value from the list.
            if "key" not in selected_filter_value or selected_filter_value.get("key") in skip_values:
                retry_count = retry_count + 1
                selected_filter_value = random.choice(filter_values)
            else:
                selected_filter_value = selected_filter_value["key"]
                break

        if retry_count == 10:
            return filter_value

        # 'Rollback' filter requires a boolean value.
        if filter_name == "rollback":
            if selected_filter_value == "false":
                selected_filter_value = False
            elif selected_filter_value == "true":
                selected_filter_value = True

        filter_value.update({
            "field_name": filter_name,
            "field_value": selected_filter_value,
        })

        return filter_value

    def generate_cicd_dora_filters_payload(self, integration_ids, pipeline_filter_eq=[],
                                              pipeline_filter_contains=[], pipeline_filter_not_eq=[],
                                              pipeline_filter_starts_with=[]
                                            ):
        """
        Function to generate payload for CICD Dora widgets.
        """

        pipeline_filters = {}
        filter_provided = False

        record_field_mappings = self.generic_obj.api_data["cicd_job_count_config"]
        # Filter for exact match of records.
        for filter in pipeline_filter_eq:
            if filter.get("field"):
                filter_provided = True
                filter_field = filter["field"]
                filter_value = filter.get("value")

                for datastore_field, mapping in record_field_mappings.items():
                    if mapping.get("filters") == filter_field:
                        filter_field = datastore_field
                        break

                if filter_value is None:
                    pipeline_filter = self.get_job_run_filter_values(
                        integration_ids=integration_ids, filter_name = filter_field
                    )
                    filter_value = pipeline_filter["field_value"]
                    if not filter_value:
                        break

                if filter_field != "rollback" and not isinstance(filter_value, list):
                    filter_value = [filter_value]

                pipeline_filters.update({
                    filter["field"]: filter_value
                })

        # Filter for sub-string match in records.
        for filter in pipeline_filter_contains:
            if filter.get("field"):
                filter_provided = True
                filter_field = filter["field"]
                filter_value = filter.get("value")

                for datastore_field, mapping in record_field_mappings.items():
                    if mapping.get("filters") == filter_field:
                        filter_field = datastore_field
                        break

                if filter_value is None:
                    pipeline_filter = self.get_job_run_filter_values(
                        integration_ids=integration_ids, filter_name = filter_field
                    )
                    filter_value = pipeline_filter["field_value"]
                    if not filter_value:
                        break

                if not pipeline_filters.get("partial_match"):
                    pipeline_filters["partial_match"] = {}

                pipeline_filters["partial_match"].update({
                    filter_field: {
                        "$contains": filter_value[0:6]
                    }
                })

        # Filter to exclude records with specific values.
        for filter in pipeline_filter_not_eq:
            if filter.get("field"):
                filter_provided = True
                filter_field = filter["field"]
                filter_value = filter.get("value")

                for datastore_field, mapping in record_field_mappings.items():
                    if mapping.get("filters") == filter_field:
                        filter_field = datastore_field
                        break

                if filter_value is None:
                    pipeline_filter = self.get_job_run_filter_values(
                        integration_ids=integration_ids, filter_name = filter_field
                    )
                    filter_value = pipeline_filter["field_value"]
                    if not filter_value:
                        break

                if filter_field != "rollback" and not isinstance(filter_value, list):
                    filter_value = [filter_value]

                if not pipeline_filters.get("exclude"):
                    pipeline_filters["exclude"] = {}

                pipeline_filters["exclude"].update({
                    filter["field"]: filter_value
                })

        # Filter for records which have field value starting with a sub-string.
        for filter in pipeline_filter_starts_with:
            if filter.get("field"):
                filter_provided = True
                filter_field = filter["field"]
                filter_value = filter.get("value")

                for datastore_field, mapping in record_field_mappings.items():
                    if mapping.get("filters") == filter_field:
                        filter_field = datastore_field
                        break

                if filter_value is None:
                    pipeline_filter = self.get_job_run_filter_values(
                        integration_ids=integration_ids, filter_name = filter_field
                    )
                    filter_value = pipeline_filter["field_value"]
                    if not filter_value:
                        break

                if not pipeline_filters.get("partial_match"):
                    pipeline_filters["partial_match"] = {}

                pipeline_filters["partial_match"].update({
                    filter_field: {
                        "$begins": filter_value[0:6]
                    }
                })

        # If no filters are expected to be applied, return default filter.
        if not filter_provided:
            pipeline_filters.update({
                "exclude": {},
                "partial_match": {}
            })

        return pipeline_filters

    def align_params_in_expected_format(self, params, filter_list=[]):
        """
        Helper function to align the parameters in the expected format for
        preparing the payload for cicd_job_runs API.
        Args:
        params (str): Parameters to consider.
        filter_list (list): List of parameters considered in the test case.

        Returns: Lisf of parameters in the expected format.
        """

        expected_format = []
        if params:
            params_list = params.split("||")
            filter_list.extend(params_list)
            expected_format = [{"field": field} for field in params_list]

        return filter_list, expected_format