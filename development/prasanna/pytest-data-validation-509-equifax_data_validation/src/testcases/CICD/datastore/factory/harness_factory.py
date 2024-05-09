"""
This file caters all the helper functions to create a data store for HarnessNG CICD Integration.
This data store will then be used by the test cases for data accuracy assertions.
"""
import json
import logging
import requests

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class HarnessFactory:
    """
    Class to collect events from the Harness environment.
    """

    def __init__(self, generic_obj, harness_filestore) -> None:
        self.generic_obj = generic_obj
        self.harness_filestore = harness_filestore
        self.account_identifier = self.generic_obj.get_env_var_info("HARNESS_ACCOUNTIDENTIFIER")
        self.org_identifier = self.generic_obj.api_data["CICD_HARNESS"]["HARNESS_ORGIDENTIFIER"]
        self.project_identifier = self.generic_obj.api_data["CICD_HARNESS"]["HARNESS_PROJECTIDENTIFIER"]
        self.harness_base_url = self.generic_obj.get_env_var_info("HARNESS_BASE_URL")

    def get_harness_pipline_summary_data(self):
        """
        Execute the Harness pipeline summary API calls for collecting pipline information.
        """
        harness_pipeline_summary_url = self.harness_base_url + \
            self.generic_obj.api_data["CICD_HARNESS"]["HARNESS_PIPELINE_SUMMARY_URL"]
        harness_token = self.generic_obj.get_env_var_info("HARNESS_X_API_KEY")

        # Exit if required information is not provided for fetching information from
        # Harness environment.
        if not harness_token or not self.account_identifier or not self.harness_base_url:
            LOG.info("Required export variables ['HARNESS_BASE_URL', 'HARNESS_ACCOUNTIDENTIFIER', "
                     "'HARNESS_X_API_KEY'] for HarnessNG not found.")
            return []

        headers = {
            "x-api-key": harness_token,
            "Content-Type": "application/json",
        }

        page = 0
        hasmore = 1

        # Iterate till all data is fetched from Harness environment.
        while hasmore:
            params = {
                "accountIdentifier": self.account_identifier,
                "orgIdentifier": self.org_identifier,
                "projectIdentifier": self.project_identifier,
                "page": page,
                "size": 100,
                "sort": "endTs,DESC",
            }
            payload = json.dumps({"filterType": "PipelineExecution"})

            # Execute the REST API call.
            response = requests.request(
                "POST", harness_pipeline_summary_url, headers=headers, params=params, data=payload
            )
            response_data = response.json()["data"]

            yield response_data

            page += 1
            hasmore = not response_data["last"]

    def check_rollback_status_for_pipeline(self, pipeline_detail):
        """
        Check if Rollback stage is executed for the pipeline or not.
        Args:
        pipeline_detail: Pipeline execution details.

        Returns: Rollback execution status.
        """

        rollback_status = False
        for node in pipeline_detail["layoutNodeMap"].values():
            for module_info in node.get("moduleInfo", {}).values():
                if module_info.get("rollbackDuration"):
                    rollback_status = True
                    break

            if rollback_status:
                break

        return rollback_status

    def store_data_into_json(self, file_path, time_range):
        """
        Parse the data collected from Harness environment. Store only those fields
        which are required to verify the records in the widgets during the test cases
        execution.
        Args:
        file_path (str): Destination file path where the parsed data will be stored on local file system.
        time_range (dict): Start and end time for the data to be fetched from the Harness environment.

        Returns: Status of data parsing task.
        """

        LOG.info(
            "==== Harness Factory fetching data from Harness API with pagination ===="
        )
        response_obj = self.get_harness_pipline_summary_data()
        pipeline_summary = []
        start_time_ms = int(time_range["$gt"]) * 1000
        end_time_ms = int(time_range["$lt"]) * 1000

        # Filtering the events based on the timestamp selected.
        for res in response_obj:
            try:
                resp_data = res["content"]

                filtered_pipeline_summary = []
                in_range = True

                for item in resp_data:
                    item_end_ts = item["endTs"]
                    if item_end_ts < start_time_ms:
                        in_range = False
                        break
                    # If pipeline execution falls in the timerange, add it in the
                    # datastore.
                    if start_time_ms <= item_end_ts <= end_time_ms:
                        filtered_pipeline_summary.append(item)

                pipeline_summary.extend(filtered_pipeline_summary)

                if not in_range:
                    break
            except Exception as e:
                print(f"Error occurred in storing data: {e}")
                return False

        # Parsing the Harness data to store only those fields used in the widgets.
        final_data = [
            {
                "cicd_user_id": pipeline["executionTriggerInfo"]["triggeredBy"]["identifier"],
                "job_normalized_full_name": f"{pipeline['orgIdentifier']}/{pipeline['projectIdentifier']}/{pipeline['name']}",
                "project_name": f"{pipeline['orgIdentifier']}/{pipeline['projectIdentifier']}",
                "job_name": pipeline["name"],
                "job_status": pipeline["status"],
                "tag": pipeline["tags"],
                "repository": [scmData["scmUrl"] for scmData in pipeline["moduleInfo"].get("ci", {}).get("scmDetailsList", [])],
                "branch": pipeline["moduleInfo"].get("ci", {}).get("branch"),
                "deployment_type": pipeline["moduleInfo"].get("cd", {}).get("serviceIdentifiers", []),
                "environment": pipeline["moduleInfo"].get("cd", {}).get("envIdentifiers", []),
                "infrastructure": pipeline["moduleInfo"].get("cd", {}).get("infrastructureIdentifiers", []),
                "service": pipeline["moduleInfo"].get("cd", {}).get("serviceIdentifiers", []),
                "rollback": self.check_rollback_status_for_pipeline(pipeline),
                "end_time": pipeline["endTs"],
                "job_run_number": pipeline["runSequence"],
                "start_time": pipeline["startTs"]
            }
            for pipeline in pipeline_summary
        ]

        # Store the parsed data in the data store.
        with open(file_path, "w", encoding="utf8") as json_file:
            json.dump(final_data, json_file)

        LOG.info(f"Harness Data collection complete. Total pipelines collected: {len(final_data)}.")
        return True
