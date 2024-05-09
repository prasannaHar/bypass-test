import requests
import logging
import json
from src.utils.datetime_reusable_functions import DateTimeReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class GitlabFactory:
    """
    Class to collect events from the Gitlab environment.
    """

    def __init__(self, generic_obj, harness_filestore) -> None:
        self.generic_obj = generic_obj
        self.harness_filestore = harness_filestore
        self.gitlab_base_url = self.generic_obj.get_env_var_info("GITLAB_BASE_URL")
        if self.gitlab_base_url:
            self.gitlab_base_url = self.gitlab_base_url.rstrip("/")
        self.gitlab_token = self.generic_obj.get_env_var_info("GITLAB_TOKEN")
        self.datetime_func = DateTimeReusable()

    def execute_gitlab_api_call(self, url, payload={}, method="GET"):
        """
        Common function to execute Gitlab API calls.
        url: URL to execute.
        payload: Content to send while executing the API call.
        method: Request method for API call.

        Returns: List of records received from the API call.
        """

        headers = {
            "Authorization": f"Bearer {self.gitlab_token}",
            "Content-Type": "application/json",
        }

        # Per-page 50 records will be received.
        payload.update({
            "per_page": 50,
            "page": 1
        })

        has_next_page = True
        records = []

        # Iterate over the API call until all records are received.
        while has_next_page:
            LOG.info(f"Collecting records from page {payload['page']}")
            if method == "GET":
                resp = requests.get(url, headers=headers, params = payload)
            else:
                resp = requests.request(method, url, headers=headers,
                                         data=json.dumps(payload))

            # Fetch response from the API call.
            try:
                resp_json = resp.json()
                if resp.status_code != 200:
                    LOG.info("Error occurred while executing the Gitlab API call. "
                             f"Status Code: {resp.status_code}, Error: {resp_json}")
                    return False, resp_json
            except:
                LOG.info("Error occurred while executing the Gitlab API call. "
                             f"Status Code: {resp.status_code}, Error: {resp.text}")
                return False, resp.text

            # Accumulate paginated records received in a list.
            if isinstance(resp_json, list):
                records.extend(resp_json)
            else:
                records.append(resp_json)

            # Check if next page is available for fetching data.
            if not resp.headers.get("x-next-page"):
                has_next_page = False
                continue

            # Increment page to fetch more records.
            payload["page"] = resp.headers["x-next-page"]

        return True, records

    def get_gitlab_projects(self):
        """
        Execute the Gitlab Projects API calls for collecting Projects information.
        """
        LOG.info("Collecting Gitlab projects metadata...")
        gitlab_project_url = self.gitlab_base_url + \
            self.generic_obj.api_data["CICD_GITLAB"]["PROJECTS_URL"]

        # Exit if required information is not provided for fetching information from
        # Harness environment.
        if not self.gitlab_token or not self.gitlab_base_url:
            LOG.info("Required export variables ['GITLAB_BASE_URL', 'GITLAB_TOKEN'] "
                     "for Gitlab not found.")
            return False, []

        params = {"owned": True}

        # Fetch projects list.
        status, projects_list = self.execute_gitlab_api_call(
            gitlab_project_url, params
        )

        if not status:
            return False, []

        return True, projects_list

    def get_gitlab_pipline_summary_data(self, time_range):
        """
        Execute the Gitlab Pipelines API call for collecting Pipelines information.

        time_range: Time range for which to collect the data from Gitlab.

        Returns: List of pipelines in the selected time range.
        """

        LOG.info(f"Fetching Gitlab pipeline updates for timerange: {time_range}")
        # Collect projects information from Gitlab.
        project_fetch_status, projects = self.get_gitlab_projects()

        if not project_fetch_status:
            return False, []

        LOG.info(f"Collected {len(projects)} projects from the Gitlab environment.")

        # Convert date in the expected format for API execution.
        updated_after = self.datetime_func.get_time_in_expected_format(time_range["$gt"])
        updated_before = self.datetime_func.get_time_in_expected_format(time_range["$lt"])

        params = {
            "updated_after": updated_after,
            "updated_before": updated_before,
            "order_by": "updated_at"
        }

        pipeline_summary = []

        # Iterate over projects to fetch pipelines information.
        for project in projects:
            project_id = project["id"]
            project_name = project["name"]

            pipeline_url = self.gitlab_base_url + \
                    self.generic_obj.api_data["CICD_GITLAB"]["PROJECTS_URL"].rstrip("/") + \
                        f"/{project_id}/pipelines"

            # Fetch list of pipelines in the project.
            status, pipeline_list = self.execute_gitlab_api_call(
                pipeline_url, params
            )

            if not status:
                return False, []

            LOG.info(f"Total {len(pipeline_list)} pipelines for Project: {project_name}")

            # Iterate over pipelines list to fetch detail of each pipeline.
            for pipeline in pipeline_list:
                project_details = {}
                pipeline_detail_url = pipeline_url + f"/{pipeline['id']}"
                LOG.info(f"Fetching details of Pipeline ID {pipeline['id']}...")

                # Fetch pipeline details.
                detail_status, pipeline_detail = self.execute_gitlab_api_call(
                    pipeline_detail_url
                )
                pipeline_detail = pipeline_detail[0]

                if not detail_status:
                    return False, []

                # Prepare data to store in Harness datastore.
                project_details.update({
                    "project_name": project["path_with_namespace"],
                    "job_run_number": pipeline_detail["id"],
                    "job_status": pipeline_detail["status"],
                    "start_time": self.datetime_func.convert_date_in_epoch(pipeline_detail.get("started_at", "")),
                    "end_time": self.datetime_func.convert_date_in_epoch(pipeline_detail.get("finished_at", "")),
                    "duration": pipeline_detail.get("duration", 0),
                    "job_name": pipeline_detail["ref"],
                    "job_normalized_full_name": project["path_with_namespace"] + \
                        f"/{pipeline_detail['ref']}",
                    "cicd_user_id": pipeline_detail.get("user", {}).get("name")
                })

                pipeline_summary.append(project_details)

        return True, pipeline_summary

    def store_data_into_json(self, file_path, time_range):
        """
        Parse the data collected from Gitlab environment. Store only those fields
        which are required to verify the records in the widgets during the test cases
        execution.
        Args:
        file_path (str): Destination file path where the parsed data will be stored on local file system.
        time_range (dict): Start and end time for the data to be fetched from the Harness environment.

        Returns: Status of data parsing task.
        """

        LOG.info(
            "==== Gitlab Factory fetching data from Gitlab API with pagination ===="
        )
        pipeline_updates_fetch_statsus, pipelines_list = self.get_gitlab_pipline_summary_data(time_range)

        if not pipeline_updates_fetch_statsus:
            raise Exception("Error occurred while fetching pipeline updates from Gitlab environment.")

        # Store the parsed data in the data store.
        with open(file_path, "w", encoding="utf8") as json_file:
            json.dump(pipelines_list, json_file)

        LOG.info(f"Gitlab Data collection complete. Total pipelines collected: {len(pipelines_list)}.")
        return True