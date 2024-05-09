import requests
import logging
import json
from src.utils.datetime_reusable_functions import DateTimeReusable


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class GithubActionsFactory:
    """
    Class to collect events from the Github Actions environment.
    """

    def __init__(self,  generic_obj, harness_filestore) -> None:
        self.generic_obj = generic_obj
        self.harness_filestore = harness_filestore
        self.gha_base_url = self.generic_obj.get_env_var_info("GHA_BASE_URL")
        if not self.gha_base_url:
            self.gha_base_url = "https://api.github.com"
        self.gha_base_url = self.gha_base_url.rstrip("/")
        self.selected_repos = self.generic_obj.env.get("GHA_SELECTED_REPOS_FOR_WORKFLOW_CHECK", [])
        self.gha_token = self.generic_obj.get_env_var_info("GHA_TOKEN")
        self.datetime_func = DateTimeReusable()

    def execute_gha_api(self, url, payload={}, list_key="workflows"):
        """
        Helper method to execute Github Actions API.
        Args:
        url (str): URL to execute.
        payload (dict): Payload to send in the URL execution.
        list_key (str): Key from which list of events should be fetched from API response.

        Returns: List of events collected from the API response.
        """
        headers = {
            "Authorization": f"Bearer {self.gha_token}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.github+json"
        }

        payload.update({
            "page": 1,
            "per_page": 100
        })

        has_next = True
        resp_data = []

        # Iterate over the pages to collect list of events from the API.
        while has_next:
            LOG.info(f"Collecting data from Page: {payload['page']}")
            # Execute the API.
            try:
                resp = requests.get(url, params=payload, headers=headers)
            except Exception as e:
                LOG.error("Error occurred while fetching workflows for github repo: "
                        f"{url}. Error: {e} ")
                return []

            # Response successful.
            if resp.status_code == 200:
                resp_json = resp.json()
            else:
                LOG.error(f"GHA API '{url}' execution failed. Status Code: "
                        f"{resp.status_code}. Error: {resp.json}")
                return []

            # No more data available in the API.
            if not resp.headers.get('Link') or \
                len(resp_json.get(list_key, [])) < payload["per_page"]:
                has_next = False
            else:
                payload["page"] = payload["page"] + 1

            resp_data.extend(resp_json.get(list_key, []))

        return resp_data

    def get_workflow_runs_detail(self, github_repo, workflow_id, workflow_name,
                                 time_range):
        """
        Fetch GHA workflow runs' metadata.
        """

        parsed_data = []
        workflow_runs_url = self.gha_base_url + f"/repos/{github_repo}" + \
            self.generic_obj.api_data["CICD_GITHUB_ACTIONS"]["GHA_WORKFLOWS_API"].rstrip("/") + \
                f"/{workflow_id}/runs"

        runs_resp = self.execute_gha_api(workflow_runs_url, list_key="workflow_runs")

        if not runs_resp:
            LOG.info(f"No records found. runs_resp: {runs_resp}")
            return parsed_data

        for run_metadata in runs_resp:
            end_time = self.datetime_func.convert_github_api_datetime_format_to_epochtime(
                input_val=run_metadata["updated_at"]
            )

            if not (int(time_range["$gt"]) <= end_time / 1000 <= int(time_range["$lt"])):
                continue

            run_details = {
                "cicd_user_id": run_metadata["triggering_actor"]["login"],
                "job_normalized_full_name": f"{github_repo}/{workflow_name}",
                "project_name": github_repo,
                "job_name": workflow_name,
                "job_status": run_metadata["conclusion"],
                "repository": [run_metadata["head_repository"]["html_url"]],
                "branch": run_metadata["head_branch"],
                "end_time": end_time,
                "job_run_number": run_metadata["run_number"],
                "start_time": self.datetime_func.convert_github_api_datetime_format_to_epochtime(run_metadata["run_started_at"])
            }
            parsed_data.append(run_details)

        LOG.info(f"Total {len(parsed_data)} Workflow runs parsed for workflow: {workflow_name}")

        return parsed_data


    def store_data_into_json(self, file_path, time_range):
        """
        Parse the data collected from Github Actions environment. Store only those fields
        which are required to verify the records in the widgets during the test cases
        execution.
        Args:
        file_path (str): Destination file path where the parsed data will be stored on local file system.
        time_range (dict): Start and end time for the data to be fetched from the Harness environment.

        Returns: Status of data parsing task.
        """

        pipeline_list = []

        LOG.info(f"Fetching Github Actions workflows for time range: {time_range}")
        # Iterate over the repos to fetch Workflow run details.
        for repo in self.selected_repos:
            LOG.info(f"Fetching workflow details in repository: {repo}")
            # Fetch workflows.
            workflows_url = self.gha_base_url + f"/repos/{repo}" + \
                self.generic_obj.api_data["CICD_GITHUB_ACTIONS"]["GHA_WORKFLOWS_API"]

            workflow_list = self.execute_gha_api(workflows_url, list_key="workflows")

            if not workflow_list:
                LOG.info(f"No records found for workflow in this repository. workflow_resp: {workflow_list}")
                continue

            LOG.info(f"Found {len(workflow_list)} workflows the repository.")

            for workflow in workflow_list:
                LOG.info(f"Fetching jobs for workflow: {workflow['name']}")

                workflow_runs_data = self.get_workflow_runs_detail(
                    github_repo=repo, workflow_id=workflow["id"],
                    workflow_name=workflow['name'], time_range = time_range
                )

                pipeline_list.extend(workflow_runs_data)

        if pipeline_list:
            # Store the parsed data in the data store.
            with open(file_path, "w", encoding="utf8") as json_file:
                json.dump(pipeline_list, json_file)
        else:
            LOG.info(f"No records found in pipeline list: {pipeline_list}")
            return False

        LOG.info(f"Github Actions Data collection complete. Total pipelines collected: {len(pipeline_list)}.")
        return True
