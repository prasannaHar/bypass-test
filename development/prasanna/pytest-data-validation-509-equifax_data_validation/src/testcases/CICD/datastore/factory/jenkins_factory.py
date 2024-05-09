import requests
from requests.auth import HTTPBasicAuth
import logging
import json

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class JenkinsFactory:
    """
    Class to collect events from the Jenkins environment.
    """
    def __init__(self, generic_obj, harness_filestore):
        self.generic_obj = generic_obj
        self.harness_filestore = harness_filestore
        self.jenkins_base_url = self.generic_obj.get_env_var_info("JENKINS_BASE_URL")

    def get_jenkins_job_summary_data(self):
        """
        Execute the Jenkins jobs API calls for collecting jobs' information.
        """
        jenkins_job_summary_url = self.jenkins_base_url.rstrip("/") + \
            self.generic_obj.api_data["CICD_JENKINS"]["JENKINS_JOB_SUMMARY_URL"]
        self.jenkins_username = self.generic_obj.get_env_var_info("JENKINS_USERNAME")
        self.jenkins_password = self.generic_obj.get_env_var_info("JENKINS_PASSWORD")

        # Exit if required information is not provided for fetching information from
        # Jenkins environment.
        if not self.jenkins_username or not self.jenkins_password or not self.jenkins_base_url:
            LOG.info("Required export variables ['JENKINS_BASE_URL', 'JENKINS_USERNAME', "
                     "'JENKINS_PASSWORD'] for Jenkins not found.")
            return {}

        authorization = HTTPBasicAuth(self.jenkins_username, self.jenkins_password)
        resp = []
        resp = requests.get(jenkins_job_summary_url, auth=authorization)
        if resp.status_code != 200:
            LOG.info(f"Error occurred while preparing Jenkins datastore. Error Code: {resp.status_code}")
            return resp

        resp = resp.json()

        return resp

    def store_data_into_json(self, file_path, time_range):
        """
        Parse the data collected from Jenkins environment. Store only those fields
        which are required to verify the records in the widgets during the test cases
        execution.
        Args:
        file_path (str): Destination file path where the parsed data will be stored on local file system.
        time_range (dict): Start and end time for the data to be fetched from the Jenkins environment.

        Returns: Status of data parsing task.
        """

        LOG.info(
            "==== Jenkins Factory fetching data from Jenkins API ===="
        )
        response_obj = self.get_jenkins_job_summary_data()
        job_summary = []
        start_time_ms = int(time_range["$gt"]) * 1000
        end_time_ms = int(time_range["$lt"]) * 1000

        # Iterating over the Jenkins jobs to fetch the required information about the build run.
        for job in response_obj.get("jobs", []):
            LOG.info(f"Iterating over builds of Job : {job['name']}."
                     f" Total Builds in this job: {len(job.get('allBuilds', []))}")
            # Iterating over the builds of a particular job.
            for build in job.get("allBuilds", []):
                if not (start_time_ms <= build["timestamp"] <= end_time_ms):
                    continue

                build_details = {
                    "job_run_number": build["number"],
                    "job_name": job["name"],
                    "job_normalized_full_name": job["name"],
                    "duration": build["duration"],
                    "start_time": build["timestamp"],
                    "end_time": build["timestamp"] + build["duration"],
                    "job_status": build["result"],
                    "instance_name": "Jenkins Instance"
                }
                user_id_found = False
                scm_url_found = False

                # Iterating over the actions performed in the build to fetch the user information
                # and the repository URL.
                for action in build.get("actions", []):
                    try:
                        if not action or (user_id_found and scm_url_found):
                            continue

                        if action["_class"] == "hudson.model.CauseAction":
                            for cause in action["causes"]:
                                if cause["_class"] == "hudson.model.Cause$UserIdCause":
                                    build_details["cicd_user_id"] = cause["userId"]
                                    user_id_found = True
                                elif cause["_class"] == "hudson.triggers.TimerTrigger$TimerTriggerCause":
                                    build_details["cicd_user_id"] = "SYSTEM"
                                    user_id_found = True
                                elif cause["_class"] == "hudson.plugins.git.util.BuildData":
                                    build_details["repository"] = cause["remoteUrls"]
                                    scm_url_found = True
                    except Exception as e:
                        LOG.info(f"Error occurred while preparing Jenkins data store. Error: {e}")
                        return False

                job_summary.append(build_details)

        # Store the parsed data in the data store.
        with open(file_path, "w", encoding="utf8") as json_file:
            json.dump(job_summary, json_file)

        LOG.info(f"Jenkins Data collection complete. Total pipelines collected: {len(job_summary)}.")
        return True
