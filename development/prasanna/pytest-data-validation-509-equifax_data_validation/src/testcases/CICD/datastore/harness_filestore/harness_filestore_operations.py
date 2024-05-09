import requests
import json
import logging
from codecs import encode

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class HarnessFilestore():
    def __init__(self, generic_obj):
        self.generic_obj = generic_obj
        self.account_identifier = self.generic_obj.get_env_var_info("HARNESS_ACCOUNTIDENTIFIER")
        self.org_identifier = self.generic_obj.api_data["CICD_HARNESS"]["HARNESS_ORGIDENTIFIER"]
        self.project_identifier = self.generic_obj.api_data["CICD_HARNESS"]["HARNESS_PROJECTIDENTIFIER"]
        self.harness_base_url = self.generic_obj.get_env_var_info("HARNESS_BASE_URL").rstrip("/")
        self.harness_token = self.generic_obj.get_env_var_info("HARNESS_X_API_KEY")
        self.boundary = "&"
        self.params = {
            "accountIdentifier": self.account_identifier,
            "orgIdentifier": self.org_identifier,
            "projectIdentifier": self.project_identifier,
        }

    def get_folder_content(self, folder_name, folder_identifier):
        """
        Helper function to get the content of the folder from Harness File store.

        folder_name (str): Folder name.
        folder_identifier (str): Identifier assigned to folder.

        Returns: Folder content (dict).
        """

        folder_url = self.harness_base_url + \
            self.generic_obj.api_data["HARNESS_FILESTORE"]["get_folder_content"]

        payload = {
            "identifier": folder_identifier,
            "name": folder_name,
            "type": "FOLDER"
        }
        headers = {
            "content-type": "application/json",
            "x-api-key": self.harness_token
        }

        resp = requests.post(folder_url, data = json.dumps(payload),
                             params = self.params, headers = headers)

        if resp.status_code != 200:
            LOG.info(f"Failure occurred while fetching contents of the folder '{folder_name}' on Harness File store."
                     f" status_code: {resp.status_code}.")
            raise Exception(f"Harness Filestore: Failure while fetching '{folder_name}' folder content. Error: "
                            f"{resp.json()}")

        folder_content = resp.json()

        return folder_content

    def get_file_content(self, file_identifier):
        """
        Helper function to get the content of the folder from Harness File store.

        file_identifier (str): Identifier assigned to folder.

        Returns: File content (dict).
        """

        download_url = self.harness_base_url + \
            self.generic_obj.api_data["HARNESS_FILESTORE"]["download_file"].rstrip("/") + \
                f"/{file_identifier}/download"

        headers = {
            "content-type": "application/json",
            "x-api-key": self.harness_token
        }

        resp = requests.get(download_url, params = self.params, headers = headers)

        if resp.status_code != 200:
            LOG.info(f"Failure occurred while fetching contents of the file '{file_identifier}' on Harness File store."
                     f" status_code: {resp.status_code}.")
            raise Exception(f"Harness Filestore: Failure while fetching '{file_identifier}' file content. Error: "
                            f"{resp.json()}")

        file_content = resp.json()

        return file_content

    def prepare_payload(self, payload):
        """
        Prepare payload for Harness File store POST, PUT API calls.
        Args:
        payload (dict): Payload to be sent in the API call.

        Returns: Payload in the expected format of Harness API.
        """

        dataList = []
        for key, value in payload.items():
            dataList.append(encode('--' + self.boundary))
            dataList.append(encode(f'Content-Disposition: form-data; name={key};'))

            dataList.append(encode('Content-Type: {}'.format('text/plain')))
            dataList.append(encode(''))

            dataList.append(encode(f"{value}"))

        dataList.append(encode('--'+self.boundary+'--'))
        dataList.append(encode(''))

        parsed_payload = b'\r\n'.join(dataList)

        return parsed_payload


    def create_artifact(self, artifact_details, artifact_type="FILE", update_action=False):
        """
        Create folder on the Harness File store.
        Args:
        artifact_details (dict): Artifact metadata like name, identifier, description, etc.
        artifact_type (enum): Type of artifact. (FILE, FOLDER).

        Returns: Artifact creation status (bool).
        """

        url = self.harness_base_url + \
                self.generic_obj.api_data["HARNESS_FILESTORE"]["create_file_folder_uri"]
        artifact_name = artifact_details["name"]
        artifact_details.update({
            "type": artifact_type
        })
        payload = self.prepare_payload(artifact_details)
        headers = {
            "content-type": f"multipart/form-data; boundary={self.boundary}",
            "x-api-key": self.harness_token
        }

        if update_action:
            method = "PUT"
            url = f"{url.rstrip('/')}/{artifact_details['identifier']}"
        else:
            method = "POST"

        resp = requests.request(method, url, data = payload,
                             params = self.params, headers = headers)

        if resp.status_code != 200:
            LOG.info(f"Failure occurred while creating artifact '{artifact_name}' on the Harness File store."
                     f" status_code: {resp.status_code}. Error: {resp.json()}")
            return {}

        resp_json = resp.json()
        return resp_json

    def delete_artifact(self, identifier):
        """
        Delete the artifact from the Harness datastore.
        Args:
        identifier: Identifier of the artifact to be deleted.

        Returns: Status of delete operation (bool).
        """

        delete_artifact_url = self.harness_base_url + \
            self.generic_obj.api_data["HARNESS_FILESTORE"]["delete_artifact"] + identifier

        headers = {
            "content-type": "application/json",
            "x-api-key": self.harness_token
        }

        resp = requests.delete(delete_artifact_url, params = self.params, headers = headers)

        if resp.status_code != 200:
            LOG.info(f"Failed to delete the artifact with identifier '{identifier}'")
            return False

        LOG.info(f"Artifact '{identifier}' deleted successfully.")
        return True

    def create_folder(self, folder_name, folder_identifier, parentIdentifier,
                      description="", tags=[]):
        """
        Create folder on the Harness File store.
        Args:
        folder_name (str): Folder name.
        folder_identifier (str): Identifier to be assigned to folder.
        parentIdentifier (str): Parent directory identifier in which the folder should be created.
        description (str): New folder description.
        tags (list): List of tags to align to the folder.

        Returns: Folder creation status (bool).
        """

        folder_details = {
            "name": folder_name,
            "identifier": folder_identifier,
            "parentIdentifier": parentIdentifier,
            "description": description,
            "tags": tags
        }

        folder_metadata = self.create_artifact(
            artifact_details = folder_details, artifact_type = "FOLDER"
        )

        return folder_metadata

    def create_file(self, file_name, file_identifier, parentIdentifier,
                    description="", tags=[], content={}, file_usage="MANIFEST_FILE",
                    mime_type="json", update_action=False):
        """
        Create folder on the Harness File store.
        Args:
        file_name (str): File name.
        file_identifier (str): Identifier to be assigned to file.
        parentIdentifier (str): Parent directory identifier in which the file should be created.
        description (str): New file description.
        tags (list): List of tags to align to the file.
        content (dict): Content to add in the file,
        file_usage (str): File usage type.
        mime_type (str): File type.
        update_action (bool): If file already exists, then update the file.

        Returns: File metadata (dict).
        """

        file_details = {
            "name": file_name,
            "identifier": file_identifier,
            "parentIdentifier": parentIdentifier,
            "description": description,
            "tags": tags,
            "content": content,
            "fileUsage": file_usage,
            "mimeType": mime_type
        }

        file_metadata = self.create_artifact(
            artifact_details = file_details, artifact_type = "FILE", update_action = update_action
        )

        return file_metadata

    def save_cicd_datastore(self, source_file_path, integration, datastore_timerange):
        """
        Function to store data on Harness File store.
        This function will store the data collected from product environment and also store the checkpoint 
        file to allow progressive data collection.
        Directory structure in which data will be stored on Harness File store:
        CICD/
            <integration_name>/
                <integration_name>_datastore.json
                <integration_name>_datastore_checkpoint.json

        Args:
        source_file_path (str): File path of the datastore collected on the local file system.
        integration (str): Name of integration for which datastore is prepared.
        datastore_timerange (dict): Time range for which datastore is prepared.

        Returns: Status of Harness file store task (bool).
        """

        root_identifier = "Root"
        cicd_identifier = "CICD"

        ### Root Folder ###
        root_content = self.get_folder_content(
            folder_name = root_identifier,
            folder_identifier = root_identifier
        )

        cicd_folder_present = False
        for child in root_content.get("data", {}).get("children", []):
            if child["identifier"] == cicd_identifier:
                cicd_folder_present = True
                LOG.info(f"'{child['path']}' folder is present on "
                         "the Harness File store.")
                break

        ### CICD Folder ###
        if not cicd_folder_present:
            cicd_folder_metadata = self.create_folder(
                folder_name = cicd_identifier,
                folder_identifier = cicd_identifier,
                parentIdentifier = root_identifier
            )
            if cicd_folder_metadata.get("status") != "SUCCESS":
                return False
            LOG.info(f"Created folder '{cicd_folder_metadata['data']['path']}' on the "
                     "Harness File store.")
        else:
            cicd_folder_metadata = self.get_folder_content(
                folder_name = cicd_identifier,
                folder_identifier = cicd_identifier
            )

        ### Integration Folder ###
        integration_folder_present = False
        for child in cicd_folder_metadata.get("data", {}).get("children", []):
            if child["identifier"] == integration:
                integration_folder_present = True
                LOG.info(f"'{child['path']}' folder present on "
                         "the Harness File store.")
                break

        if not integration_folder_present:
            integration_folder_metadata = self.create_folder(
                folder_name = integration,
                folder_identifier = integration,
                parentIdentifier = cicd_identifier
            )
            if integration_folder_metadata.get("status") != "SUCCESS":
                return False
            LOG.info(f"Created folder '{integration_folder_metadata['data']['path']}' "
                     f"on the Harness File store.")
        else:
            integration_folder_metadata = self.get_folder_content(
                folder_name = integration,
                folder_identifier = integration
            )

        # Storing the datastore.
        # TODO: Add rollback mechanism.
        with open(source_file_path, "r") as f:
            data = json.load(f)

        datastore_already_present = False
        datastore_metadata = {}
        datastore_identifier = integration + "_datastore"
        datastore_file_name = f"{datastore_identifier}.json"

        checkpoint_found = False
        checkpoint_metadata = {}
        checkpoint_identifier = integration + "_datastore_checkpoint"
        checkpoint_file_name = f"{checkpoint_identifier}.json"

        for child in integration_folder_metadata.get("data", {}).get("children", []):
            if child.get("identifier") == datastore_identifier:
                datastore_already_present = True
                datastore_metadata = child
                LOG.info(f"'{child['path']}' file present on "
                         "the Harness File store.")
            elif child.get("identifier") == checkpoint_identifier:
                checkpoint_found = True
                checkpoint_metadata = child
                LOG.info(f"'{child['path']}' file present on "
                         "the Harness File store.")

            if datastore_already_present and checkpoint_found:
                break

        datastore_status = self.create_file(
            file_name = datastore_file_name,
            file_identifier = datastore_identifier,
            parentIdentifier = integration,
            content = json.dumps(data),
            description = datastore_metadata.get("description", f"'{integration}' datastore."),
            update_action = datastore_already_present
        )

        if not datastore_status:
            return False
        else:
            LOG.info(f"Updated the datastore for integration '{integration}'. File path: "
                     f"{integration_folder_metadata['data']['path']}/{datastore_file_name}.")

        checkpoint_status = self.create_file(
            file_name = checkpoint_file_name,
            file_identifier = checkpoint_identifier,
            parentIdentifier = integration,
            content = json.dumps(datastore_timerange),
            description = checkpoint_metadata.get("description", f"'{integration}' datastore checkpoint."),
            update_action = checkpoint_found
        )

        if not checkpoint_status:
            return False
        else:
            LOG.info(f"Updated the checkpoint for integration '{integration}'. File path: "
                     f"{integration_folder_metadata['data']['path']}/{checkpoint_file_name}.")

        return True
