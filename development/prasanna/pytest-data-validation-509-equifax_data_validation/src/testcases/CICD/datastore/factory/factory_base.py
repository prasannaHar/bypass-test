from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.testcases.CICD.datastore.harness_filestore.harness_filestore_operations import HarnessFilestore
import logging
import tempfile
import os
import json

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class FactoryBase:
    """
    Base class which prepares the ground for data collection of all supported integrations.
    """
    def __init__(self):
        self.generic_obj = TestGenericHelper()
        self.product_name = self.generic_obj.env.get(
            "cicd_product", ["CICD_HARNESS"]
        )
        self.disable_datastore_check = self.generic_obj.env.get(
            "CICD_DISABLE_DATASTORE_CHECK", {})
        self.harness_filestore = HarnessFilestore(self.generic_obj)

    def download_datastore(self):
        """
        Fetch the datastore prepared for the CICD integrations. This function will
        download the datastore and store it in local file for references during test
        cases execution.
        """
        datastore_path = {}

        # Iterate over all supported CICD integrations.
        for product in self.product_name:
            product_application_name = self.generic_obj.api_data[
                    product]["INTEGRATION_NAME"]

            # Check if datastore check is disabled for the CICD integration.
            if self.disable_datastore_check.get(product_application_name) == True:
                LOG.info(f"Skipping the datastore check for integration '{product_application_name}' as it is disabled.")
                continue

            # Creating a temp directory to store the data received from the Product API.
            # This data will be used to verify the widget response for data accuracy.
            temp_dir = tempfile.mkdtemp()
            datastore_file_name = product_application_name + "_datastore_localdownload.json"
            checkpoint_file_name = product_application_name + "_datastore_checkpoint_localdownload.json"
            local_datastore_file_path = os.path.join(temp_dir, datastore_file_name)
            local_checkpoint_file_path = os.path.join(temp_dir, checkpoint_file_name)

            # Fetch data from the Harness File store.
            try:
                datastore = self.harness_filestore.get_file_content(
                    file_identifier=f"{product_application_name}_datastore"
                )
                checkpoint = self.harness_filestore.get_file_content(
                    file_identifier = f"{product_application_name}_datastore_checkpoint"
                )

                # Storing the datastore in the local directory.
                with open(local_datastore_file_path, "w", encoding="utf8") as f:
                    json.dump(datastore, f)

                # Storing the checkpoint in the local directory.
                with open(local_checkpoint_file_path, "w", encoding="utf-8") as f:
                    json.dump(checkpoint, f)

                datastore_path[product_application_name] = {
                    "datastore": local_datastore_file_path,
                    "checkpoint": local_checkpoint_file_path
                }
            except Exception as e:
                raise Exception(f"Failed create the data store for integration '{product_application_name}'."
                                f"Error: {e}")

        return datastore_path
