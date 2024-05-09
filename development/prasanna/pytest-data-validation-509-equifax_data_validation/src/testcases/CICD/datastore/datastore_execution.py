from src.testcases.CICD.datastore.factory.harness_factory import HarnessFactory
from src.testcases.CICD.datastore.factory.jenkins_factory import JenkinsFactory
from src.testcases.CICD.datastore.factory.gitlab_factory import GitlabFactory
from src.testcases.CICD.datastore.factory.github_actions_factory import GithubActionsFactory
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.testcases.CICD.datastore.harness_filestore.harness_filestore_operations import HarnessFilestore
from src.lib.core_reusable_functions import epoch_timeStampsGenerationForRequiredTimePeriods
import logging
import os

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class DatastoreExecution():
    def __init__(self):
        self.generic_obj = TestGenericHelper()
        self.harness_filestore = HarnessFilestore(self.generic_obj)
        self.supported_products = {
            "harnessng": HarnessFactory,
            "jenkins": JenkinsFactory,
            "gitlab": GitlabFactory,
            "github_actions": GithubActionsFactory
        }
        self.disable_datastore_check = self.generic_obj.env.get(
            "CICD_DISABLE_DATASTORE_CHECK", {})

    def prepare_datastore(self):
        time_range = self.get_last_week_time_range()
        update_datastore = self.generic_obj.api_data.get(
            "HARNESS_FILESTORE", {}).get("update_datastore", {})
        for product, product_class in self.supported_products.items():
            if not update_datastore.get(product):
                LOG.info(f"Skipping datastore preparation for integration '{product}' as it is disabled.")
                continue

            LOG.info(f"\n\n===== Preparing datastore for the integration '{product}'")

            product_obj = product_class(self.generic_obj, self.harness_filestore)
            file_path = f"/tmp/{product}_datastore.json"

            datastore_collection_status = product_obj.store_data_into_json(
                file_path = file_path, time_range = time_range
            )

            if not datastore_collection_status:
                LOG.info(f"Datastore preparation status: {datastore_collection_status}")
                continue
            if not os.path.exists(file_path):
                LOG.info(f"Datastore file not found at the expected location '{file_path}' for the integration '{product}'")
                continue

            LOG.info(f"Data collected for integration '{product}' will be stored on Harness Filestore."
                     f" Datastore time range: {time_range}.")
            file_store_status = self.harness_filestore.save_cicd_datastore(
                source_file_path = file_path,
                integration = product,
                datastore_timerange = time_range
            )

            if file_store_status:
                LOG.info(f"===== Datastore preparation complete for the integration '{product}'.")
            else:
                LOG.info(f"===== Datastore preparation failed for the integration '{product}'.")

    def get_last_week_time_range(self):
        """
        Get time range for the Harness File store.
        """

        status, gt, lt = epoch_timeStampsGenerationForRequiredTimePeriods("LAST_WEEK")
        time_range = {"$gt": gt, "$lt": lt}
        return time_range


if __name__ == "__main__":
    obj = DatastoreExecution()
    obj.prepare_datastore()
