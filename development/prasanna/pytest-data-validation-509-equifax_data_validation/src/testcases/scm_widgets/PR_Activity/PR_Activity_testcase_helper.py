import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestPrActivityHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info=self.generic.get_env_based_info()


    def pr_activity(self, time_range, across="repo_id", var_filters=False):
        """ create PR Activity """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"ou_ref_ids": org_id, "across": across, "time_range": time_range}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_pr_activity(filters=filters)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None
