import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmCommitSingleStatHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()


    def scm_commit_single_stat(self, integration_id, across="trend", agg_type="average",
                               code_change_size_config={"small": "100", "medium": "1000"}, time_period=1,
                               code_change_size_unit="lines",
                               var_filters=False
                               ):
        """ create scm single stat """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"agg_type": agg_type, "code_change_size_config": code_change_size_config,
                   "code_change_size_unit": code_change_size_unit,
                   "integration_ids": integration_id, "time_period": time_period, "visualization": "pie_chart"}
        if var_filters:
            filters.update(var_filters)
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        filters["committed_at"] = {"$gt": gt, '$lt': lt}
        resp = self.widget.create_scm_commit_single_stat(ou_ids=org_id, filters=filters, across=across)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None
