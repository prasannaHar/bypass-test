import logging
import urllib3
import pytest 

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueBacklogHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info=self.generic.get_env_based_info()


    def issue_backlog_trend_report(self, product_id, snapshot_range, across="trend", interval="week", var_filters=False,
                                   keys=False):
        """ create issue backlog report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"sort_xaxis": "default_old-latest", "snapshot_range": snapshot_range,
                   "product_id": product_id}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_backlog_trend_report(ou_ids=org_id, filters=filters, across=across,
                                                             interval=interval)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            if keys:
                keys_id = {}
                for key in resp["records"]:
                    keys_id[key["key"]] = key["total_tickets"]
                LOG.info("key with total tickets    : {}".format(keys_id))
                return keys_id
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def issue_backlog_trend_drilldown(self, product_id, key, snapshot_range, across="trend", var_filters=False):
        """get drilldown of each key detatils"""

        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"across": across, "ingested_at": int(key), "integration_ids": [], "product_id": product_id,
                   "snapshot_range": snapshot_range}
        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_list(filters, org_id, across=across)

        return resp_assign
