import logging
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueTimeAcrossStagesHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()

    def issue_time_across_stages(self, resolved_at, metric="median_time", across="none",
                                 var_filters=False,
                                 keys=False):
        """ create issue time across stages"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        filters = {"metric": metric, "issue_resolved_at": resolved_at,
                   "integration_ids": integration_id}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_time_across_stages(ou_ids=org_id, filters=filters, across=across,
                                                           across_limit=20)

        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    multikeys.append({"stage": key["stage"], "total_tickets": key["total_tickets"]})
                LOG.info("key with total tickets    : {}".format(multikeys))
                return multikeys
            return resp
        else:
            return None

    def issue_time_across_stages_drilldown(self, key, resolved_at, across="none", metric="median_time",
                                           var_filters=False):
        """get drilldown of each key detatils"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()

        filters = {"metric": metric, "issue_resolved_at": resolved_at,
                   "integration_ids": integration_id, "include_solve_time": True, "stages": [key]}

        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_list(filters, org_id, across=across)

        return resp_assign
