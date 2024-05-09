import logging
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueBounceHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()

    def issue_bounce_report(self, across="assignee", across_limit=20, sort_id="bounces", var_filters=False,
                            keys=False):
        """ create issue backlog report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        sort = [{"id": sort_id, "desc": True}]
        filters = {"sort_xaxis": "value_high-low", "integration_ids": integration_id}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_bounce_report(ou_ids=org_id, filters=filters, across_limit=across_limit,
                                                      across=across, sort=sort)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    if key["additional_key"] != "_UNASSIGNED_":
                        multikeys.append({"key": key["key"], "total_tickets": key["total_tickets"], "min": key["min"],
                                          "max": key["max"]})
                return multikeys
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def issue_bounce_drilldown(self, key, across="assignee", var_filters=False, sort_id="bounces", bounces=False):
        """get drilldown of each key detatils"""

        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        sort = [{"id": sort_id, "desc": True}]

        filters = {"integration_ids": integration_id, "assignees": [key]}
        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_list(filters, ou_ids=org_id, across=across, ou_exclusions="assignees",
                                                      sort=sort)
        if bounces:
            bounces_value = []
            for eachbounce in resp_assign['records']:
                bounces_value.append(int(eachbounce['bounces']))
            return bounces_value

        return resp_assign
