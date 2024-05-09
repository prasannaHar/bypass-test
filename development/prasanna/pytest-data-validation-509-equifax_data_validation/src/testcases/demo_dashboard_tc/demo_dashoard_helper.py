import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestDemotestHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info=self.generic.get_env_based_info()


    def issue_progress_report(self, product_id, epics=False):
        """ create issue progress report and gives epics _ids """
        integration_id = self.generic.integration_ids_basedon_workspace()
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        resp = self.widget.create_assignee_allocation_report(product_id, integration_id, org_id)
        if epics:
            epics_id = []
            for epic_id in resp["records"]:
                epics_id.append(epic_id["key"])
            return epics_id
        return resp

    def epics_data(self, product_id, epics_list):
        """get epics data list from the drilldown"""

        values_dict = {}
        integration_id = self.generic.integration_ids_basedon_workspace()
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        resp = self.widget.create_assignee_allocation_report(product_id, integration_id, org_id, epics_list)
        if resp["records"]:
            for names in resp["records"]:
                if names["total"] == len(names["assignees"]):
                    values_dict[names["key"]] = names["total"]
                else:
                    values_dict[names["key"]] = 0
        return values_dict

    def epics_drilldown(self, product_id, epics):
        """get drilldown assignee detatils"""
        resp_assign_list = []
        integration_id = self.generic.integration_ids_basedon_workspace()
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters={"integration_ids":integration_id,"epics":[epics]}
        resp_assign = self.widget.jira_drilldown_list(filters=filters,across="epic",ou_ids=org_id)
        if resp_assign["records"]:
            for names in resp_assign["records"]:
                if names["assignee"]:
                    resp_assign_list.append(names["assignee"])
        return resp_assign_list
