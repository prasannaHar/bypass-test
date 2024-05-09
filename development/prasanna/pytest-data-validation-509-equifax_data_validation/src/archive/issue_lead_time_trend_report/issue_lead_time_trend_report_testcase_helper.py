import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueLeadTimeTrendtHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()

    def issues_lead_time_trend_report(self, across="trend", calculation="ticket_velocity", var_filters=False,
                                      keys=False):
        """ create issue  lead time TREND REPORT """
        # org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        org_id= [(self.env_info['set_ous'])[0]]
        integration_id = self.generic.integration_ids_basedon_workspace()
        velocity_config_id = self.env_info["env_jira_velocity_config_id"]
        project_names = self.env_info["project_names"]
        # gt, lt = self.generic.get_epoc_time(value=1)
        jira_default_time_range = self.generic.env["jira_default_time_range"]
        gt, lt = self.generic.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filters = {"calculation": calculation, "work_items_type": "jira",
                   "jira_issue_resolved_at": {"$gt": gt, "$lt": lt},
                   "limit_to_only_applicable_data": False, "velocity_config_id": velocity_config_id,
                   "integration_ids": integration_id, "jira_projects": project_names}

        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_lead_time_by_stage_report(ou_ids=org_id, filters=filters, across=across)
        if resp["records"]:
            if keys:
                keys_id = {}
                for key in resp["records"]:
                    keys_id[key["key"]] = key["data"][0]["count"]
                return keys_id
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def issues_lead_time_trend_report_drilldown(self, key, across="values", var_filters=False,
                                                calculation="ticket_velocity"):
        """get drilldown of each key detatils"""

        # org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        org_id = [(self.env_info['set_ous'])[0]]
        velocity_config_id = self.env_info["env_jira_velocity_config_id"]
        project_names = self.env_info["project_names"]
        integration_id = self.generic.integration_ids_basedon_workspace()
        sort = [{"id": [{"id": [], "desc": True}], "desc": True}]
        # gt, lt = self.generic.get_epoc_time(value=1)
        jira_default_time_range = self.generic.env["jira_default_time_range"]
        gt, lt = self.generic.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filters = {"calculation": calculation, "integration_ids": integration_id,
                   "jira_issue_resolved_at": {"$gt": gt, "$lt": lt},
                   "limit_to_only_applicable_data": False, "velocity_config_id": velocity_config_id,
                   "value_trend_keys": [key], "jira_projects": project_names}

        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_velocity_values(filters, ou_ids=org_id, across=across,
                                                                 ou_exclusions=across,
                                                                 sort=sort)
        return resp_assign
