import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestLeadSingleStatHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()

    def lead_time_single_stat(self, calculation="ticket_velocity", across="velocity", var_filters=False, ):
        """ create issue  lead_time_single_stat"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        velocity_config_id = self.env_info["env_jira_velocity_config_id"]
        integration_id = self.generic.integration_ids_basedon_workspace()

        filters = {"calculation": calculation, "velocity_config_id": velocity_config_id,
                   "integration_ids": integration_id, "jira_projects":self.env_info["project_names"]}
        # gt, lt = self.generic.get_epoc_time(value=15, type="days")
        jira_default_time_range = self.generic.env["jira_default_time_range"]
        gt, lt = self.generic.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        jira_issue_resolved_at = {"$gt": gt, "$lt": lt}
        filters["jira_issue_resolved_at"] = jira_issue_resolved_at

        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_lead_time_by_stage_report(ou_ids=org_id, filters=filters, across=across)
        if resp["records"]:
            LOG.info("created widget : {}".format(resp))
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None
