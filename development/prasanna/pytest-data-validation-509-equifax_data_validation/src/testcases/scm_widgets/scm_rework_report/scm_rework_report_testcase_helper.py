import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmReworkHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()

    def scm_rework_report(self, integration_id, interval_config, across="author", var_filters=False,
                          keys=False):
        """ create scm rework report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id, "legacy_update_interval_config": interval_config}

        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_scm_rework_report(ou_ids=org_id, filters=filters, across=across)
        if resp:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    multikeys.append({"key": key["key"], "count": key["count"]})
                return multikeys
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def scm_rework_report_drilldown(self, integration_id, interval_config, key, key_option="authors", across="author",
                                    var_filters=False):
        """ create scm rework report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id, key_option: [key],
                   "include_metrics": True, "legacy_update_interval_config": interval_config}
        if var_filters:
            filters.update(var_filters)

        resp = self.widget.scm_commits_drilldown_list(ou_ids=org_id, filters=filters, across=across)

        return resp
