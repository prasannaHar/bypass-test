import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmFilesHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info=self.generic.get_env_based_info()


    def scm_files_report(self, integration_id, across="repo_id", var_filters=False,
                         keys=False):
        """ create scm Files report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id}
        if var_filters:
            filters.update(var_filters)
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        filters["committed_at"] = {"$gt": gt, '$lt': lt}
        resp = self.widget.create_scm_files_report(ou_ids=org_id, filters=filters, across=across, )
        if resp:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    multikeys.append({"key": key["key"], "repo_id": key["repo_id"], "count": key["count"]})
                return multikeys
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def scm_files_report_drilldown(self, integration_id, module, repo_id, across="repo_id", var_filters=False):
        """ create scm Files report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id, "module": module, "repo_id": repo_id}
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        filters["committed_at"] = {"$gt": gt, '$lt': lt}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_scm_files_report(ou_ids=org_id, filters=filters, across=across)

        if resp["records"]:
            total_count = 0
            for count in resp["records"]:
                if count['repo_id']==repo_id:
                    total_count = total_count + count['count']
            return total_count
        else:
            LOG.warning("No Data In Widget Api")
            return None
