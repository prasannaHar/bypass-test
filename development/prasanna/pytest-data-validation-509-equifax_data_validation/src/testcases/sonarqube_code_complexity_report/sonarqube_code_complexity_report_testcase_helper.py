import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSonarqubeCodeComplexityHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info=self.generic.get_env_based_info()


    def sonarqube_code_complexity_report(self, integration_id, across="project", metrics="cognitive_complexity",
                                         var_filters=False):
        """ create SonarQube code complexity Report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id, "metrics": metrics}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_sonarqube_code_complexity(ou_ids=org_id, filters=filters, across=across)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None
