import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestBullseyeCodeCoverage:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)

    def bullseye_code_coverage(self, across="project", var_filters=False,
                               keys=False):
        """ create bullseye code coverage report """
        filters = {}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_bulleye_code_coverage(filters=filters, across=across)
        if resp["records"]:
            if keys:
                multikeys = []
                for key in resp["records"]:

                    multikeys.append({"key": key["key"], "functions_covered": key["additional_counts"]["bullseye_coverage_metrics"]["functions_covered"]})
                LOG.info("key with total tickets    : {}".format(multikeys))
                return multikeys
            LOG.info("created widget : {}".format(resp))
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def bullseye_code_coverage_drilldown(self,key, across="project", var_filters=False):
        """ create Bullseye code coverage List Drilldown  """
        filters = {"integration_ids": [], "projects": [key]}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.bullseye_files_drilldown_list(filters=filters, across=across)

        if resp["records"]:
            LOG.info(" drilldown response : {}".format(resp))

            return resp
        else:
            LOG.warning("No Data In Drilldown Api")
            return None
