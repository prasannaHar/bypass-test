import logging
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
from src.utils.api_reusable_functions import ApiReusableFunctions

# api_reusable_funct = ApiReusableFunctions()


class TestScmPrResponseTimeHelper:

    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.api_reusable_funct=ApiReusableFunctions(generic_helper)

    def scm_pr_response_time_report(self, integration_id, across="author", var_filters=False, metrics=False,
                                    keys=False, interval=None, sort=None, desc=None, sort_value=None, sort_list=False):
        """ create scm scm_pr_response_time_report """
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        base_url = self.generic.connection["base_url"] + self.api_info[
            "scm_author_response_time_report"]
        filters = {"integration_ids": integration_id, "code_change_size_unit": "files",
                   "code_change_size_config": {"small": "50", "medium": "150"},
                   "comment_density_size_config": {"shallow": "1", "good": "5"},
                   "pr_closed_at": {"$gt": gt, "$lt": lt}}
        if metrics:
            filters.update({"metrics": metrics})
            if "author" in metrics:
                base_url = base_url
            else:
                base_url = self.generic.connection["base_url"] + self.api_info[
                    "scm_reviewer_response_time_report"]
        if sort_list:
            filters.update({"sort_xaxis": sort_value})

        if var_filters:
            filters.update(var_filters)

        resp = self.widget.create_widget_report(ou_ids=org_id, filters=filters, across=across, base_url=base_url,
                                                interval=interval, sort=sort, desc=desc)
        if resp:
            if keys:
                multikeys = {}
                for key in resp["records"]:
                    try:
                        if key["key"] in multikeys:
                            multikeys[key["key"]] = multikeys[key["key"]] + key["count"]
                        else:
                            multikeys[key["key"]] = key["count"]

                    except:
                        continue
                sorted_dict = dict(sorted(multikeys.items(), key=lambda x: x[1], reverse=True)[:3])
                return sorted_dict
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            pytest.skip("no data in widget API")
            return None

    def scm_pr_response_time_drilldown(self, integration_id, key, key_option="author", across="repo_id",
                                       var_filters=False):
        """ create scm pr response time drilldown  """
        gt, lt = self.generic.get_epoc_utc(value_and_type=self.env_info['scm_default_time_range'])
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        filters = {"integration_ids": integration_id, key_option: [key], "code_change_size_unit": "files",
                   "code_change_size_config": {"small": "50", "medium": "150"},
                   "comment_density_size_config": {"shallow": "1", "good": "5"},
                    "pr_closed_at": {"$gt": gt, "$lt": lt}}

        if var_filters:
            filters.update(var_filters)

        resp = self.widget.scm_pr_list_drilldown_list(ou_ids=org_id, filters=filters, ou_exclusions=key_option,
                                                      across=across)

        return resp

    def check_response_sorting_by_value(self, metrics, sort_value, response):
        mean = []
        median = []
        lst = []
        if response["_metadata"] == 0:
            return None
        else:
            for i in range(0, len(response['records'])):
                mean.append(response['records'][i]['mean'])
                median.append((response['records'][i]['median']))

            if "average" in metrics:
                if "low-high" in sort_value:
                    asc = self.api_reusable_funct.is_ascending(mean)
                    if not asc:
                        lst.append("average-low-high-{}".format(metrics))
                elif "high-low" in sort_value:
                    desc = self.api_reusable_funct.is_descending(mean)
                    if not desc:
                        lst.append("average-high-low-{}".format(metrics))

            if "median" in metrics:
                if "low-high" in sort_value:
                    asc = self.api_reusable_funct.is_ascending(median)
                    if not asc:
                        lst.append("median-low-high-{}".format(metrics))
                elif "high-low" in sort_value:
                    desc = self.api_reusable_funct.is_descending(median)
                    if not desc:
                        lst.append("median-high-low-{}".format(metrics))

        return lst

    def check_response_sorting_by_label(self, metrics, sort_value, response):
        lst = []
        label = []

        if response["_metadata"] == 0:
            return None

        for i in range(0, len(response['records'])):
            label.append(response['records'][i]['key'])

        if "low-high" in sort_value:
            asc = self.api_reusable_funct.is_ascending(label)
            if not asc:
                lst.append("label-low-high-{}".format(metrics))

            elif "high-low" in sort_value:
                desc = self.api_reusable_funct.is_descending(label)
                if not desc:
                    lst.append("label-high-low-{}".format(metrics))

        return lst
