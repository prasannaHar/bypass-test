import json
import logging
from copy import deepcopy

import pytest
import pandas as pd

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.widget_reusable_functions import WidgetReusable
from src.utils.jira_report_helper import TestJiraReportHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueLeadTimeBytimeInStagestHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.jira_helper = TestJiraReportHelper(generic_helper)
        self.widgethelper = WidgetReusable(generic_helper)

    def issue_lead_time_by_time_spent_in_stages(self, issue_resolved_at, var_filters=False,
                                                keys=False):
        """ create issue_lead_time_by_time_spent_in_stages """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        velocity_config_id = self.env_info["env_jira_velocity_config_id"]
        projects = self.env_info["project_names"]

        filters = {"issue_resolved_at": issue_resolved_at,
                   "velocity_config_id": velocity_config_id, "projects": projects,
                   "integration_ids": integration_id, "work_items_type": "jira"}

        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_lead_time_by_time_spent_in_stages(ou_ids=org_id, filters=filters)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp['records']:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    multikeys.append({"key": key["key"], "mean": key["mean"], "total_tickets": key["total_tickets"]})
                return multikeys
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def issue_lead_time_by_time_spent_in_stages_drilldown(self, key, issue_resolved_at, across="none",
                                                          var_filters=False, mean=False):
        """get drilldown of each key detatils"""

        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        velocity_config_id = self.env_info["env_jira_velocity_config_id"]
        integration_id = self.generic.integration_ids_basedon_workspace()
        projects = self.env_info["project_names"]

        filters = {"issue_resolved_at": issue_resolved_at,
                   "velocity_config_id": velocity_config_id, "projects": projects,
                   "integration_ids": integration_id, "velocity_stages": [key], "work_items_type": "jira"}

        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_list(filters, ou_ids=org_id, across=across,
                                                      ou_exclusions=across)
        if mean:
            mean_value = 0
            total_tickets = resp_assign["_metadata"]["total_count"]
            for record in resp_assign['records']:
                try:
                    mean_value = mean_value + record["velocity_stage_time"]
                except:
                    continue
            if total_tickets != 0:
                mean_value = mean_value / total_tickets
                return {"mean": mean_value, "total_tickets": total_tickets}
            else:
                return None
        return resp_assign

    def get_leadtime_spent_in_stages_response(self, payload):
        leadtime_spent_in_stages_url = self.generic.connection['base_url'] + self.api_info[
            "issue_lead_time_by_time_spent_in_stages"]

        response = self.generic.execute_api_call(leadtime_spent_in_stages_url, "post", data=payload)
        return response

    def drill_down_for_each_stage(self, velocity_stage, velocity_payload):
        drilldown_url = self.generic.connection['base_url'] + self.api_info[
            "drill_down_api_url"]

        drilldown_payload = deepcopy(velocity_payload)
        # breakpoint()
        drilldown_payload.update({"across": "none", "ou_exclusions": ["none"],
                                  "sort": [{"id": velocity_stage, "desc": True}]})
        drilldown_payload['filter'].update({"velocity_stages": [velocity_stage]})
        del drilldown_payload['filter']['calculateSingleState']
        del drilldown_payload['filter']['visualization']
        del drilldown_payload['filter']['sort_xaxis']
        drilldown_response = self.generic.execute_api_call(drilldown_url, "post", data=drilldown_payload)
        return drilldown_response, drilldown_payload

    def compare_widget_mean_median_p90_p95_with_drilldown(self, widget_data, data, ou_id):
        mismatch_data_ous_list = []
        if widget_data['mean'] != data["mean"]:
            mismatch_data_ous_list.append(
                f"Mean not matching--widget_mean {widget_data['mean']},drilldown_mean {data['mean']} for the OU {ou_id}")

        if widget_data['median'] != data["median"]:
            mismatch_data_ous_list.append(
                f"Median not matching--widget_median {widget_data['median']},drilldown_median {data['median']} for the OU {ou_id}")

        if widget_data['p90'] != data["p90"]:
            mismatch_data_ous_list.append(
                f"p90 not matching--widget_p90 {widget_data['p90']},drilldown_p90 {data['p90']} for the OU {ou_id}")

        if widget_data['p95'] != data["p95"]:
            mismatch_data_ous_list.append(
                f"p95 not matching--widget_p95 {widget_data['p95']},drilldown_p95 {data['p95']} for the OU {ou_id}")

        return mismatch_data_ous_list

    def issue_lead_time_spent_in_stages_1x_widget(self, int_ids, gt, lt, custom_values, filter, filter2, exclude, ou_id,
                                                  datetime_filters, sprint, sprint_field, velocity_config_id,
                                                  fixed_date_time,
                                                  calculateSingleState=True):
        flag_list = []
        payload = {}
        try:

            jira_filter = self.jira_helper.jira_filter_creation(filter_type=filter, filter2=filter2,
                                                                integration_id=int_ids,
                                                                gt=gt, lt=lt,
                                                                custom_values=custom_values,
                                                                exclude=exclude,
                                                                datetime_filters=datetime_filters,
                                                                sprint=sprint
                                                                )

            jira_filter.update(
                {"calculateSingleState": calculateSingleState, "velocity_config_id": velocity_config_id,
                 "work_items_type": "jira"})
            if fixed_date_time is not None:
                jira_filter.update({fixed_date_time: {"$gt": gt, "$lt": lt}})

            payload = {"filter": jira_filter, "ou_ids": [ou_id],
                       "ou_user_filter_designation": {"sprint": [sprint_field]}}
            # breakpoint()
            res = self.get_leadtime_spent_in_stages_response(payload)
            if len(res['records']) == 0:
                pytest.skip("No data for the given combo")
            else:
                res_df = pd.json_normalize(res['records'], max_level=2)
                print(res_df)
                for index, row in res_df.iterrows():
                    if row['total_tickets'] != 0 and row['key'] != 'SingleState':
                        # breakpoint()
                        drilldown_resp, drilldown_payload = self.drill_down_for_each_stage(velocity_stage=row['key'],
                                                                                           velocity_payload=payload)

                        widget_data = {"mean": row['mean'], "median": row["median"], "p90": row['p90'],
                                       "p95": row["p95"]}
                        data = self.widgethelper.retrive_mean_median_p90_p95(drilldown_resp, row['key'], key="stage",
                                                                             record_path="velocity_stages",
                                                                             tc_name="issue_lead_time_by_time")
                        mismatch_data_ous_list = self.widgethelper.compare_mean_median_p0_p95(org_id=ou_id,
                                                                                              key=row['key'],
                                                                                              widget_data=widget_data,
                                                                                              drilldown_data=data)
                        if len(mismatch_data_ous_list) != 0:
                            flag_list.append({"mismatch_data_ous_list": mismatch_data_ous_list,
                                              "drilldown_payload": drilldown_payload})
                        if drilldown_resp['_metadata']['total_count'] != row['total_tickets']:
                            flag_list.append(
                                {"drilldown_resp['_metadata']['total_count']": drilldown_resp['_metadata'][
                                    'total_count'],
                                 row['key']: row['total_tickets'], "widget_payload": payload,
                                 "drilldown_payload": drilldown_payload})

        except Exception as ex:
            # breakpoint()
            LOG.info(f"Exception occured in issue_lead_time_spent_in_stages_1x_widget---{ex}")
            flag_list.append({"Exception in issue_lead_time_spent_in_stages_1x_widget": ex})

        return flag_list, json.dumps(payload)
