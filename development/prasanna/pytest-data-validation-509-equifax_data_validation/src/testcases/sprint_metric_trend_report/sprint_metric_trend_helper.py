import inspect
import json
import logging
import random
from copy import deepcopy
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSprintMetricHelper:

    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.widgetresuable = WidgetReusable(generic_helper)

    def sprint_metric_trend_report(self, completed_at, across="sprint", agg_type="average",
                                   metric=["creep_done_points", "commit_done_points", "commit_not_done_points",
                                           "creep_not_done_points"], var_filters=False,
                                   keys=False):
        """ create issue sprint metric trend report """
        project_names = self.env_info["project_names"]
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        filters = {"agg_type": agg_type, "completed_at": completed_at,
                   "metric": metric, "integration_ids": integration_id,
                   "projects": project_names}

        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_sprint_metric_trend_report(ou_ids=org_id, filters=filters, interval="week",
                                                             across=across)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            if keys:
                multikeys = []
                for key in resp["records"]:
                    multikeys.append({"additional_key": key["additional_key"], "total_issues": key["total_issues"]})
                return multikeys
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def sprint_metric_trend_drilldown(self, completed_at, key, agg_type="average", var_filters=False,
                                      ):
        """ create issue sprint metric trend report drilldown """
        project_names = self.env_info["project_names"]
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        filters = {"agg_type": agg_type, "completed_at": completed_at, "include_issue_keys": True,
                   "include_workitem_ids": True, "sprint_report": [key],
                   "integration_ids": integration_id, "projects": project_names}
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_sprint_metric_trend_report(ou_ids=org_id, filters=filters)
        return resp

    def get_additional_statuses(self, int_ids):
        """
        Sample payload - To get additional statuses we need to remove "status_categories":["Done","DONE"] from rest status
        {"integration_ids":["71","76","77","78","79","80","81","82"],"fields":["status"],
        "filter":{"integration_ids":["71","76","77","78","79","80","81","82"],"status_categories":["Done","DONE"]}}
        https://testapi1.propelo.ai/v1/jira_issues/values
        {"integration_ids":["71","76","77","78","79","80","81","82"],"fields":["status"],"filter":{"integration_ids":["71","76","77","78","79","80","81","82"]}}"""
        base_url = self.generic.connection['base_url'] + self.api_info['jira_values_end_point']
        all_status_payload = {"integration_ids": int_ids,
                              "fields": ["status"],
                              "filter": {"integration_ids": int_ids}}

        all_status_response = self.generic.execute_api_call(base_url, "post", data=all_status_payload)

        all_status_payload['filter'].update({"status_categories": ["Done", "DONE"]})
        done_status_response = self.generic.execute_api_call(base_url, "post", data=all_status_payload)
        list_all_status = []
        list_all_done_status = []

        for d in all_status_response['records'][0]['status']:
            list_all_status.extend(d.values())

        for d in done_status_response['records'][0]['status']:
            list_all_done_status.extend(d.values())
        diff = set(list_all_status) - set(list_all_done_status)
        result = list(diff)
        return result

    def filter_creation(self, filter_type, creep_buffer, filter2, integration_id, gt, lt, custom_values,
                        exclude=None, additional_done_status=None, sprint_mandate_date=None):
        """creating jira/azure filter object for payload with different filter_type"""
        # jira
        """{"filter":{"agg_type":"average","completed_at":{"$gt":"1681603200","$lt":"1683590399"},"metric":["creep_done_points","commit_done_points","commit_not_done_points","creep_not_done_points"],
        "integration_ids":["71","76","77","78","79","80","81","82"]},"across":"sprint","interval":"week","ou_ids":["1617"],
        "ou_user_filter_designation":{"sprint":["sprint_report"]},"page":0,"page_size":1000}"""
        # breakpoint()
        filter = {"agg_type": "average",
                  "metric": ["creep_done_points", "commit_done_points", "commit_not_done_points",
                             "creep_not_done_points"],
                  "integration_ids": integration_id}

        if "test_sprint_metric_widget_single_stat_drilldown_comparison_jira" == str(inspect.stack()[1][3]):
            filter = {"agg_type": "average",
                      "completed_at": {"$gt": gt, "$lt": lt},
                      "include_issue_keys": True,
                      "integration_ids": integration_id}

        if sprint_mandate_date:
            filter.update({sprint_mandate_date: {"$gt": gt, "$lt": lt}})

        if filter_type:
            if "custom" in filter_type:
                filter.update({filter_type: custom_values})
            elif filter_type == "sprint_count":
                filter.update({"sprint_count": 3})
            elif filter_type in ["issue_created_at", "issue_updated_at", "issue_resolved_at"]:
                gt1, lt1 = self.generic.get_epoc_time(value=3)
                filter.update({filter_type: {"$gt": gt1, "$lt": lt1}})
            elif filter_type == "sprint_report":
                sprint_list = self.sprint_report_filter_value(gt, lt, integration_id)
                if len(sprint_list) == 0:
                    sprint = sprint_list
                else:
                    sprint = random.sample(sprint_list, min(3, len(sprint_list)))

                filter.update({filter_type: sprint})
            elif filter_type == "projects":
                projects = self.env_info['project_names']
                if len(projects) != 0:
                    filter.update({"projects": projects})
                else:
                    filter_update = self.generic.max_3_values_filter_type(filter_type, int_ids=integration_id)
                    filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(filter_type)
                    filter.update({filter_update[0][0]: filter_update[0][1]})
            else:
                filter_update = self.generic.max_3_values_filter_type(filter_type, int_ids=integration_id)
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(filter_type)
                filter.update({filter_update[0][0]: filter_update[0][1]})

        if creep_buffer:
            filter.update({"creep_buffer": int(creep_buffer)})

        if filter2:
            projects = self.env_info['project_names']
            if len(projects) == 0:
                filter_update = self.generic.max_3_values_filter_type(filter2, int_ids=integration_id)
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(filter2)
                filter.update({filter_update[0][0]: filter_update[0][1]})
            else:
                filter.update({"projects": projects})

        if exclude:
            if exclude == "sprint_report":
                sprint_list = self.sprint_report_filter_value(gt, lt, integration_id)
                if len(sprint_list) == 0:
                    sprint = sprint_list
                else:
                    sprint = random.sample(sprint_list, min(3, len(sprint_list)))
                filter.update({"exclude": {"sprint_report": sprint}})
            else:
                filter_update = self.generic.max_3_values_filter_type(exclude, int_ids=integration_id)
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(exclude)
                filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})

        if additional_done_status is not None:
            filter.update({"additional_done_statuses": additional_done_status})

        return filter

    def sprint_metrics_trend_payload_generation(self, filter, ou_id, aggregation=None):

        payload = {"filter": filter,
                   "ou_ids": ou_id,
                   "ou_user_filter_designation": {"sprint": ["sprint_report"]},
                   "page": 0, "page_size": 1000
                   }

        if aggregation is not None:
            payload["across"] = aggregation
            payload["interval"] = "week"

        LOG.info("payload-----{}".format(json.dumps(payload)))

        return payload

    def get_sprint_metric_response(self, payload):
        url = self.generic.connection['base_url'] + self.api_info['sprint_metrics_report_jira']
        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def sprint_report_filter_value(self, gt, lt, int_ids):
        sprint_name = []
        url = self.generic.connection["base_url"] + self.generic.api_data["sprint_goal_report"]
        payload = {"filter": {"integration_ids": int_ids,
                              "completed_at": {"$gt": gt, "$lt": lt}}, "page": 0, "page_size": 1000}

        res = self.generic.execute_api_call(url, "post", data=payload)
        if res['count'] == 0:
            return sprint_name
        else:
            for i in range(0, len(res['records'])):
                sprint_name.append(res['records'][i]['name'])

        return sprint_name

    def widget_creation_sprint_metrics_trend_report(self, filter, creep_buffer, filter2, int_ids, gt, lt, custom_value,
                                                    exclude, get_3_additional_done_statuses_values, ou_id,
                                                    aggregation=None,
                                                    metric=None, sprint_mandate_date=None):
        res = []
        flag_list = []
        try:
            # breakpoint()
            filter = self.filter_creation(filter_type=filter, creep_buffer=creep_buffer,
                                          filter2=filter2,
                                          integration_id=int_ids, gt=gt, lt=lt,
                                          custom_values=custom_value,
                                          exclude=exclude,
                                          additional_done_status=get_3_additional_done_statuses_values,
                                          sprint_mandate_date=sprint_mandate_date
                                          )

            payload = self.sprint_metrics_trend_payload_generation(filter=filter, ou_id=[ou_id],
                                                                   aggregation=aggregation)
            res = self.get_sprint_metric_response(payload)
            if len(res['records']) == 0:
                LOG.info("NO records for the params passed-----")
                pytest.skip("{'records': 0} therefore we skipped the test")
                # flag_list.append({"records": 0})
            if len(res['records']) != 0:
                if "test_sprint_metric_widget_single_stat_drilldown_comparison_jira" == str(inspect.stack()[1][3]):
                    tc_name = "test_sprint_metric_widget_single_stat_drilldown_comparison_jira"
                    # breakpoint()
                    for i in range(0, len(res['records'])):
                        drilldown = self.drill_down_verification_sprint_trend_report(payload, res['records'][i][
                            'additional_key'], aggregation=metric, tc_name=tc_name)
                        if res['records'] != drilldown['records']:
                            flag_list.append({"widget_data": res['records'],
                                              "drilldown_data": drilldown['records']})
                else:
                    if payload['across'] == "sprint" and len(res['records']) != 0:
                        for i in range(0, len(res['records'])):
                            drilldown = self.drill_down_verification_sprint_trend_report(payload, res['records'][i][
                                'additional_key'])
                            if res['records'][i]['total_issues'] != drilldown['records'][0]['total_issues']:
                                flag_list.append({"widget_count": res['records'][i]['total_issues'],
                                                  "drilldown_count": drilldown['records'][0]['total_issues']})
                    if payload['across'] == "month" and len(res['records']) != 0:
                        # get all months start date between the given complted at dates
                        month_start_dates = self.widgetresuable.get_all_month_start_date_between_2_dates(
                            start_epoch=int(gt),
                            end_epoch=int(lt))
                        for i in month_start_dates:
                            start_gt = i
                            end_lt = self.widgetresuable.get_last_day_of_month(start_gt)
                            drilldown = self.drill_down_verification_sprint_trend_report(payload, start_gt=start_gt,
                                                                                         end_lt=end_lt)
                            # using jira sprint list check for the appropriate sprint that lies between the given completed date
                            sprint_list = self.get_sprint_list_for_completed_at(gt=start_gt, lt=end_lt, int_ids=int_ids,
                                                                                ou_id=[ou_id])
                            if len(sprint_list) == 0 and len(drilldown['records']) != 0:
                                LOG.info(
                                    f"sprint_list returned by sprint list jira end point for the given gt--{gt},lt---{lt} is null")
                                flag_list.append({
                                    "data": f"drilldown is not matching with the sprint list end point result sprintresult--{len(sprint_list)} and drilldown result---{len(drilldown['records'])}"})

                            elif len(sprint_list) != 0 and len(drilldown['records']) != 0:
                                for i in range(0, len(drilldown['records'])):
                                    if drilldown['records'][i]['additional_key'] not in sprint_list:
                                        flag_list.append({
                                            f"sprint present in drilldown--{drilldown['records'][i]['additional_key']} not present in sprint list--{sprint_list}"})

                            elif len(sprint_list) == 0 and len(drilldown['records']) == 0:
                                LOG.info(
                                    f"-----both sprint list api and drilldown count is zero for the given completed at gt {start_gt},lt {end_lt}---------f")

                    if payload['across'] == "week" and len(res['records']) != 0:
                        week_start_dates = self.widgetresuable.get_week_start_dates(start_epoch=int(gt),
                                                                                    end_epoch=int(lt))
                        for i in week_start_dates:
                            start_gt = i
                            end_lt = self.widgetresuable.get_week_end_date(epoch_time=start_gt)
                            drilldown = self.drill_down_verification_sprint_trend_report(payload, start_gt=start_gt,
                                                                                         end_lt=end_lt)
                            # using jira sprint list check for the appropriate sprint that lies between the given completed date
                            sprint_list = self.get_sprint_list_for_completed_at(gt=start_gt, lt=end_lt, int_ids=int_ids,
                                                                                ou_id=[ou_id])
                            if len(sprint_list) == 0 and len(drilldown['records']) != 0:
                                LOG.info(
                                    f"sprint_list returned by sprint list jira end point for the given gt--{gt},lt---{lt} is null")
                                flag_list.append({
                                    "data": f"drilldown is not matching with the sprint list end point result sprintresult--{len(sprint_list)} and drilldown result---{len(drilldown['records'])}"})

                            elif len(sprint_list) != 0 and len(drilldown['records']) != 0:
                                for i in range(0, len(drilldown['records'])):
                                    if drilldown['records'][i]['additional_key'] not in sprint_list:
                                        flag_list.append({
                                            f"sprint present in drilldown--{drilldown['records'][i]['additional_key']} not present in sprint list--{sprint_list}"})

                            elif len(sprint_list) == 0 and len(drilldown['records']) == 0:
                                LOG.info(
                                    f"-----both sprint list api and drilldown count is zero for the given completed at gt {start_gt},lt {end_lt}---------f")


        except Exception as ex:
            LOG.info("Exception while creating the widget------{}".format(ex))
            flag_list.append({"exception_caught": ex})

        return flag_list, json.dumps(payload), res['records']

    def drill_down_verification_sprint_trend_report(self, payload, res=None, start_gt=None, end_lt=None,
                                                    aggregation=None, tc_name=None):

        payload_drilldown = deepcopy(payload)
        # breakpoint()
        payload_drilldown['filter']['include_total_count'] = True
        payload_drilldown['filter']['include_workitem_ids'] = True

        if "test_sprint_metric_widget_single_stat_drilldown_comparison_jira" == tc_name:
            payload_drilldown['filter']["metric"] = aggregation

        else:
            del payload_drilldown['filter']['metric']

            if payload['across'] == "sprint":
                payload_drilldown['filter']['sprint_report'] = [res]
                payload_drilldown["ou_exclusions"] = ["sprint"]
            elif payload['across'] == "month" or payload['across'] == "week":
                payload_drilldown['filter']['completed_at'] = {"$gt": str(start_gt), "$lt": str(end_lt)}

        LOG.info(f"drilldown_payload-----{payload_drilldown}")
        drilldown_response = self.get_sprint_metric_response(payload_drilldown)

        return drilldown_response

    def get_sprint_list_for_completed_at(self, gt, lt, int_ids, ou_id):
        sprint_list = []
        url = base_url = self.generic.connection['base_url'] + "jira_issues/sprints/list"
        payload = {"filter": {"completed_at": {"$gt": str(gt), "$lt": str(lt)},
                              "integration_ids": int_ids}, "ou_ids": ou_id,
                   "ou_user_filter_designation": {"sprint": ["sprint_report"]}}

        response = self.generic.execute_api_call(url, "post", data=payload)
        if len(response['records']) != 0:
            for i in range(0, len(response['records'])):
                sprint_list.append(response['records'][i]["name"])

        return sprint_list
