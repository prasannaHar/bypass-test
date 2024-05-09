import inspect
import json
import logging
import random
from copy import deepcopy
import pandas as pd
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.api_reusable_functions import ApiReusableFunctions
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReportHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.api_reusable_funct = ApiReusableFunctions(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.widgetresuable = WidgetReusable(generic_helper)

    def issues_report(self, metric="ticket", across="assignee", across_limit=20, sort_id="ticket_count", stacks=False,
                      story=False,
                      var_filters=False,
                      keys=False):
        """ create issue backlog report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        project_names = self.env_info["project_names"]
        integration_id = self.generic.integration_ids_basedon_workspace()
        sort = [{"id": sort_id, "desc": True}]
        filters = {"metric": metric, "sort_xaxis": "value_high-low", "visualization": "bar_chart",
                   "integration_ids": integration_id, "projects": project_names}
        if stacks:
            filters.update({"stacks": [stacks]})
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issues_report(ou_ids=org_id, filters=filters, across_limit=across_limit,
                                                across=across, sort=sort, story=story)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            if keys:
                keys_id = {}
                for key in resp["records"]:
                    if story == False and key["additional_key"] != "_UNASSIGNED_":
                        keys_id[key["key"]] = key["total_tickets"]
                return keys_id
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def issues_report_drilldown(self, key, key_option="assignees", metric="ticket", across="assignee",
                                var_filters=False, sort_id="bounces", stacks=False, ):
        """get drilldown of each key detatils"""

        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        sort = [{"id": sort_id, "desc": True}]
        project_names = self.env_info["project_names"]

        filters = {"metric": metric, "visualization": "bar_chart", "integration_ids": integration_id,
                   key_option: [key], "projects": project_names}
        if stacks:
            filters.update({"stacks": [stacks]})
        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_list(filters, ou_ids=org_id, across=across, ou_exclusions=key_option,
                                                      sort=sort)
        return resp_assign

    def filter_creation(self, filter_type, filter2, integration_id, gt, lt, custom_values, dependency_analysis,
                        sort_x_axis="label_high-low",
                        exclude=None, metric="ticket", datetime_filters=None, sprint=None):
        """creating jira/azure filter object for payload with different filter_type"""
        # jira
        """{"filter":{"metric":"ticket","sort_xaxis":"value_high-low",
        "visualization":"bar_chart","integration_ids":["229","231"]},
        """
        # breakpoint()
        filter = {"integration_ids": integration_id, "visualization": "bar_chart"}

        if sprint:
            if sprint == 'ACTIVE':
                filter.update({"sprint_states": ["ACTIVE"]})
            else:
                filter.update({"last_sprint": True})

        if metric:
            filter.update({"metric": metric})

        if datetime_filters:
            filter.update({datetime_filters: {"$gt": gt, "$lt": lt}})

        if sort_x_axis:
            filter.update({"sort_xaxis": sort_x_axis})

        if filter_type:
            if "custom" in filter_type:
                filter.update({"custom_fields": {filter_type: custom_values}})

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

        if filter2:
            projects = self.env_info['project_names']
            if len(projects) == 0:
                filter_update = self.generic.max_3_values_filter_type(filter2, int_ids=integration_id)
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(filter2)
                filter.update({filter_update[0][0]: filter_update[0][1]})
            else:
                filter.update({"projects": projects})

        if exclude:
            filter_update = self.generic.max_3_values_filter_type(exclude, int_ids=integration_id, exclude=True)
            if len(filter_update) == 0:
                pass
            else:
                filter_update[0][0] = self.generic.get_jira_field_based_on_filter_type(exclude)
                filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})

        if dependency_analysis:
            filter.update({"links": [dependency_analysis]})

        return filter

    def issue_report_payload_generation(self, filter, ou_id, sprint_field, across,
                                        across_limit=20, interval=None, stacks=None, metric=None):
        """{"filter":{"metric":"ticket","sort_xaxis":"value_high-low",
        "visualization":"bar_chart","integration_ids":["229","231"]},
        "across":"assignee","stacks":["assignee"],
        "across_limit":20,"sort":[{"id":"ticket_count","desc":true}],
        "ou_ids":["2575"],
        "ou_user_filter_designation":{"sprint":["customfield_10020"]}"""
        # breakpoint()

        payload = {"filter": filter,
                   "ou_ids": [ou_id],
                   "ou_user_filter_designation": {"sprint": [sprint_field]},
                   "across_limit": across_limit,
                   "sort": [{"id": across, "desc": True}],
                   "across": across
                   }

        if across in ["issue_due", "issue_updated", "issue_resolved", "issue_created"]:
            payload.update({"interval": interval})
        else:
            if metric == "story_point":
                payload.update({"sort": [{"id": "story_points", "desc": True}]})
            else:
                payload.update({"sort": [{"id": "ticket_count", "desc": True}]})

        if "custom" in across:
            payload.update({"sort": [{"id": "custom_field", "desc": True}]})

        if stacks:
            if "custom" in stacks:
                payload.update({"stacks": ["custom_field"]})
                payload['filter'].update({"custom_stacks": [stacks]})
            else:
                payload.update({"stacks": [stacks]})

        LOG.info("payload-----{}".format(json.dumps(payload)))

        return payload

    def get_issues_report_response(self, payload, metric="ticket"):
        url = self.generic.connection['base_url'] + self.api_info['jira_tickets_report'] + "?there_is_no_cache=true"
        if metric == "story_point":
            url = self.generic.connection['base_url'] + self.api_info[
                'jira_story_point_report'] + "?there_is_no_cache=true"
        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def get_issue_report_drilldown_response(self, drilldown_payload):
        url = self.generic.connection['base_url'] + self.api_info['drill_down_api_url'] + "?there_is_no_cache=true"
        response = self.generic.execute_api_call(url, "post", data=drilldown_payload)
        return response

    def widget_creation_issues_report(self, filter, filter2, int_ids, gt, lt, custom_values, stacks, interval,
                                      exclude, ou_id, metric, datetime_filters, sprint, sort_x_axis, sprint_field,
                                      dependency_analysis=None,
                                      across=None, sort=None, across_limit=20):

        flag_list = []
        # breakpoint()
        try:

            filter = self.filter_creation(filter_type=filter, filter2=filter2, integration_id=int_ids, gt=gt, lt=lt,
                                          custom_values=custom_values, sort_x_axis=sort_x_axis,
                                          exclude=exclude, metric=metric, datetime_filters=datetime_filters,
                                          sprint=sprint, dependency_analysis=dependency_analysis
                                          )

            payload = self.issue_report_payload_generation(filter=filter, ou_id=ou_id, sprint_field=sprint_field,
                                                           across=across,
                                                           across_limit=across_limit, interval=interval, stacks=stacks,
                                                           metric=metric)

            LOG.info("Widget Payload------{}".format(json.dumps(payload)))
            # breakpoint()
            res = self.get_issues_report_response(payload, metric=metric)
            # LOG.info("widget_response-----{}".format(json.dumps(res)))

            if len(res['records']) == 0:
                pytest.skip(f"Record count is zero---res['records']= {res['records']}")
            issue_widget_response = res['records']
            widget_response_df = pd.json_normalize(issue_widget_response)
            drilldown_filter_key = self.get_key_value_pair_for_drilldown_filter(issue_widget_response)
            drilldown_filter_key = sorted(drilldown_filter_key, key=lambda x: x["key"], reverse=True)
            # check for 5 random drill down to widget count
            if interval:
                start_time = drilldown_filter_key[-1]['key']
                if interval == "month":
                    if gt != None and lt != None:
                        month_start_dates = self.widgetresuable.get_all_month_start_date_between_2_dates(
                            start_epoch=int(gt),
                            end_epoch=int(lt))

                        if len(drilldown_filter_key) != 0:
                            drilldown_filter_key[-1]['key'] = str(gt)

                        if len(drilldown_filter_key) == 1:
                            # drilldown_filter_key[0]['key'] = str(gt)
                            month_start_dates_updated = [{"key": str(gt)}]
                        else:
                            month_start_dates_updated = random.sample(drilldown_filter_key,
                                                                      min(3, len(month_start_dates)))
                    else:
                        month_start_dates_updated = []
                        for j in range(0, len(drilldown_filter_key)):
                            month_start_dates_updated.append({"key": drilldown_filter_key[j]['key']})
                    # month_start_dates.append(initial_date)

                    for i in month_start_dates_updated:
                        start_gt = i['key']
                        end_lt = self.widgetresuable.get_last_day_of_month(int(start_gt))
                        # for j in range(0, len(drilldown_filter_key)):
                        drilldown_response, payload_drilldown = self.drill_down_verification_issues_report(
                            payload=payload,
                            start_gt=start_gt,
                            end_lt=str(end_lt),
                            interval=interval, dependency_analysis=dependency_analysis)
                        LOG.info(f"drilldown_payload---{payload_drilldown}")
                        if drilldown_response is not None and len(drilldown_response['records']) != 0:

                            if start_gt in widget_response_df['key'].values:
                                location = widget_response_df.loc[widget_response_df['key'] == start_gt].index[0]
                            else:
                                location = widget_response_df.loc[widget_response_df['key'] == start_time].index[0]
                            total_tickets = widget_response_df.loc[location, 'total_tickets']
                            if str(drilldown_response['_metadata']['total_count']) != str(total_tickets):
                                flag_list.append(
                                    {"drilldown_count": drilldown_response['_metadata']['total_count'],
                                     "widget_count": total_tickets, "key": start_gt,
                                     "payload_drilldown": payload_drilldown})
                        else:
                            flag_list.append({"drilldown": drilldown_response,"widget_count": issue_widget_response})

                if interval == "week":
                    # breakpoint()
                    week_start_dates = random.sample(drilldown_filter_key, min(3, len(drilldown_filter_key)))
                    week_start_dates = sorted(week_start_dates, key=lambda x: x["key"], reverse=True)
                    for i in week_start_dates:
                        start_gt = i['key']
                        end_lt = self.widgetresuable.get_week_end_date(int(start_gt))
                        if gt != None and lt != None:
                            if i['key'] == week_start_dates[-1]['key']:
                                if int(start_gt) < int(gt):
                                    locations = widget_response_df.index[widget_response_df['key'] == str(start_gt)]
                                    widget_response_df.loc[locations, 'key'] = str(gt)
                                    start_gt = gt
                            if i['key'] == week_start_dates[0]['key']:
                                if int(end_lt) > int(lt):
                                    end_lt = lt
                        # breakpoint()
                        drilldown_response, payload_drilldown = self.drill_down_verification_issues_report(
                            payload=payload,
                            start_gt=str(start_gt),
                            end_lt=str(end_lt),
                            interval=interval, dependency_analysis=dependency_analysis)
                        LOG.info(f"drilldown_payload---{payload_drilldown},widget_count: {res}")
                        if drilldown_response is not None and len(drilldown_response['records']) != 0:
                            location = widget_response_df.loc[widget_response_df['key'] == start_gt].index[0]
                            total_tickets = widget_response_df.loc[location, 'total_tickets']
                            if str(drilldown_response['_metadata']['total_count']) != str(total_tickets):
                                flag_list.append(
                                    {"drilldown_count": drilldown_response['_metadata']['total_count'],
                                     "widget_count": total_tickets, "key": start_gt,
                                     "payload_drilldown": payload_drilldown})
                        else:
                            flag_list.append({"drilldown": drilldown_response, "widget_count": issue_widget_response})

            else:
                # breakpoint()
                for i in drilldown_filter_key:
                    drilldown_response, payload_drilldown = self.drill_down_verification_issues_report(
                        payload=payload,
                        key=i["key"],
                        interval=interval, dependency_analysis=dependency_analysis)
                    LOG.info(f"drilldown_payload---{payload_drilldown}")

                    if drilldown_response is not None and len(drilldown_response['records']) != 0:
                        widget_response_df = pd.json_normalize(issue_widget_response)
                        widget_response_df = widget_response_df.fillna("")
                        location = widget_response_df.loc[widget_response_df['key'] == i["key"]].index[0]
                        total_tickets = widget_response_df.loc[location, 'total_tickets']
                        if str(drilldown_response['_metadata']['total_count']) != str(total_tickets):
                            flag_list.append({"drilldown_count": drilldown_response['_metadata']['total_count'],
                                              "widget_count": total_tickets, "key": i['key'],
                                              "widget_payload": payload, "payload_drilldown": payload_drilldown,"widget_count": total_tickets})

                    else:
                        flag_list.append(
                            {"msg": "drilldown response is None", "drilldown_response": drilldown_response,
                             "widget_response": res['records'], "drilldown_payload": payload_drilldown})

        except Exception as ex:
            LOG.info("Exception caught in widget_creation_issues_report function----{}".format(ex))
            flag_list.append({"exception": ex})

        return flag_list, json.dumps(payload)

    def drill_down_verification_issues_report(self, payload, key=None, start_gt=None, end_lt=None, interval=None,
                                              dependency_analysis=None):
        try:
            # breakpoint()
            payload_drilldown = deepcopy(payload)
            del payload_drilldown['filter']['sort_xaxis']
            del payload_drilldown['filter']["visualization"]
            del payload_drilldown['across_limit']

            if "stacks" in list(payload_drilldown.keys()):
                if "custom_field" in payload_drilldown['stacks']:
                    del payload_drilldown['stacks']
                    payload_drilldown['filter']['stacks'] = payload['filter']['custom_stacks']
                else:
                    payload_drilldown['filter']['stacks'] = payload_drilldown['stacks']
                    del payload_drilldown['stacks']
            payload_drilldown.update({"page": 0, "page_size": 10, "sort": [{"id": "bounces", "desc": True}]})
            if interval:
                if payload['across'] == "issue_due":
                    # breakpoint()
                    if dependency_analysis:
                        payload_drilldown['filter'].update(
                            {"linked_" + payload['across'] + "_at": {"$gte": start_gt, "$lte": end_lt}})
                        payload_drilldown['filter'].update(
                            {payload['across'] + "_at": {"$gte": start_gt, "$lte": end_lt}})

                    else:
                        payload_drilldown['filter'].update(
                            {payload['across'] + "_at": {"$gte": start_gt, "$lte": end_lt}})
                else:
                    if dependency_analysis:
                        # payload_drilldown['filter'].update(
                        #     {payload['across'] + "_at": {"$gt": start_gt, "$lt": end_lt}})
                        payload_drilldown['filter'].update(
                            {"linked_" + payload['across'] + "_at": {"$gt": start_gt, "$lt": end_lt}})
                    else:
                        payload_drilldown['filter'].update(
                            {payload['across'] + "_at": {"$gt": start_gt, "$lt": end_lt}})
                del payload_drilldown['interval']
            if payload["across"] in ["status", "sprint", "resolution", "epic", "project", "status_category", "assignee",
                                     "component", "version"]:
                if payload["across"] == "sprint":
                    payload_drilldown['filter'].update(
                        {"custom_fields": {payload["ou_user_filter_designation"]['sprint'][0]: [key]}})
                    # del payload_drilldown['filter']['sprints']
                    payload_drilldown.update({"ou_exclusions": [payload["ou_user_filter_designation"]['sprint'][0]]})
                    payload_drilldown.update({"across": payload["ou_user_filter_designation"]['sprint'][0]})

                elif payload['across'] == "status":
                    if dependency_analysis:
                        payload_drilldown['filter'].update({"linked_statuses": [key]})
                    else:
                        payload_drilldown['filter'].update({"statuses": [key]})
                        payload_drilldown.update({"ou_exclusions": ["statuses"]})

                elif payload['across'] == "status_category":
                    if dependency_analysis:
                        payload_drilldown['filter'].update({"linked_status_categories": [key]})
                    else:
                        payload_drilldown['filter'].update({"status_categories": [key]})
                        payload_drilldown.update({"ou_exclusions": ["status_categories"]})
                else:
                    if dependency_analysis:
                        if payload['across'] == "assignee" and key == "":
                            payload_drilldown['filter'].update({"missing_fields": {"assignee": True}})
                        else:
                            payload_drilldown['filter'].update({"linked_" + payload['across'] + 's': [key]})

                    else:
                        if payload['across'] == "assignee" and key == "":
                            payload_drilldown['filter'].update({"missing_fields": {"assignee": True}})
                        else:
                            payload_drilldown['filter'].update({payload['across'] + 's': [key]})
                        payload_drilldown.update({"ou_exclusions": [payload['across'] + 's']})

            elif "customfield" in payload['across']:
                payload_drilldown['filter'].update({"custom_fields": {payload['across']: [key]}})
                if not dependency_analysis:
                    payload_drilldown.update({"ou_exclusions": [payload['across']]})

            LOG.info("drilldown----payload-------{}".format(json.dumps(payload_drilldown)))
            drilldown_response = self.get_issue_report_drilldown_response(payload_drilldown)

        except Exception as ex:
            LOG.info("Exception occured ----{}".format(ex))
            return None, payload_drilldown

        return drilldown_response, payload_drilldown

    def get_key_value_pair_for_drilldown_filter(self, issue_widget_response):
        key_value_pair = []

        if len(issue_widget_response) != 0:
            for i in range(0, len(issue_widget_response)):
                if "key" in list(issue_widget_response[i].keys()):
                    key_value_pair.append({"key": issue_widget_response[i]['key'],
                                           "total_tickets": issue_widget_response[i]['total_tickets'],
                                           "story_points": issue_widget_response[i]['total_story_points']})

                else:
                    key_value_pair.append(({"key": "", "additional_key": issue_widget_response[i]["additional_key"],
                                            "total_tickets": issue_widget_response[i]['total_tickets'],
                                            "story_points": issue_widget_response[i]['total_story_points']}))

        return key_value_pair

    def issues_report_creation(self, filter, filter2, int_ids, gt, lt, custom_values, stacks, interval,
                               exclude, ou_id, metric, datetime_filters, sprint, sort_x_axis, sprint_field,
                               dependency_analysis,
                               across=None, across_limit=20):
        res = None
        try:

            filter = self.filter_creation(filter_type=filter, filter2=filter2, integration_id=int_ids, gt=gt, lt=lt,
                                          custom_values=custom_values, sort_x_axis=sort_x_axis,
                                          exclude=exclude, metric=metric, datetime_filters=datetime_filters,
                                          sprint=sprint, dependency_analysis=dependency_analysis
                                          )

            payload = self.issue_report_payload_generation(filter=filter, ou_id=ou_id, sprint_field=sprint_field,
                                                           across=across,
                                                           across_limit=across_limit, interval=interval, stacks=stacks,
                                                           metric=metric)

            LOG.info("Widget Payload------{}".format(json.dumps(payload)))
            # breakpoint()
            res = self.get_issues_report_response(payload, metric=metric)
            # LOG.info("widget_response-----{}".format(json.dumps(res)))

            if len(res['records']) == 0:
                pytest.skip(f"Record count is zero---res['records']= {res['records']}")

            return res

        except Exception as ex:
            LOG.info(f"Exception in issues_report_creation---{ex}")
        return res
