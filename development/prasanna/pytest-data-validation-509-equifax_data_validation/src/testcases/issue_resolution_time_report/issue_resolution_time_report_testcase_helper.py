import json
import logging
import random
from collections import OrderedDict
from copy import deepcopy
import pandas as pd
import pytest

from src.lib.widget_details.widget_helper import TestWidgetHelper
from src.utils.jira_report_helper import TestJiraReportHelper
from src.utils.api_reusable_functions import ApiReusableFunctions
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueResolutionTimetHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.jira_helper = TestJiraReportHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.api_reusable_funct = ApiReusableFunctions(generic_helper)
        self.widgetresuable = WidgetReusable(generic_helper)

    def issue_resolution_time_report(self, issue_resolve, across="assignee", across_limit=20,
                                     metric=["median_resolution_time", "number_of_tickets_closed"],
                                     var_filters=False, sort_xaxis="value_high-low",
                                     sort=[{"id": "resolution_time", "desc": True}],
                                     ou_user_filter_designation=None, additional_key=True,
                                     keys=False, arg_ou_id=None,
                                     required_item="total_tickets"):  ## -- keys = True, issue_resolved
        """ create issue  issue_resolution_time_report"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        if arg_ou_id:
            org_id = arg_ou_id
        integration_id = self.generic.integration_ids_basedon_workspace()
        filters = {"issue_resolved_at": issue_resolve, "metric": metric,
                   "sort_xaxis": sort_xaxis,
                   "integration_ids": integration_id}

        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_resolution_time_report(ou_ids=org_id, filters=filters, across=across,
                                                               across_limit=across_limit, sort=sort,
                                                               ou_user_filter_designation=ou_user_filter_designation)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            if keys:
                keys_id = {}
                for key in resp["records"]:
                    if additional_key:
                        if key["additional_key"] != "_UNASSIGNED_":
                            keys_id[key["key"]] = key[required_item]
                    else:
                        keys_id[key["key"]] = key[required_item]
                return keys_id
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def issue_resolution_time_report_drilldown(self, issue_resolve, key=None, key_option="assignees", across="assignee",
                                               var_filters=False, ou_user_filter_designation=None,
                                               metric=["median_resolution_time", "number_of_tickets_closed"],
                                               arg_ou_id=None):
        """get drilldown of each key detatils"""

        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        if arg_ou_id:
            org_id = arg_ou_id

        integration_id = self.generic.integration_ids_basedon_workspace()
        sort = [{"id": "bounces", "desc": True}]
        filters = {"include_solve_time": True, "integration_ids": integration_id,
                   "issue_resolved_at": issue_resolve,
                   "metric": metric}
        if key:
            filters[key_option] = [key]

        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_list(filters, ou_ids=org_id, across=across,
                                                      ou_exclusions=key_option, sort=sort,
                                                      ou_user_filter_designation=ou_user_filter_designation
                                                      )
        return resp_assign

    def issue_resolution_time_report_payload_generation(self, filter, ou_id, sprint_field, across,
                                                        across_limit=20, interval=None, stacks=None, sort_xaxis=None):
        """{"filter":{"metric":["median_resolution_time","number_of_tickets_closed"],"sort_xaxis":"value_high-low",
        "issue_resolved_at":{"$gt":"1683504000","$lt":"1685577599"},
        "integration_ids":["13","14"]},"across":"component","across_limit":20,"sort":[{"id":"resolution_time","desc":true}],"ou_ids":["140"],
        "ou_user_filter_designation":{"sprint":["customfield_10103"]}}"""
        # breakpoint()

        payload = {"filter": filter,
                   "ou_ids": [ou_id],
                   "ou_user_filter_designation": {"sprint": [sprint_field]},
                   "across_limit": across_limit,
                   "sort": [{"id": "resolution_time", "desc": True}],
                   "across": across
                   }

        if interval:
            payload.update({"interval": interval})
        if stacks:
            if "custom" in stacks:
                payload.update({"stacks": ["custom_field"]})
                payload['filter'].update({"custom_stacks": [stacks]})
            else:
                payload.update({"stacks": [stacks]})

        if sort_xaxis:
            if "low-high" in sort_xaxis:
                desc = False
            elif "high-low" in sort_xaxis:
                desc = True
            else:
                desc = True

            payload['sort'] = [{"id": "resolution_time", "desc": desc}]

        LOG.info("payload-----{}".format(json.dumps(payload)))

        return payload

    def get_resolution_time_report_response(self, payload):
        url = self.generic.connection['base_url'] + self.api_info['resolution_time_report']
        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def get_resolution_time_report_drilldown_response(self, drilldown_payload):
        url = self.generic.connection['base_url'] + self.api_info['drill_down_api_url']
        response = self.generic.execute_api_call(url, "post", data=drilldown_payload)
        return response

    def widget_creation_issues_resolution_time_report(self, filter, filter2, int_ids, gt, lt, custom_values, interval,
                                                      exclude, ou_id, metric, datetime_filters, sprint, sort_x_axis,
                                                      sprint_field,
                                                      across=None, sort=None, across_limit=20):
        flag_list = []
        try:

            filter = self.jira_helper.jira_filter_creation(filter_type=filter, filter2=filter2, integration_id=int_ids,
                                                           gt=gt, lt=lt,
                                                           custom_values=custom_values, sort_x_axis=sort_x_axis,
                                                           exclude=exclude, metric=metric,
                                                           datetime_filters=datetime_filters,
                                                           sprint=sprint
                                                           )

            payload = self.issue_resolution_time_report_payload_generation(filter=filter, ou_id=ou_id,
                                                                           sprint_field=sprint_field,
                                                                           across=across,
                                                                           across_limit=across_limit, interval=interval)

            LOG.info("Widget Payload------{}".format(json.dumps(payload)))
            # breakpoint()
            res = self.get_resolution_time_report_response(payload)
            # LOG.info("widget_response-----{}".format(json.dumps(res)))

            if len(res['records']) == 0:
                pytest.skip(f"Skipping test as there length of the len(res['records']) is ---{len(res['records'])}")
                flag_list.append({"records": 0})
            if len(res['records']) != 0:
                if not res['count'] <= across_limit:
                    flag_list.append({"count": res['count'], "across_limit": across_limit,
                                      "message": "across_limit and count are not matching"})
                issue_widget_response = res['records']
                widget_response_df = pd.json_normalize(issue_widget_response)

                drilldown_filter_key = self.get_key_value_pair_for_drilldown_filter(issue_widget_response)
                # check for 5 random drill down to widget count
                if interval:
                    # breakpoint()
                    if interval == "month":
                        month_start_dates = self.widgetresuable.get_all_month_start_date_between_2_dates(
                            start_epoch=int(gt),
                            end_epoch=int(lt))
                        month_start_dates = random.sample(drilldown_filter_key, min(3, len(month_start_dates)))
                        month_start_dates = sorted(month_start_dates, key=lambda x: x["key"], reverse=True)

                        for i in month_start_dates:
                            start_gt = i['key']
                            end_lt = self.widgetresuable.get_last_day_of_month(int(start_gt))
                            if int(start_gt) < int(gt):
                                locations = widget_response_df.index[widget_response_df['key'] == str(start_gt)]
                                widget_response_df.loc[locations, 'key'] = str(gt)
                                start_gt = gt
                            if int(end_lt) > int(lt):
                                end_lt = lt
                            # for j in range(0, len(drilldown_filter_key)):
                            drilldown_response, payload_drilldown = self.drill_down_verification_issue_resolution_time_report(
                                payload=payload,
                                start_gt=start_gt,
                                end_lt=end_lt,
                                interval=interval)
                            if drilldown_response is not None and len(drilldown_response['records']) != 0:
                                location = widget_response_df.loc[widget_response_df['key'] == start_gt].index[0]
                                total_tickets = widget_response_df.loc[location, 'total_tickets']
                                if str(drilldown_response['_metadata']['total_count']) != str(total_tickets):
                                    flag_list.append(
                                        {"drilldown_count": drilldown_response['_metadata']['total_count'],
                                         "widget_count": total_tickets, "key": start_gt,
                                         "payload_drilldown": payload_drilldown})
                            else:
                                flag_list.append({"drilldown": drilldown_response})

                    if interval == "week":
                        # breakpoint()
                        week_start_dates = random.sample(drilldown_filter_key, min(3, len(drilldown_filter_key)))
                        week_start_dates = sorted(week_start_dates, key=lambda x: x["key"], reverse=True)
                        for i in week_start_dates:
                            start_gt = i['key']
                            end_lt = self.widgetresuable.get_week_end_date(int(start_gt), get_day_end_time=True)
                            if i['key'] == week_start_dates[-1]['key']:
                                if int(start_gt) < int(gt):
                                    locations = widget_response_df.index[widget_response_df['key'] == str(start_gt)]
                                    widget_response_df.loc[locations, 'key'] = str(gt)
                                    start_gt = gt
                            if i['key'] == week_start_dates[0]['key']:
                                if int(end_lt) > int(lt):
                                    end_lt = lt

                            # breakpoint()
                            drilldown_response, payload_drilldown = self.drill_down_verification_issue_resolution_time_report(
                                payload=payload,
                                start_gt=str(start_gt),
                                end_lt=str(end_lt),
                                interval=interval)
                            if drilldown_response is not None and len(drilldown_response['records']) != 0:
                                location = widget_response_df.loc[widget_response_df['key'] == start_gt].index[0]
                                total_tickets = widget_response_df.loc[location, 'total_tickets']
                                if str(drilldown_response['_metadata']['total_count']) != str(total_tickets):
                                    flag_list.append(
                                        {"drilldown_count": drilldown_response['_metadata']['total_count'],
                                         "widget_count": total_tickets, "key": start_gt,
                                         "payload_drilldown": payload_drilldown})
                            else:
                                flag_list.append({"drilldown": drilldown_response})

                else:
                    # breakpoint()
                    for i in drilldown_filter_key:
                        drilldown_response, payload_drilldown = self.drill_down_verification_issue_resolution_time_report(
                            payload=payload,
                            key=i["key"],
                            interval=interval)

                        if drilldown_response is not None and len(drilldown_response['records']) != 0:
                            widget_response_df = pd.json_normalize(issue_widget_response)
                            widget_response_df = widget_response_df.fillna("")
                            location = widget_response_df.loc[widget_response_df['key'] == i["key"]].index[0]
                            total_tickets = widget_response_df.loc[location, 'total_tickets']
                            if str(drilldown_response['_metadata']['total_count']) != str(total_tickets):
                                flag_list.append({"drilldown_count": drilldown_response['_metadata']['total_count'],
                                                  "widget_count": total_tickets, "key": i['key'],
                                                  "widget_payload": payload, "payload_drilldown": payload_drilldown})

                        else:
                            flag_list.append({"drilldown response is none"})

        except Exception as ex:
            LOG.info("Exception caught in widget_creation_issues_report function----{}".format(ex))
            flag_list.append({"exception": ex})

        return flag_list, json.dumps(payload)

    def drill_down_verification_issue_resolution_time_report(self, payload, key=None, start_gt=None, end_lt=None,
                                                             interval=None):
        # sample payload:
        """{"page":0,"page_size":10,"sort":[{"id":"bounces","desc":true}],
        "filter":{"metric":["median_resolution_time","number_of_tickets_closed"],
        "issue_resolved_at":{"$gt":"1683504000","$lt":"1685577599"},
        "integration_ids":["13","14"],"include_solve_time":true,"components":["Backend"]},
        "across":"component","ou_ids":["140"],"ou_user_filter_designation":{"sprint":["customfield_10103"]},
        "ou_exclusions":["components"]}"""

        try:

            payload_drilldown = deepcopy(payload)
            del payload_drilldown['filter']['sort_xaxis']
            payload_drilldown['filter'].update({"include_solve_time": True})
            payload_drilldown.update({"page": 0, "page_size": 10, "sort": [{"id": "bounces", "desc": True}]})
            if interval:
                payload_drilldown['filter'].update({payload['across'] + "_at": {"$gt": start_gt, "$lt": end_lt}})
                del payload_drilldown['interval']
            if payload["across"] in ["status", "sprint", "resolution", "epic", "project", "status_category", "assignee",
                                     "component", "version", "label"]:
                if payload["across"] == "sprint":
                    payload_drilldown['filter'].update(
                        {"custom_fields": {payload["ou_user_filter_designation"]['sprint'][0]: [key]}})
                    # del payload_drilldown['filter']['sprints']
                    payload_drilldown.update({"ou_exclusions": [payload["ou_user_filter_designation"]['sprint'][0]]})
                    payload_drilldown.update({"across": payload["ou_user_filter_designation"]['sprint'][0]})

                elif payload['across'] == "status":
                    payload_drilldown['filter'].update({"statuses": [key]})
                    payload_drilldown.update({"ou_exclusions": ["statuses"]})
                elif payload['across'] == "status_category":
                    payload_drilldown['filter'].update({"status_categories": [key]})
                    payload_drilldown.update({"ou_exclusions": ["status_categories"]})
                else:
                    if payload['across'] == "assignee" and key == "":
                        payload_drilldown['filter'].update({"missing_fields": {"assignee": True}})
                    else:
                        payload_drilldown['filter'].update({payload['across'] + 's': [key]})
                    payload_drilldown.update({"ou_exclusions": [payload['across'] + 's']})

            elif "customfield" in payload['across']:
                payload_drilldown['filter'].update({"custom_fields": {payload['across']: [key]}})
                payload_drilldown.update({"ou_exclusions": [payload['across']]})

            LOG.info("drilldown----payload-------{}".format(json.dumps(payload_drilldown)))
            drilldown_response = self.get_resolution_time_report_drilldown_response(payload_drilldown)

        except Exception as ex:
            LOG.info("Exception occured ----{}".format(ex))
            return None

        return drilldown_response, payload_drilldown

    def get_key_value_pair_for_drilldown_filter(self, issue_widget_response):
        key_value_pair = []

        if len(issue_widget_response) != 0:
            for i in range(0, len(issue_widget_response)):
                if "key" in list(issue_widget_response[i].keys()):
                    key_value_pair.append({"key": issue_widget_response[i]['key'],
                                           "total_tickets": issue_widget_response[i]['total_tickets']})

                else:
                    key_value_pair.append(({"key": "", "additional_key": issue_widget_response[i]["additional_key"],
                                            "total_tickets": issue_widget_response[i]['total_tickets']}))

        return key_value_pair
