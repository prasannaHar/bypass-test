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
from src.utils.jira_report_helper import TestJiraReportHelper
from src.utils.datetime_reusable_functions import DateTimeReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestEffortInvestmentReportHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.api_reusable_funct = ApiReusableFunctions(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.widgetresuable = WidgetReusable(generic_helper)
        self.jira_helper = TestJiraReportHelper(generic_helper)
        self.dt = DateTimeReusable()

    def effort_investment_filter_creation(self, filter_type, datetime_filters, filter2, exclude, sprint,
                                          ba_attribution_mode,
                                          ba_historical_assignees_statuses, ba_completed_work_statuses,
                                          int_ids, gt, lt,
                                          custom_values, ticket_categorization_scheme, ba_in_progress_statuses,
                                          ticket_categories, dashboard_filter
                                          ):
        # breakpoint()
        filter = self.jira_helper.jira_filter_creation(filter_type=filter_type, filter2=filter2, integration_id=int_ids,
                                                       gt=gt, lt=lt,
                                                       custom_values=custom_values, sort_x_axis=None, exclude=exclude,
                                                       metric=None,
                                                       datetime_filters=datetime_filters, sprint=sprint, ba=True,
                                                       dashboard_filter=dashboard_filter)

        filter["issue_resolved_at"] = {"$gt": gt, "$lt": lt}
        if ba_attribution_mode is not None:
            filter['ba_attribution_mode'] = ba_attribution_mode
        if ba_historical_assignees_statuses is not None:
            # breakpoint()
            ba_historical_assignees_statuses = self.get_status_field_values(integration_id=int_ids)
            filter["ba_historical_assignees_statuses"] = ba_historical_assignees_statuses
        if ba_completed_work_statuses is not None:
            ba_completed_work_statuses = self.get_status_field_values(integration_id=int_ids)
            filter["ba_completed_work_statuses"] = ba_completed_work_statuses
        if ticket_categorization_scheme is not None:
            filter["ticket_categorization_scheme"] = ticket_categorization_scheme
        if ticket_categories is not None:
            filter['ticket_categories'] = [ticket_categories]
        if ba_in_progress_statuses is not None:
            ba_in_progress_statuses = self.get_status_field_values(integration_id=int_ids)
            filter["ba_in_progress_statuses"] = ba_in_progress_statuses

        del filter["visualization"]

        return filter

    def effort_investment_payload_generation(self, filter, ou_id, sprint_field, across, interval=None):
        payload = {"filter": filter,
                   "ou_ids": [ou_id],
                   "ou_user_filter_designation": {"sprint": [sprint_field]},
                   "across": across
                   }
        if interval:
            payload["interval"] = interval

        return payload

    def get_status_field_values(self, integration_id):
        filter_type = "statuses"
        filter_update = self.generic.max_3_values_filter_type(filter_type, int_ids=integration_id)
        # filter.update({filter_update[0][0]: filter_update[0][1]})
        return filter_update[0][1]

    def get_effort_investment_response(self, payload, effort_unit, type_of="db"):
        if type_of == "db":
            type_of = "?there_is_no_cache=true&there_is_no_precalculate=true&force_source=db"
        elif type_of is None:
            type_of = "?there_is_no_cache=true&there_is_no_precalculate=true"
        else:
            type_of = "?there_is_no_cache=true&there_is_no_precalculate=true&force_source=es"
        if effort_unit == "story_point":
            url = self.generic.connection['base_url'] + "ba/jira/story_points_fte" + type_of
        elif effort_unit == "ticket_time":
            url = self.generic.connection['base_url'] + "ba/jira/ticket_time_fte" + type_of
        else:
            url = self.generic.connection['base_url'] + "ba/jira/ticket_count_fte" + type_of

        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def get_effort_investment_categories(self, effort_investment_id):
        categories = []
        url = self.generic.connection['base_url'] + "ticket_categorization_schemes/" + effort_investment_id
        response = self.generic.execute_api_call(url, "get")
        keys = list(response['config']["categories"].keys())
        for i in keys:
            categories.append(response['config']["categories"][i]['name'])

        return categories

    def get_active_work_details(self, active_work_unit, payload):
        if active_work_unit == "story_points":
            url = self.generic.connection['base_url'] + "ba/jira/active_work/story_points"
        else:
            url = self.generic.connection['base_url'] + "ba/jira/active_work/ticket_count"

        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def generate_payload_active_work(self, filter, interval, ou_id, across, across_limit=None):
        payload = {"across": across, "filter": filter,
                   "interval": interval, "ou_ids": [ou_id]}

        if across_limit:
            payload['across_limit']: across_limit

        return payload

    def generate_drilldown_response(self, effort_payload, page=0):
        payload = deepcopy(effort_payload)
        payload.update({"page": page, "page_size": 500})
        LOG.info(f"drill_down_payload----{json.dumps(payload)}")
        url = self.generic.connection['base_url'] + self.api_info['drill_down_api_url'] + "?there_is_no_cache=true"
        response = self.generic.execute_api_call(url, "post", data=payload)
        return response

    def effort_investment_trend_report_widget_creation(self, filter_type, filter2, int_ids, gt, lt, custom_value,
                                                       interval, exclude, ou_id, datetime_filters,
                                                       sprint, sprint_field, across, active_work_unit, effort_unit,
                                                       ba_attribution_mode, ba_historical_assignees_statuses,
                                                       ba_completed_work_statuses,
                                                       ticket_categorization_scheme, ba_in_progress_statuses,
                                                       dashboard_filter):

        flag = []

        categories = self.get_effort_investment_categories(ticket_categorization_scheme)
        categories.append("Other")
        df = pd.DataFrame()
        if len(categories) == 0:
            LOG.warning("NO category name found")
        keys = []
        filter = self.effort_investment_filter_creation(filter_type, datetime_filters, filter2, exclude, sprint,
                                                        ba_attribution_mode,
                                                        ba_historical_assignees_statuses,
                                                        ba_completed_work_statuses,
                                                        int_ids, gt, lt,
                                                        custom_value, ticket_categorization_scheme,
                                                        ba_in_progress_statuses, dashboard_filter=dashboard_filter,
                                                        ticket_categories=None)
        for i in categories:
            filter["ticket_categories"] = [i]
            payload = self.effort_investment_payload_generation(filter=filter, ou_id=ou_id, sprint_field=sprint_field,
                                                                across=across,
                                                                interval=interval)
            LOG.info(f"Payload to create the report----{json.dumps(payload)}")
            response = self.get_effort_investment_response(payload, effort_unit=effort_unit, type_of=None)

            if len(response['records']) != 0:
                for k in range(0, len(response['records'])):
                    if k == 0:
                        continue
                    else:
                        keys.append(response['records'][k]['key'])

                df1 = pd.json_normalize(response['records'], max_level=1)
                df1['ticket_categories'] = i

                df = df.append(df1)

        # breakpoint()
        keys = list(set(keys))
        if len(df) != 0:
            if effort_unit == "ticket_time":
                df['effort'] = round(df["effort"] / 86400).astype('int64')
                average_values = df.groupby('key')['effort'].sum().reset_index()

                # keys=['1685577600', '1682899200', '1680307200', '1677628800', '1688169600', '1685577600', '1682899200', '1680307200', '1677628800', '1675209600', '1688169600', '1685577600', '1682899200', '1680307200', '1677628800', '1675209600']
            for m in keys:
                for g in categories:

                    subset_df = df[df['key'] == m]
                    rl_df = subset_df["fte"]
                    total_sum = rl_df.sum()
                    individual_fte_value = subset_df['fte'][
                        (subset_df['key'] == m) & (subset_df['ticket_categories'] == g)]
                    # breakpoint()
                    if len(individual_fte_value) == 0:
                        percentage_of_each_cat = 0
                    else:
                        if total_sum == 0.0:
                            percentage_of_each_cat = 0
                        else:
                            percentage_of_each_cat = int(round((individual_fte_value / total_sum) * 100))

                    # breakpoint()

                    if interval == "month":
                        # breakpoint()
                        lt_cal = self.widgetresuable.get_last_day_of_month(int(gt))
                        lt_cal1 = self.widgetresuable.get_last_day_of_month(int(m))

                    if interval == "week":
                        lt_cal = self.widgetresuable.get_week_end_date(int(gt), get_day_end_time=True) + 19800
                        lt_cal1 = self.widgetresuable.get_week_end_date(int(m), get_day_end_time=True) + 19800

                    if interval == "biweekly":
                        start_gt, lt_cal = self.dt.get_biweekly_range(epoch_date=int(gt))
                        start_gt1, lt_cal1 = self.dt.get_biweekly_range(epoch_date=int(m))

                    if int(lt_cal) == int(lt_cal1):
                        gt1 = gt
                        lt1 = lt_cal1
                    else:
                        gt1 = m
                        lt1 = lt_cal1

                    drill_payload = deepcopy(payload)
                    # breakpoint()
                    drill_payload['filter'].update({"issue_resolved_at": {"$gt": str(gt1), "$lt": str(lt1)}})
                    del drill_payload['filter']['ticket_categories']
                    del drill_payload["interval"]
                    if ba_completed_work_statuses is None:
                        drill_payload["filter"]["status_categories"] = ["Done"]
                    if ba_completed_work_statuses:
                        drill_payload['filter']['statuses'] = payload['filter']["ba_completed_work_statuses"]
                        del drill_payload['filter']['ba_completed_work_statuses']
                    if ba_in_progress_statuses:
                        del drill_payload['filter']['ba_in_progress_statuses']

                    # LOG.info(f"drilldown_payload-------{drill_payload}")
                    has_next = True
                    page = 0
                    drilldown_df_main = pd.DataFrame()
                    # breakpoint()
                    while has_next:
                        drilldown_res = self.generate_drilldown_response(effort_payload=drill_payload, page=page)
                        if len(drilldown_res['records']) != 0:
                            drilldown_df = pd.json_normalize(drilldown_res['records'], max_level=1)
                            drilldown_df_main = drilldown_df_main.append(drilldown_df)
                            page = page + 1
                            has_next = drilldown_res['_metadata']['has_next']

                    if effort_unit == "ticket_time":
                        location = average_values.loc[average_values['key'] == m].index[0]
                        total = average_values.loc[location, 'effort']
                        df3 = pd.DataFrame()
                        for k in range(0, len(drilldown_df_main['assignee_list'])):
                            if len(drilldown_df_main['assignee_list'][k]) == 0:
                                continue
                            else:
                                df4 = pd.json_normalize(drilldown_df_main['assignee_list'][k][0])
                                df3 = df3.append(df4)
                        # breakpoint()
                        df3['start_time'] = df3['start_time'].astype("int64")
                        df3['end_time'] = df3['end_time'].astype("int64")
                        df3['diff'] = round((df3['end_time'] - df3['start_time']) / 86400).astype('int64')
                        df3 = df3.groupby('assignee')['diff'].sum().reset_index()
                        # breakpoint()
                        if df3['diff'].sum() != total:
                            flag.append(
                                {"total time spent in drilldown": df3['diff'][0], "total time spent in widget": total,
                                 "key": m})

                    else:
                        total_count = drilldown_res['_metadata']['total_count']
                        subset_df_drilldown_cat = drilldown_df_main[drilldown_df_main['ticket_category'] == g]
                        drilldown_percentage_cat = int(round((len(subset_df_drilldown_cat) / total_count) * 100))
                        # breakpoint()
                        if percentage_of_each_cat != drilldown_percentage_cat:
                            # breakpoint()
                            flag.append({"message": "drilldown and bar percentage not matching",
                                         "bar_percenatge": round(percentage_of_each_cat, 2),
                                         "drilldown_percentage_cat": round(drilldown_percentage_cat, 2), "category": g,
                                         "ou_id": ou_id, "gt": gt1, "lt": lt1})
                        else:
                            LOG.info(
                                f"the drilldown and widget data is matching for time_period---{gt1} and {lt1} with ou_id---{ou_id}")

        else:
            LOG.info("No Data returned by widget")

        return flag

    def effort_investment_payload_generation_for_compare(self, filter,
                                                         interval, ou_id, sprint_field, across, effort_unit,
                                                         ticket_categorization_scheme, type_of):

        categories = self.get_effort_investment_categories(ticket_categorization_scheme)
        categories.append("Other")
        df = pd.DataFrame()
        if len(categories) == 0:
            LOG.warning("NO category name found")
        keys = []
        # breakpoint()
        # filter =

        if across in ["ticket_category", "assignee"]:
            payload = self.effort_investment_payload_generation(filter=filter, ou_id=ou_id, sprint_field=sprint_field,
                                                                across=across,
                                                                interval=interval)
            LOG.info(f"Payload to create the report----{json.dumps(payload)}")
            response = self.get_effort_investment_response(payload, effort_unit=effort_unit, type_of=type_of)
            df1 = pd.json_normalize(response['records'], max_level=1)
            df = df.append(df1)

        else:
            for i in categories:
                filter["ticket_categories"] = [i]
                payload = self.effort_investment_payload_generation(filter=filter, ou_id=ou_id,
                                                                    sprint_field=sprint_field,
                                                                    across=across,
                                                                    interval=interval)
                LOG.info(f"Payload to create the report----{json.dumps(payload)}")
                response = self.get_effort_investment_response(payload, effort_unit=effort_unit, type_of=type_of)

                if len(response['records']) != 0:
                    for k in range(0, len(response['records'])):
                        keys.append(response['records'][k]['key'])

                    df1 = pd.json_normalize(response['records'], max_level=1)
                    df1['ticket_categories'] = i

                    df = df.append(df1)

        return df

    def effort_investment_engineer_report_widget_creation(self, filter_type, filter2, int_ids, gt, lt, custom_value,
                                                          interval, exclude, ou_id, datetime_filters,
                                                          sprint, sprint_field, across, active_work_unit, effort_unit,
                                                          ba_attribution_mode, ba_historical_assignees_statuses,
                                                          ba_completed_work_statuses, ba_in_progress_statuses,
                                                          dashboard_filter,
                                                          ticket_categorization_scheme):

        flag = []

        categories = self.get_effort_investment_categories(ticket_categorization_scheme)
        categories.append("Other")
        df = pd.DataFrame()
        if len(categories) == 0:
            LOG.warning("NO category name found")

        filter = self.effort_investment_filter_creation(filter_type, datetime_filters, filter2, exclude, sprint,
                                                        ba_attribution_mode,
                                                        ba_historical_assignees_statuses,
                                                        ba_completed_work_statuses,
                                                        int_ids, gt, lt,
                                                        custom_value, ticket_categorization_scheme,
                                                        ba_in_progress_statuses,
                                                        ticket_categories=None, dashboard_filter=dashboard_filter)
        for i in categories:
            filter["ticket_categories"] = [i]
            payload = self.effort_investment_payload_generation(filter=filter, ou_id=ou_id, sprint_field=sprint_field,
                                                                across=across,
                                                                interval=interval)
            LOG.info(f"Payload to create the report----{json.dumps(payload)}")
            response = self.get_effort_investment_response(payload, effort_unit=effort_unit, type_of=None)
            # breakpoint()
            if len(response['records']) != 0:
                df1 = pd.json_normalize(response['records'], max_level=1)
                df1['ticket_categories'] = i
                df = df.append(df1)

        if len(df) == 0:
            pytest.skip("No widget data available")
        if effort_unit == "ticket_time":
            df['effort'] = round(df["effort"] / 86400).astype('int64')

        average_values = df.groupby('key')['effort'].sum().reset_index()
        # average_values['total'] = average_values['total'].astype(int)
        # location = widget_response_df.loc[widget_response_df['key'] == start_gt].index[0]
        # total_tickets = widget_response_df.loc[location, 'total_tickets']

        keys = df['key'].unique().tolist()
        for m in keys:
            # average_values['total'].where(average_values['key'] == m)
            location = average_values.loc[average_values['key'] == m].index[0]
            total = average_values.loc[location, 'effort']
            story_point_total = 0
            drill_payload = deepcopy(payload)
            del drill_payload['filter']['ticket_categories']
            # del drill_payload["interval"]
            drill_payload['filter']["assignee_display_names"] = [m]
            drill_payload["ou_exclusions"] = ["assignees"]
            if ba_completed_work_statuses is None:
                drill_payload["filter"]["status_categories"] = ["Done"]
            if ba_in_progress_statuses:
                del drill_payload['filter']['ba_in_progress_statuses']
            # LOG.info(f"drilldown_payload-------{drill_payload}")
            has_next = True
            page = 0

            drilldown_df_main = pd.DataFrame()
            while has_next:
                drilldown_res = self.generate_drilldown_response(effort_payload=drill_payload, page=page)
                if len(drilldown_res['records']) != 0:
                    drilldown_df = pd.json_normalize(drilldown_res['records'], max_level=1)
                    drilldown_df_main = drilldown_df_main.append(drilldown_df)
                    page = page + 1
                    has_next = drilldown_res['_metadata']['has_next']
                    if effort_unit == "story_point":
                        for i in range(0, len(drilldown_res['records'])):
                            if "story_points" in drilldown_res['records'][i].keys():
                                story_point_total = story_point_total + drilldown_res['records'][i]['story_points']

            if effort_unit == "ticket_time":
                df3 = pd.DataFrame()
                for k in range(0, len(drilldown_df_main['assignee_list'])):
                    df4 = pd.json_normalize(drilldown_df_main['assignee_list'][k][0])
                    df3 = df3.append(df4)
                # breakpoint()
                df3['start_time'] = df3['start_time'].astype("int64")
                df3['end_time'] = df3['end_time'].astype("int64")
                df3['diff'] = round((df3['end_time'] - df3['start_time']) / 86400).astype('int64')
                df3 = df3.groupby('assignee')['diff'].sum().reset_index()
                if df3['diff'][0] != total:
                    flag.append(
                        {"total time spent in drilldown": df3['diff'][0], "total time spent in widget": total,
                         "key": m})

            elif effort_unit == "story_point":
                if story_point_total != total:
                    flag.append(
                        f"total story point  in widget {total} is  not matching total story points in drilldown {story_point_total}")


            else:
                if len(drilldown_df_main) != drilldown_res['_metadata']['total_count']:
                    flag.append({
                        f"drilldown_res['_metadata']['total_count']--{drilldown_res['_metadata']['total_count']} is not equal to the total records returned by API ---{len(drilldown_df_main)} for teh user --{m}"})
                total_count = drilldown_res['_metadata']['total_count']

                total_count_from_widget = total
                if total_count != total_count_from_widget:
                    flag.append({
                        f"total_drilldown_count--{total_count} and total widget count --{total_count_from_widget} are not same for teh user --{m}"})

        return flag
