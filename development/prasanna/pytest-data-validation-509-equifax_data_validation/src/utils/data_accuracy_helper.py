import json
import logging
import random
import numpy as np
import pandas as pd
import inspect

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class DataAccuracyHelper:
    def __init__(self, create_gen):
        self.generic = create_gen
        self.env_info = self.generic.get_env_based_info()

    def accuracy_payload(self, metric=None, arg_req_dynamic_fiters=None, visualization=None, dashboardfilter=None,
                         across=None, stacks=None, calculation=None, ratings=None, jira_issue_types=None,
                         velocity_config_id=None, agg_type=None,
                         across_limit=None, sort_value=None, ou_ids=None, customfield=None, interval=None,
                         exclude=None,
                         ou_exclusions=None, hygiene_types=None, issue_resolved_at=None, issue_types=None,
                         jira_issue_resolved_at=None, pr_merged_at=None, committed_at=None,
                         code_change_size_config=None, time_period=None, integration_ids=None):
        payload = {}
        projects = self.env_info['project_names']
        dynamic_filter_tags_making = {"integration_ids": integration_ids, "projects": projects}
        if arg_req_dynamic_fiters:
            for eachDynamicFilter in arg_req_dynamic_fiters:
                dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]
        if metric:
            dynamic_filter_tags_making['metric'] = metric
        if visualization:
            dynamic_filter_tags_making['visualization'] = visualization
        if dashboardfilter:
            dynamic_filter_tags_making['or'] = dashboardfilter
        if exclude:
            dynamic_filter_tags_making['exclude'] = exclude
        if ratings:
            dynamic_filter_tags_making['ratings'] = ratings
        if calculation:
            dynamic_filter_tags_making["calculation"] = calculation
        if jira_issue_types:
            dynamic_filter_tags_making["jira_issue_types"] = jira_issue_types
        if velocity_config_id:
            dynamic_filter_tags_making["velocity_config_id"] = velocity_config_id
        if issue_resolved_at:
            dynamic_filter_tags_making["issue_resolved_at"] = issue_resolved_at
        if issue_types:
            dynamic_filter_tags_making["issue_types"] = issue_types
        if pr_merged_at:
            dynamic_filter_tags_making["pr_merged_at"] = pr_merged_at
        if jira_issue_resolved_at:
            dynamic_filter_tags_making["jira_issue_resolved_at"] = jira_issue_resolved_at
        if code_change_size_config:
            dynamic_filter_tags_making["code_change_size_config"] = code_change_size_config
        if agg_type:
            dynamic_filter_tags_making["agg_type"] = agg_type
        if committed_at:
            dynamic_filter_tags_making["committed_at"] = committed_at
        if time_period:
            dynamic_filter_tags_making["committed_at"] = committed_at
        if across:
            payload['across'] = across
        if across_limit:
            payload['across_limit'] = across_limit
        if sort_value:
            payload['sort'] = sort_value
        if stacks:
            payload["stacks"] = [stacks]
        if interval:
            payload["interval"] = interval
        if ou_exclusions:
            payload['ou_exclusions'] = [ou_exclusions]
        if hygiene_types:
            payload["hygiene_types"]: [hygiene_types]
        if ou_ids:
            payload['ou_ids'] = [ou_ids]
            if customfield:
                payload['ou_user_filter_designation'] = customfield

        payload['filter'] = dynamic_filter_tags_making
        LOG.info("payload = {}".format(json.dumps(payload)))
        return payload

    def comparing_reports(self, widgetname_response, related_response, add_keys=False):
        """This will compare the data in both widgets"""
        flag = True
        zero_flag = True
        try:
            if len(related_response['records']) == 0:
                LOG.info("related not having data")
                zero_flag = False
            elif len(widgetname_response['records']) == 0:
                LOG.info("widget name not having data")
                zero_flag = False
            else:

                rl_df = pd.json_normalize(related_response['records'])
                wn_df = pd.json_normalize(widgetname_response['records'])
                columns = np.intersect1d(wn_df.columns, rl_df.columns)
                wn_df = wn_df.applymap(lambda s: s.strip() if type(s) == str else s)
                wn_df = pd.DataFrame(wn_df, columns=columns)
                if add_keys:
                    wn_df['additional_key'] = pd.to_datetime(wn_df['additional_key'], format='%d-%M-%Y')
                rl_df = rl_df.applymap(lambda s: s.strip() if type(s) == str else s)
                rl_df = pd.DataFrame(rl_df, columns=columns)
                if add_keys:
                    rl_df['additional_key'] = pd.to_datetime(rl_df['additional_key'], format='%d-%M-%Y')
                wn_df = wn_df.astype(str)
                rl_df = rl_df.astype(str)
                val1 = pd.merge(wn_df, rl_df, on=columns.tolist(),
                                how='outer', indicator=True)
                LOG.info("related widget data not matching - {}".format(val1[val1['_merge'] == 'right_only'].head))
                LOG.info("widget name data not matching - {}".format(val1[val1['_merge'] == 'left_only'].head))
                if (len(val1[val1['_merge'] == 'right_only']) != 0) or (len(val1[val1['_merge'] == 'left_only']) != 0) :
                    val1.to_csv(
                        "log_updates/" + str(inspect.stack()[1][3])
                        + '.csv', header=True,
                        index=False, mode='a')
                    flag = False


        except Exception as ex:
            LOG.info("Not executed")
            flag = False
            return flag, zero_flag
        return flag, zero_flag

    def dropdown_validation(self, response, across_type, ou_id, customfield, int_ids):
        flag = True
        ran_value = random.sample(response['records'], min(2, len(response['records'])))
        if ran_value:
            dropdown_list = self.accuracy_payload(
                metric="ticket",
                visualization="bar_chart",
                across=across_type,
                sort_value=[{"id": "ticket_count", "desc": True}], ou_ids=ou_id, customfield=customfield,
                ou_exclusions=across_type, integration_ids=int_ids)
            for list_data in ran_value:
                # breakpoint()
                key_value = list_data["key"]
                if "customfield" in across_type:
                    dropdown_list["filter"]["custom_fields"] = {across_type: [key_value]}
                elif across_type == "priority":
                    dropdown_list["filter"]["priorities"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["priorities"]
                elif across_type == "label":
                    dropdown_list["filter"]["labels"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["labels"]
                elif across_type == "project":
                    dropdown_list["filter"]["projects"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["projects"]
                elif across_type == "sprint":
                    dropdown_list["filter"]["sprints"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["sprints"]
                elif across_type == "assignee":
                    dropdown_list["filter"]["assignees"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["assignees"]
                elif across_type == "status":
                    dropdown_list["filter"]["statuses"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["statuses"]
                elif across_type == "resolution":
                    dropdown_list["filter"]["resolutions"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["resolutions"]
                elif across_type == "issue_type":
                    dropdown_list["filter"]["issue_types"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["issue_types"]
                elif across_type == "epic":
                    dropdown_list["filter"]["epics"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["epics"]
                elif across_type == "fix_version":
                    dropdown_list["filter"]["fix_versions"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["fix_versions"]
                elif across_type == "status_category":
                    dropdown_list["filter"]["status_categories"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["status_categories"]
                elif across_type == "reporter":
                    dropdown_list["filter"]["reporters"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["reporters"]
                elif across_type == "component":
                    dropdown_list["filter"]["components"] = [key_value]
                    dropdown_list["ou_exclusions"] = ["components"]
                else:
                    dropdown_list["filter"][across_type] = [key_value]
                baseurl_list = self.generic.connection[
                                   "base_url"] + self.generic.api_data["drill_down_api_url"]
                LOG.info("payload : {}".format(dropdown_list))
                list_response = self.generic.execute_api_call(baseurl_list, "post", data=dropdown_list)
                if list_response['_metadata']['total_count'] != list_data['total_tickets']:
                    flag = False
        return flag

    def compare_data_in_df(self, first_resp, sec_resp, data_name=None):

        if (len(first_resp['records']) != 0) and (len(sec_resp['records']) != 0):
            rl_df = pd.json_normalize(first_resp['records'])
            sc_df = pd.json_normalize(sec_resp['records'])
            rl_df = rl_df[rl_df.key.str.lower().str.contains(data_name)]
            if "stage" in sc_df.columns:
                done_df = sc_df[sc_df.stage.str.contains("DONE")]
                wont_df = sc_df[sc_df.stage.str.contains("WON'T DO")]
            else:
                done_df = sc_df[sc_df.key.str.contains("DONE")]
                wont_df = sc_df[sc_df.key.str.contains("WON'T DO")]
            count = int(rl_df['count'])
            totalticket = int(done_df['total_tickets']) + int(wont_df['total_tickets'])
            return count == totalticket
        else:
            LOG.info("No Data")
            return False
