import json
import logging
import random

import pytest
from src.utils.OU_helper import Ouhelper
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

generic_object = TGhelper()
OuHelper = Ouhelper(generic_object)


class TestIssueLeadTimeByStagetHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)
        self.env_info = self.generic.get_env_based_info()
        self.env = self.generic.get_connect_info()

    def convert_days_secs(self, days):
        seconds = days * 24 * 60 * 60
        return seconds

    def calculateRating(self, valueInSeconds, lowerLimitValue, upperLimitValue):
        if valueInSeconds is None:
            return "MISSING"
        if valueInSeconds <= lowerLimitValue:
            return "GOOD"
        else:
            if valueInSeconds <= upperLimitValue:
                return "NEEDS_ATTENTION"
            else:
                return "SLOW"

    def issues_lead_time_by_stage(self, across="velocity", calculation="ticket_velocity", rating=[], var_filters=False,
                                  keys=False):
        """ create issue backlog report """
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        integration_id = self.generic.integration_ids_basedon_workspace()
        velocity_config_id = self.env_info["env_jira_velocity_config_id"]
        projects = self.env_info["project_names"]

        if len(rating) == 0:
            rating = [
                "good",
                "slow",
                "needs_attention",
                "missing"
            ]
        filters = {"calculation": calculation, "work_items_type": "jira", "ratings": rating,
                   "limit_to_only_applicable_data": False, "velocity_config_id": velocity_config_id,
                   "integration_ids": integration_id, "jira_projects": projects}
        jira_default_time_range = self.generic.env["jira_default_time_range"]
        gt, lt = self.generic.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt, lt = self.generic.get_epoc_time(value=15, type="days")
        jira_issue_resolved_at = {"$gt": gt, "$lt": lt}
        filters["jira_issue_resolved_at"] = jira_issue_resolved_at
        if var_filters:
            filters.update(var_filters)
        resp = self.widget.create_issue_lead_time_by_stage_report(ou_ids=org_id, filters=filters, across=across)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")
        if resp["records"]:
            if keys:
                keys_id = {}
                for key in resp["records"]:
                    keys_id[key["key"]] = key["mean"]
                return keys_id
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def issues_lead_time_by_stage_drilldown(self, key, across="values", var_filters=False,
                                            calculation="ticket_velocity", rating=[], mean=False):
        """get drilldown of each key detatils"""

        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
        velocity_config_id = self.env_info["env_jira_velocity_config_id"]
        projects = self.env_info["project_names"]
        integration_id = self.generic.integration_ids_basedon_workspace()
        sort = [{"id": key, "desc": True}]
        if len(rating) == 0:
            rating = [
                "good",
                "slow",
                "needs_attention",
                "missing"
            ]
        filters = {"calculation": calculation, "integration_ids": integration_id,
                   "limit_to_only_applicable_data": False, "ratings": rating, "velocity_config_id": velocity_config_id,
                   "histogram_stage_name": key, "jira_projects": projects}
        # gt, lt = self.generic.get_epoc_time(value=15, type="days")
        jira_default_time_range = self.generic.env["jira_default_time_range"]
        gt, lt = self.generic.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        jira_issue_resolved_at = {"$gt": gt, "$lt": lt}
        filters["jira_issue_resolved_at"] = jira_issue_resolved_at

        if var_filters:
            filters.update(var_filters)
        resp_assign = self.widget.jira_drilldown_velocity_values(filters, ou_ids=org_id, across=across,
                                                                 ou_exclusions=across,
                                                                 sort=sort)
        if mean:
            mean_value = 0
            total_record_count = int(resp_assign['count'])
            for record in range(total_record_count):
                for data in range(len(resp_assign['records'][record]['data'])):
                    if resp_assign['records'][record]['data'][data]['key'] == key:
                        try:
                            mean_value = mean_value + int(resp_assign['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                mean_value = mean_value / total_record_count
                return mean_value
            else:
                return 0.0

        return resp_assign

    def max_3_values_filter_type(self, filter_type, int_ids):
        # df_sprint = ou_list
        req_filter_names_and_value_pair = ""
        if "customfield" in filter_type:
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]

        else:
            if filter_type == "statuses":
                filter_option = "status"
            elif filter_type == "priorities":
                filter_option = "priority"
            elif filter_type == "projects":
                filter_option = "project_name"
            elif filter_type == "status_categories":
                filter_option = "status_category"
            else:
                filter_option = filter_type[:-1]
        # breakpoint()
        get_filter_response = self.generic.get_filter_options(arg_filter_type=[filter_option],
                                                              arg_integration_ids=int_ids)

        if len(get_filter_response['records'][0][filter_option]) == 0:
            pytest.skip("Filter Doesnot have Any values")
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []

        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            if filter_option == "assignee":
                if eachissueType['additional_key'] != "_UNASSIGNED_":
                    value.append(eachissueType['additional_key'])
            else:
                value.append(eachissueType['key'])

        try:
            if "customfield" in filter_option:
                required_filters_needs_tobe_applied = ["custom_fields"]
                filter_value = [{filter_option: value}]
            else:
                required_filters_needs_tobe_applied = [filter_type]
                filter_value = [value]

            req_filter_names_and_value_pair = []
            for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                req_filter_names_and_value_pair.append([eachfilter, eachvalue])

        except Exception as ex:
            LOG.info("Exception max_3_values_filter_type function----{}".format(ex))

        return req_filter_names_and_value_pair

    def max_3_values_filter_type_ado(self, filter_type, int_ids):
        # df_sprint = ou_list
        req_filter_names_and_value_pair = ""
        """["code_area","teams","workitem_parent_workitem_ids","assignee","epic","fix_version","label","priority","reporter","status","project",
            "workitem_type","status_category","version"]"""
        LOG.info("filter_type in max_3_values_filter_type_ado------{} ".format(filter_type))
        get_filter_response = self.generic.get_filter_options_ado(arg_filter_type=filter_type,
                                                                  arg_integration_ids=int_ids)
        if len(get_filter_response['records'][0][filter_type]['records']) == 0:
            pytest.skip("Filter Doesnot have Any values")
        all_filter_records = [get_filter_response['records'][0][filter_type]['records']]
        value = []
        #
        filter_dict = {"status": "workitem_statuses", "assignee": "workitem_assignees", "project": "workitem_projects",
                       "status_category": "workitem_status_categories", "epic": "workitem_epics",
                       "fix_version": "workitem_fix_versions",
                       "label": "workitem_labels", "parent_workitem_id": "workitem_parent_workitem_ids",
                       "priority": "workitem_priorities", "reporter": "workitem_reporters",
                       "workitem_type": "workitem_types", "version": "workitem_versions"}

        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            if filter_type == "assignee":
                if eachissueType['additional_key'] != "_UNASSIGNED_":
                    value.append(eachissueType['additional_key'])
            else:
                value.append(eachissueType['key'])

        try:
            if "Microsoft" in filter_type:
                required_filters_needs_tobe_applied = ["workitem_custom_fields"]
                filter_value = [{filter_type: value}]
                filter_option = "workitem_custom_fields"
            elif "code_area" in filter_type:
                required_filters_needs_tobe_applied = ["workitem_attributes"]
                filter_value = [{"child": value}]
                filter_option = "workitem_attributes"
            elif "teams" in filter_type:
                required_filters_needs_tobe_applied = ["workitem_attributes"]
                filter_value = [{"teams": value}]
                filter_option = "workitem_attributes"

            else:
                filter_option = filter_dict[filter_type]
                required_filters_needs_tobe_applied = [filter_option]
                filter_value = [value]

            req_filter_names_and_value_pair = []
            for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                req_filter_names_and_value_pair.append([eachfilter, eachvalue])

        except Exception as ex:
            LOG.info("Exception max_3_values_filter_type ado function----{}".format(ex))

        return req_filter_names_and_value_pair, filter_option

    def get_jira_field_based_on_filter_type(self, filter_type):

        if "customfield" in filter_type:
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]

        else:
            if filter_type == "statuses":
                filter_option = "jira_statuses"
            elif filter_type == "priorities":
                filter_option = "jira_priorities"
            elif filter_type == "projects":
                filter_option = "jira_projects"
            elif filter_type == "status_categories":
                filter_option = "jira_status_categories"
            else:
                filter_option = filter_type[:-1]

        return filter_option

    def get_leadtime_workflow_profile_details(self, workflow_profile_id):
        lead_time_for_changes = []
        try:
            url = self.generic.connection['base_url'] + "velocity_configs/" + workflow_profile_id
            resp = self.generic.execute_api_call(url, 'get', data={})
            #     get lead time details from response
            lead_time_for_changes = resp['lead_time_for_changes']

        except Exception as ex:
            LOG.info("get_workflow_profile_details exception-----{}".format(ex))

        return lead_time_for_changes

    def create_new_dora_lead_time_widget(self, payload):
        lead_time_resp = []
        try:
            # breakpoint()
            lead_time_url = self.generic.connection['base_url'] + "dora/lead-time"
            LOG.info("leadtime dora url----{}".format(lead_time_url))
            lead_time_resp = self.generic.execute_api_call(lead_time_url, "post", data=payload)
            # LOG.info("lead Time resp----{}".format(lead_time_resp))

        except Exception as ex:
            LOG.info("create_new_dora_lead_time_widget exception-----{}".format(ex))
        return lead_time_resp

    def get_leadtime_drilldown_resp(self, payload):
        lead_time_drilldown_resp = []
        try:
            lead_time_drilldown_url = self.generic.connection['base_url'] + "dora/lead-time/drilldown"
            LOG.info("leadtime dora url----{}".format(lead_time_drilldown_url))
            lead_time_drilldown_resp = self.generic.execute_api_call(lead_time_drilldown_url, "post", data=payload)
            # LOG.info("lead Time resp----{}".format(lead_time_drilldown_resp))

        except Exception as ex:
            LOG.info("create_new_dora_lead_time_widget exception-----{}".format(ex))
        return lead_time_drilldown_resp

    def lead_time_payload(self, page, stage, ou_id, filter):

        payload = {"page": page, "page_size": 500,
                   "sort": [{"id": stage, "desc": True}],
                   "filter": filter,
                   "across": "values", "ou_ids": [ou_id], "ou_exclusions": ["values"]}

        LOG.info("payload_drilldoen-----{}".format(json.dumps(payload)))

        return payload

    def filter_creation(self, filter_type, integration_id, gt, lt, datetime_filters=None,
                        work_items_type="jira", exclude=None, custom_values=None, sprint=None):
        """creating jira/azure filter object for payload with different filter_type"""
        """{"filter": {"ratings": ["good", "slow", "needs_attention"], "calculation": "ticket_velocity",
                                 "work_items_type": "jira", "limit_to_only_applicable_data": True,
                                 "integration_ids": ["4284", "4292"]}, "ou_ids": ["32978"], "across": "velocity"}"""
        # breakpoint()
        filter = {"ratings": ["good", "slow", "needs_attention"], "limit_to_only_applicable_data": True,
                  "integration_ids": integration_id, "work_items_type": work_items_type,
                  "calculation": "ticket_velocity"}
        if work_items_type == "jira":
            if filter_type is not None and exclude is None:
                projects = self.env_info['project_names']
                # LOG.info("projects---{}".format(projects))
                # filter.update({"jira_projects": ["PROP"]})

                if len(projects) == 0 and filter_type == 'projects':
                    filter_update = self.max_3_values_filter_type(filter_type, int_ids=integration_id)
                    filter_update[0][0] = self.get_jira_field_based_on_filter_type(filter_type)
                    filter.update({filter_update[0][0]: filter_update[0][1]})
                if len(projects) != 0 and filter_type == 'projects':
                    filter.update({"jira_projects": projects})
                if filter_type not in ['projects']:
                    if filter_type:
                        if "custom" in filter_type:
                            filter.update({"custom_fields": {filter_type: custom_values}})
                    else:
                        filter_update = self.max_3_values_filter_type(filter_type, int_ids=integration_id)
                        filter_update[0][0] = self.get_jira_field_based_on_filter_type(filter_type)
                        filter.update({filter_update[0][0]: filter_update[0][1]})
                        filter_update = self.max_3_values_filter_type(filter_type="projects", int_ids=integration_id)
                        filter_update[0][0] = self.get_jira_field_based_on_filter_type("projects")
                        filter.update({filter_update[0][0]: filter_update[0][1]})

                if sprint:
                    if sprint == 'ACTIVE':
                        filter.update({"sprint_states": ["ACTIVE"]})
                    else:
                        filter.update({"last_sprint": True})
                if datetime_filters:
                    filter.update({datetime_filters: {"$gt": gt, "$lt": lt}})
                elif datetime_filters is None:
                    filter.update({"jira_issue_created_at": {"$gt": gt, "$lt": lt}})

            if filter_type is not None and exclude is not None:
                """not excluding project as lot of data might get injested"""
                # breakpoint()
                projects = self.env_info['project_names']
                # LOG.info("projects---{}".format(projects))
                # filter.update({"jira_projects": ["PROP"]})
                if len(projects) == 0 and filter_type == 'projects':
                    filter_update = self.max_3_values_filter_type(filter_type, int_ids=integration_id)
                    filter_update[0][0] = self.get_jira_field_based_on_filter_type(filter_type)
                    filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})
                if len(projects) != 0 and filter_type == 'projects':
                    filter.update({"exclude": {"jira_projects": projects}})

                if filter_type not in ['projects']:
                    if filter_type:
                        if "custom" in filter_type:
                            filter.update({"custom_fields": {filter_type: custom_values}})
                    else:
                        filter_update = self.max_3_values_filter_type(filter_type, int_ids=integration_id)
                        filter_update[0][0] = self.get_jira_field_based_on_filter_type(filter_type)
                        filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})
                        filter_update = self.max_3_values_filter_type(filter_type="projects", int_ids=integration_id)
                        filter_update[0][0] = self.get_jira_field_based_on_filter_type("projects")
                        filter.update({filter_update[0][0]: filter_update[0][1]})

                if sprint:
                    if sprint == 'ACTIVE':
                        filter.update({"sprint_states": ["ACTIVE"]})
                    else:
                        filter.update({"last_sprint": True})
                if datetime_filters:
                    filter.update({datetime_filters: {"$gt": gt, "$lt": lt}})
                elif datetime_filters is None:
                    filter.update({"jira_issue_created_at": {"$gt": gt, "$lt": lt}})

        elif work_items_type == "azure_devops":
            if filter_type is not None and exclude is None:
                projects = self.env_info['project_names']
                # LOG.info("projects---{}".format(projects))
                # filter.update({"jira_projects": ["PROP"]})
                if len(projects) == 0 and filter_type == 'project':
                    filter_update, filter_option = self.max_3_values_filter_type_ado(filter_type,
                                                                                     int_ids=integration_id)
                    filter_update[0][0] = filter_option
                    filter.update({filter_update[0][0]: filter_update[0][1]})
                if len(projects) != 0 and filter_type == 'project':
                    filter.update({"workitem_projects": projects})
                if filter_type not in ['projects']:
                    filter_update, filter_option = self.max_3_values_filter_type_ado(filter_type,
                                                                                     int_ids=integration_id)
                    filter_update[0][0] = filter_option
                    filter.update({filter_update[0][0]: filter_update[0][1]})
                    filter_update, filter_option = self.max_3_values_filter_type_ado(filter_type="project",
                                                                                     int_ids=integration_id)
                    filter_update[0][0] = filter_option
                    filter.update({filter_update[0][0]: filter_update[0][1]})

                filter.update({"workitem_created_at": {"$gt": gt, "$lt": lt}})

            if filter_type is not None and exclude is not None:
                # breakpoint()
                projects = self.env_info['project_names']
                # LOG.info("projects---{}".format(projects))
                # filter.update({"jira_projects": ["PROP"]})
                if len(projects) == 0 and filter_type == 'projects':
                    filter_update, filter_option = self.max_3_values_filter_type_ado(filter_type,
                                                                                     int_ids=integration_id)
                    filter_update[0][0] = filter_option
                    filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})
                if len(projects) != 0 and filter_type == 'projects':
                    filter.update({"exclude": {"workitem_projects": projects}})
                if filter_type not in ['projects']:
                    filter_update, filter_option = self.max_3_values_filter_type_ado(filter_type,
                                                                                     int_ids=integration_id)
                    filter_update[0][0] = filter_option
                    filter.update({"exclude": {filter_update[0][0]: filter_update[0][1]}})
                    # breakpoint()
                    filter_update, filter_option = self.max_3_values_filter_type_ado(filter_type="project",
                                                                                     int_ids=integration_id)
                    filter_update[0][0] = filter_option
                    filter.update({filter_update[0][0]: filter_update[0][1]})

                filter.update({"workitem_created_at": {"$gt": gt, "$lt": lt}})

        return filter

    def new_dora_lead_time_ticket_velocity(self, gt, lt, filter_type, ou_id=None, exclude=None):
        # ci_cd_application = ["harnessng", "jenkins", "circleci"]
        lead_time_resp = []
        rating_list = []

        try:
            """check for the integrations in OU if section all integrations then take all the integrations from workspace f not take the specific integration 
            from the OU section"""
            # breakpoint()
            # if ou_id is None:
            #     ou_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name"])
            integration_id = []
            application_list = []
            integration_id = integration_id + self.generic.get_integrations_based_on_ou_id(ou_id=ou_id)
            workspace_id = OuHelper.get_workspace_associated_to_ou(ou_id=ou_id)
            # breakpoint()
            if len(integration_id) == 0:
                application_list = self.generic.get_application_type_with_workspace_id(workspace_id[0])

            # from OU get workflow profile id
            workflow_profile_id = OuHelper.get_workflow_profile_associated_OU(ou_id=ou_id)

            lead_time_for_changes = self.get_leadtime_workflow_profile_details(workflow_profile_id[0])
            stages = []

            """check the stages from workflow profile tied to the OU and accordingly pick the integration ids """
            # breakpoint()
            if len(lead_time_for_changes['fixed_stages']) != 0:
                for i in range(0, len(lead_time_for_changes['fixed_stages'])):
                    stages.append(lead_time_for_changes['fixed_stages'][i]['name'])
                if len(application_list) != 0:
                    integration_id = integration_id + application_list[2]['scm']

            if len(lead_time_for_changes['post_development_custom_stages']) != 0:
                for i in range(0, len(lead_time_for_changes['post_development_custom_stages'])):
                    stages.append(lead_time_for_changes['post_development_custom_stages'][i]['name'])
                if len(application_list) != 0:
                    integration_id = integration_id + application_list[3]['pipelines']

            if lead_time_for_changes["issue_management_integrations"][0].lower() == "Jira".lower():
                if len(lead_time_for_changes["pre_development_custom_stages"]) != 0:
                    for i in range(0, len(lead_time_for_changes['pre_development_custom_stages'])):
                        stages.append(lead_time_for_changes['pre_development_custom_stages'][i]['name'])
                    if len(application_list) != 0:
                        integration_id = integration_id + application_list[1]['jira']

                filter = self.filter_creation(filter_type, integration_id, gt, lt, work_items_type="jira",
                                              exclude=exclude)

            if lead_time_for_changes["issue_management_integrations"][0].lower() == "azure_devops":
                if len(lead_time_for_changes["pre_development_custom_stages"]) != 0:
                    for i in range(0, len(lead_time_for_changes['pre_development_custom_stages'])):
                        stages.append(lead_time_for_changes['pre_development_custom_stages'][i]['name'])
                    if len(application_list) != 0:
                        integration_id = integration_id + application_list[0]['azure_boards']
                # breakpoint()
                filter = self.filter_creation(filter_type, integration_id, gt, lt, work_items_type="azure_devops",
                                              exclude=exclude)

            payload = {"filter": filter, "ou_ids": [ou_id], "across": "velocity"}
            LOG.info("lead_time payload------{}".format(payload))

            lead_time_resp = self.create_new_dora_lead_time_widget(payload)
            if len(lead_time_resp['records']) == 0:
                LOG.info("No records founds----lead_time_resp['records'] {}".format(lead_time_resp['records']))

            else:
                for i in range(0, len(lead_time_resp['records'])):
                    if lead_time_resp['records'][i]["velocity_stage_result"]["lower_limit_unit"] == "DAYS" and \
                            lead_time_resp['records'][i]["velocity_stage_result"]["upper_limit_unit"] == "DAYS":
                        lower_limit = self.convert_days_secs(
                            lead_time_resp['records'][i]["velocity_stage_result"]["lower_limit_value"])
                        upper_limit = self.convert_days_secs(
                            lead_time_resp['records'][i]["velocity_stage_result"]["upper_limit_value"])
                        if "median" not in list(lead_time_resp['records'][i].keys()):
                            rating = "MISSING"
                        else:
                            rating = self.calculateRating(lowerLimitValue=lower_limit, upperLimitValue=upper_limit,
                                                          valueInSeconds=lead_time_resp['records'][i]['median'])

                    elif lead_time_resp['records'][i]["velocity_stage_result"]["lower_limit_unit"] == "SECONDS" and \
                            lead_time_resp['records'][i]["velocity_stage_result"]["upper_limit_unit"] == "SECONDS":
                        lower_limit = lead_time_resp['records'][i]["velocity_stage_result"]["lower_limit_value"]
                        upper_limit = lead_time_resp['records'][i]["velocity_stage_result"]["upper_limit_value"]
                        if "median" not in list(lead_time_resp['records'][i].keys()):
                            rating = "MISSING"
                        else:
                            rating = self.calculateRating(lowerLimitValue=lower_limit, upperLimitValue=upper_limit,
                                                          valueInSeconds=lead_time_resp['records'][i]['median'])

                    if rating != lead_time_resp['records'][i]["velocity_stage_result"]['rating']:
                        rating_list.append({"key": lead_time_resp['records'][i]['key'],
                                            "api_rating": lead_time_resp['records'][i]["velocity_stage_result"][
                                                'rating'],
                                            "calculated_rating": rating})

            # LOG.info("lead_time_resp---.{}".format(lead_time_resp))

        except Exception as ex:
            LOG.info("Exception caught in new_dora_leadtime_funct-----{}".format(ex))

        return lead_time_resp, stages, filter, rating_list

    def lead_time_drilldown_verification(self, filter, ou_id, stage):
        flag_list = []
        rating_list = []
        drilldown_dict = {}
        filter_update = {"histogram_stage_name": stage}
        filter.update(filter_update)
        individual_stage_mean = 0
        page = 0
        drilldown_resp = self.get_leadtime_drilldown_resp(self.lead_time_payload(page, stage, ou_id, filter))
        drilldown_dict.update({"drilldown_count": drilldown_resp["_metadata"]['total_count']})
        total = 0

        # breakpoint()
        try:

            for i in range(0, len(drilldown_resp['records'])):
                each_ticket_total_mean = 0
                total = total + drilldown_resp['records'][i]['total']
                dict = []

                for j in range(0, len(drilldown_resp['records'][i]['data'])):
                    dict.append(
                        {"velocity_stage_result": drilldown_resp['records'][i]['data'][j]['velocity_stage_result'],
                         "key": drilldown_resp['records'][i]["additional_key"]})

                    if "mean" in list(drilldown_resp['records'][i]['data'][j].keys()):
                        each_ticket_total_mean = each_ticket_total_mean + drilldown_resp['records'][i]['data'][j][
                            'mean']
                        if drilldown_resp['records'][i]['data'][j]['key'].lower() == stage.lower():
                            individual_stage_mean = individual_stage_mean + drilldown_resp['records'][i]['data'][j][
                                'mean']
                # LOG.info(
                #     "round(drilldown_resp['records'][i]['total']) == round(each_ticket_total_mean)-------{} , {}".format(
                #         drilldown_resp['records'][i]['total'], each_ticket_total_mean))
                drilldown_dict.update(
                    {"total_of_all_tickets": total, "stage": stage, "total_stage_mean": individual_stage_mean})
                if round(drilldown_resp['records'][i]['total']) != round(each_ticket_total_mean):
                    # LOG.info(
                    #     """total of mean in the each ticket stage is not matching the total leadtime of the ticket---{},round(drilldown_resp['records'][i][total)-----{},round(each_ticket_total_mean)----{}""".format(
                    #         drilldown_resp['records'][i]['additional_key'], drilldown_resp['records'][i]['total'],
                    #         each_ticket_total_mean))
                    flag_list.append(
                        "total of mean in the each ticket stage is not matching the total leadtime of the ticket---{}".format(
                            drilldown_resp['records'][i]['additional_key']))

            if len(drilldown_resp['records']) > 500:
                LOG.info("Given page size is 500 but getting more records")
                flag_list.append("Given page size is 10 but getting more records")
            while drilldown_resp["_metadata"]['has_next']:
                page = page + 1
                drilldown_resp = self.get_leadtime_drilldown_resp(self.lead_time_payload(page, stage, ou_id, filter))
                for i in range(0, len(drilldown_resp['records'])):
                    each_ticket_total_mean = 0
                    total = total + drilldown_resp['records'][i]['total']

                    for j in range(0, len(drilldown_resp['records'][i]['data'])):
                        if "mean" in list(drilldown_resp['records'][i]['data'][j].keys()):
                            each_ticket_total_mean = each_ticket_total_mean + drilldown_resp['records'][i]['data'][j][
                                'mean']
                            if drilldown_resp['records'][i]['data'][j]['key'].lower() == stage.lower():
                                individual_stage_mean = individual_stage_mean + drilldown_resp['records'][i]['data'][j][
                                    'mean']

                                """Verify the rating of each record"""

                        if drilldown_resp['records'][i]['data'][j]['velocity_stage_result'][
                            "lower_limit_unit"] == "DAYS" and \
                                drilldown_resp['records'][i]['data'][j]['velocity_stage_result'][
                                    "upper_limit_unit"] == "DAYS":
                            lower_limit = self.convert_days_secs(
                                drilldown_resp['records'][i]['data'][j]['velocity_stage_result']["lower_limit_value"])
                            upper_limit = self.convert_days_secs(
                                drilldown_resp['records'][i]['data'][j]['velocity_stage_result']["upper_limit_value"])
                            if "median" not in list(drilldown_resp['records'][i]['data'][j].keys()):
                                rating = "MISSING"
                            else:
                                rating = self.calculateRating(lowerLimitValue=lower_limit, upperLimitValue=upper_limit,
                                                              valueInSeconds=drilldown_resp['records'][i]['data'][j][
                                                                  'median'])

                        elif "SECONDS" in drilldown_resp['records'][i]['data'][j]['velocity_stage_result'][
                            "lower_limit_unit"] and \
                                "SECONDS" in drilldown_resp['records'][i]['data'][j]['velocity_stage_result'][
                            "upper_limit_unit"]:
                            lower_limit = drilldown_resp['records'][i]['data'][j]['velocity_stage_result'][
                                "lower_limit_value"]
                            upper_limit = drilldown_resp['records'][i]['data'][j]['velocity_stage_result'][
                                "upper_limit_value"]
                            if "median" not in list(drilldown_resp['records'][i]['data'][j].keys()):
                                rating = "MISSING"
                            else:
                                rating = self.calculateRating(lowerLimitValue=lower_limit, upperLimitValue=upper_limit,
                                                              valueInSeconds=drilldown_resp['records'][i]['data'][j][
                                                                  'median'])
                        if rating != drilldown_resp['records'][i]['data'][j]['velocity_stage_result']['rating']:
                            rating_list.append({"key": drilldown_resp['records'][i]['additional_key'],
                                                "api_rating":
                                                    drilldown_resp['records'][i]['data'][j]['velocity_stage_result'][
                                                        'rating'],
                                                "calculated_rating": rating})
                    # LOG.info(
                    #     "round(drilldown_resp['records'][i]['total']) == round(each_ticket_total_mean)-------{} , {}".format(
                    #         drilldown_resp['records'][i]['total'], each_ticket_total_mean))
                    if round(drilldown_resp['records'][i][
                                 'total']) != round(each_ticket_total_mean):
                        LOG.info(
                            "total of mean in the each ticket stage is not matching the total leadtime of the ticket---{}".format(
                                drilldown_resp['records'][i]['additional_key']))
                        flag_list.append(
                            "total of mean in the each ticket stage is not matching the total leadtime of the ticket---{}".format(
                                drilldown_resp['records'][i]['additional_key']))

            if drilldown_resp["_metadata"]['total_count'] == 0:
                LOG.warning("Drilldown has no data -----drilldown_resp['_metadata']['total_count']--{}".format(
                    drilldown_resp["_metadata"]['total_count']))
                avg_total_of_all_tickets = 0
                individual_stage_mean = 0
            else:
                avg_total_of_all_tickets = (total / drilldown_resp["_metadata"]['total_count'])
                individual_stage_mean = individual_stage_mean / drilldown_resp["_metadata"]['total_count']

            drilldown_dict.update(
                {"total_of_all_tickets": avg_total_of_all_tickets, "stage": stage,
                 "total_stage_mean": individual_stage_mean})
            LOG.info("total----.{}{}".format(total, stage))

        except Exception as ex:
            flag_list.append("There is exception in lead_time_drilldown_verification-----{}".format(ex))
            LOG.info("The exception in caused in lead_time_drilldown_verification---{}".format(ex))
        return drilldown_dict, flag_list, rating_list

    def lead_time_single_stat(self, gt, lt, filter_type, sprint_field, integration_id, datetime_filters, ou_id=None,
                              calculation="ticket_velocity", velocity_config_id=None, exclude=None, custom_values=None,
                              sprint=None):
        filter = self.filter_creation(filter_type=filter_type, sprint=sprint, integration_id=integration_id, gt=gt, lt=lt, work_items_type="jira",
                                      exclude=exclude, datetime_filters=datetime_filters, custom_values=custom_values)

        filter.update({"calculation": calculation, "velocity_config_id": velocity_config_id})
        payload = {"filter": filter, "ou_ids": [ou_id], "across": "velocity",
                   "ou_user_filter_designation": {"sprint": [sprint_field]}}

        base_url = self.generic.connection["base_url"] + self.api_info["velocity"]
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        if len(resp["records"]) == 0:
            pytest.skip("no data in widget API")

        return resp
