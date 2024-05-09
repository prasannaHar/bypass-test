import requests
import json

from src.utils.OU_helper import Ouhelper


class WidgetDrillDown:
    def __init__(self, create_generic):
        self.generic = create_generic
        self.dashboard = Ouhelper(self.generic)

    def generate_issues_report_payload(self,
                                       arg_req_integration_ids=None,
                                       arg_product_id="",
                                       arg_metric="ticket",
                                       arg_sort_xaxis="value_high-low",
                                       arg_across="assignee",
                                       arg_id="ticket_count",
                                       arg_req_dynamic_fiters=[],
                                       arg_visualization="bar_chart",
                                       arg_across_limit=50,
                                       arg_stacks="",
                                       arg_ou_id=None,
                                       arg_interval=""
                                       ):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:
            arg_interval:
            arg_ou_id:
            arg_stacks:
            arg_across_limit:
            arg_visualization:
            arg_product_id:
            arg_req_integration_ids (list): required integration ids in list format
            arg_metric (str, optional): required metric name. Defaults to "ticket".
            arg_sort_xaxis (str, optional): required sorting option. Defaults to "value_high-low".
            arg_across (str, optional): required across or x-axis. Defaults to "assignee".
            arg_id (str, optional): required sorting nmetrics. Defaults to "ticket_count".
            arg_req_dynamic_fiters (list, optional): required filters in 2-D list with inside list had key and values pair. Defaults to []



        Returns:
            Json: dynamically generated payload
        """
        arg_ou_id = self.generic.get_org_unit_id(org_unit_name=self.generic.env["org_unit_name"])
        dynamic_filter_tags_making = {
            "sort_xaxis": arg_sort_xaxis,
            "product_id": arg_product_id}
        if arg_req_integration_ids:
            dynamic_filter_tags_making["integration_ids"] = arg_req_integration_ids
        if len(arg_metric) != 0:
            dynamic_filter_tags_making["metric"] = arg_metric

        # if len(arg_visualization) != 0:
        #     dynamic_filter_tags_making["visualization"] = arg_visualization

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
            # "sort": [
            #     {
            #         "id": arg_id,
            #         "desc": True
            #     }
            # ]
        }
        if arg_stacks != "":
            payloadDump["stacks"] = [arg_stacks]

        if arg_across != "trend":
            payloadDump["across_limit"] = arg_across_limit

        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
        return payloadDump

    def generate_issues_report_drilldown_payload(self,
                                                 arg_req_integration_ids,
                                                 arg_assignees=[],
                                                 arg_id="bounces",
                                                 arg_metric="story_point",
                                                 arg_across="assignee",
                                                 arg_req_dynamic_fiters=[],
                                                 arg_product_id="",
                                                 arg_visualization="bar_chart",
                                                 arg_stacks="",
                                                 arg_ou_id=None,
                                                 arg_ou_exclusion=[]

                                                 ):
        """this funtion will be responsible for generating the issues report drill-down payload


        Args:
            arg_ou_exclusion:
            arg_ou_id:
            arg_stacks:
            arg_visualization:
            arg_product_id:
            arg_req_integration_ids (list): required integration ids
            arg_assignees (list): required assignee ids
            arg_id (str, optional): sorting id taken for basis. Defaults to "bounces".
            arg_metric (str, optional): required metrics. Defaults to "story_point".
            arg_across (str, optional): required x-axis/across value. Defaults to "assignee".
            arg_req_dynamic_fiters (list, optional): required filters in 2-D list with inside list had key and values pair. Defaults to []



        Returns:
            Json: dynamically generated payload
        """
        arg_ou_id = self.generic.get_org_unit_id(org_unit_name=self.generic.env["org_unit_name"]),
        dynamic_filter_tags_making = {
            # "visualization": arg_visualization,
            "product_id": arg_product_id,
            "integration_ids": arg_req_integration_ids,

        }

        if len(arg_assignees) != 0:
            dynamic_filter_tags_making["assignees"] = arg_assignees

        if len(arg_metric) != 0:
            dynamic_filter_tags_making["metric"] = arg_metric

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "page": 0,
            "page_size": 10000,
            "sort": [
                {
                    "id": arg_id,
                    "desc": True
                }
            ],
            "filter": dynamic_filter_tags_making,
            "across": arg_across
        }
        if arg_stacks != "":
            payloadDump["stacks"] = [arg_stacks]
        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
            payloadDump["ou_user_filter_designation"] = {"sprint": ["customfield_10020"]}
        if len(arg_ou_exclusion) != 0:
            payloadDump["ou_exclusions"] = [arg_ou_exclusion]
        payload = json.dumps(payloadDump)
        return payload

    def generate_scm_pr_lead_time_by_stage_widget_payload(self,
                                                          arg_velocity_config_id_to_be_used,
                                                          arg_required_integration_ids=[],
                                                          arg_req_dynamic_fiters=[],
                                                          arg_assignees=[],
                                                          arg_ratings=["good", "slow", "needs_attention"],
                                                          arg_ou_ids=False
                                                          ):
        dynamic_filter_making = {
            "calculation": "pr_velocity",
            "ratings": arg_ratings,
            "velocity_config_id": arg_velocity_config_id_to_be_used,
            "limit_to_only_applicable_data": False,
        }

        if len(arg_required_integration_ids) > 0:
            dynamic_filter_making["integration_ids"] = arg_required_integration_ids

        if len(arg_assignees) > 0:
            dynamic_filter_making["assignees"] = arg_assignees
        for each_dyn_filter in arg_req_dynamic_fiters:
            dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

        payload_generated = {
            "filter": dynamic_filter_making,
            "across": "velocity",
            "ou_user_filter_designation": {
                "jira": ["none"],
                "azure_devops": ["none"]
            }
        }
        if type(arg_ou_ids) == type(True):
            ou_ids = self.generic.get_org_unit_id(org_unit_name=self.generic.env["org_unit_name"])
            payload_generated["ou_ids"] = ou_ids
        else:
            payload_generated["ou_ids"] = arg_ou_ids
        return payload_generated

    def generate_scm_pr_lead_time_filter_values_retriever_payload(self,
                                                                  arg_required_integration_ids,
                                                                  arg_required_filter_name
                                                                  ):
        dynamic_filter_tags_making = {
            "integration_ids": arg_required_integration_ids,
            "fields": [arg_required_filter_name],
            "filter": {
                "integration_ids": arg_required_integration_ids
            },
            "ou_id": self.generic.get_org_unit_id(org_unit_name=self.generic.env["org_unit_name"])
        }
        return dynamic_filter_tags_making

    def generate_scm_pr_lead_time_by_stage_drilldown_payload(self,
                                                             arg_velocity_config_id_to_be_used,
                                                             arg_required_stage,
                                                             arg_required_integration_ids=[],
                                                             arg_across="values",
                                                             arg_req_dynamic_fiters=[],
                                                             arg_ratings=[
                                                                 "good",
                                                                 "needs_attention",
                                                                 "slow",
                                                                 "missing"
                                                             ],
                                                             arg_ou_user_filter_designation={
                                                                 "jira": [
                                                                     "none"
                                                                 ]
                                                             },
                                                             arg_ou_ids=False
                                                             ):
        dynamic_filter_tags_making = {
            "ratings": arg_ratings,
            "velocity_config_id": arg_velocity_config_id_to_be_used,
            "limit_to_only_applicable_data": False,
            "calculation": "pr_velocity",
        }

        if len(arg_required_integration_ids) > 0:
            dynamic_filter_tags_making["integration_ids"] = arg_required_integration_ids

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        req_payload = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
            "ou_user_filter_designation": arg_ou_user_filter_designation,
            "page": 0,
            "page_size": 10000,
        }

        if type(arg_ou_ids) is type(True):
            ou_ids = self.generic.get_org_unit_id(org_unit_name=self.generic.env["org_unit_name"])
            req_payload["ou_ids"] = ou_ids
        else:
            req_payload["ou_ids"] = arg_ou_ids

        return req_payload

    def generate_jira_lead_time_filter_values_retriever_payload(self,
                                                                arg_required_integration_ids,
                                                                arg_required_filter_name
                                                                ):
        dynamic_filter_tags_making = {
            "integration_ids": arg_required_integration_ids,
            "fields": [arg_required_filter_name],
            "filter": {
                "integration_ids": arg_required_integration_ids
            }
        }

        payload = json.dumps(dynamic_filter_tags_making)

        return payload

    def generate_hygiene_report_payload(self,
                                        arg_required_integration_ids=[],
                                        arg_required_hygiene_types=[],
                                        arg_req_dynamic_fiters=[],
                                        arg_across="hygiene_type",
                                        arg_ou_ids=None,
                                        arg_ou_exclusion=[]
                                        ):
        arg_ou_ids = self.generic.get_org_unit_id(org_unit_name=self.generic.env["org_unit_name"])
        dynamic_filter_making = {
            "hideScore": False
        }

        if len(arg_required_integration_ids) > 0:
            dynamic_filter_making["integration_ids"] = arg_required_integration_ids

        if len(arg_required_hygiene_types) > 0:
            dynamic_filter_making["hygiene_types"] = arg_required_hygiene_types

        for each_dyn_filter in arg_req_dynamic_fiters:
            dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

        # if(len(arg_ou_ids)>0):

        #     payload_generated["ou_ids"] = arg_ou_ids

        payload_making = {
            "across": arg_across,
            "filter": dynamic_filter_making
        }

        if len(arg_ou_ids) != 0:
            payload_making["ou_ids"] = arg_ou_ids
            payload_making["ou_user_filter_designation"] = {"sprint": ["customfield_10000"]}

        if len(arg_ou_exclusion) != 0:
            payload_making["ou_exclusions"] = [arg_ou_exclusion]
        return payload_making

    def generate_hygiene_report_custom_hygiene_payload(self,
                                                       arg_required_integration_ids=[],
                                                       arg_required_hygiene_types=[],
                                                       arg_req_dynamic_fiters=[],
                                                       arg_across="project",
                                                       arg_exclude_custom_fields=False,
                                                       arg_ou_ids=None,
                                                       arg_ou_exclusion=[]
                                                       ):
        arg_ou_ids = self.generic.get_org_unit_id(org_unit_name=self.generic.env["org_unit_name"])
        dynamic_filter_making = {
            "hideScore": False,
            "product_id": "866",
        }
        if arg_exclude_custom_fields:
            dynamic_filter_making["exclude"] = {
                "custom_fields": {}
            }

        if len(arg_required_integration_ids) > 0:
            dynamic_filter_making["integration_ids"] = arg_required_integration_ids

        if len(arg_required_hygiene_types) > 0:
            dynamic_filter_making["hygiene_types"] = arg_required_hygiene_types

        for each_dyn_filter in arg_req_dynamic_fiters:
            dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

        payload_making = {
            "page": 0,
            "page_size": 0,
            # "sort": [
            #     {
            #         "id": "bounces",
            #         "desc": True
            #     }
            # ],
            "filter": dynamic_filter_making,
            "across": arg_across
        }
        if len(arg_ou_ids) != 0:
            payload_making["ou_ids"] = arg_ou_ids
            payload_making["ou_user_filter_designation"] = {"sprint": ["customfield_10020"]}

        if len(arg_ou_exclusion) != 0:
            payload_making["ou_exclusions"] = [arg_ou_exclusion]

        return payload_making

    def payload_custom_vs_filters(self):
        custom_hygienes_vs_filters_mapping = {

            "No PM Acceptance": [
                ["issue_types", ["STORY", "IMPROVEMENT"]]
            ],
            "Missing UX Design": [
                ["status_categories", ["Done"]]
            ],
            "No Business Priority": [
                ["priorities", ["HIGHEST", "HIGH", "MEDIUM"]]
            ],
            "No Customer Use cases": [
                ["statuses", ["DONE", "IN PROGRESS", "IN REVIEW", "TO DO"]]
            ],
            "Missing Story Points": [
                ["missing_fields", {"story_points": True}],
                ["issue_types", ["STORY", "BUG"]]
            ],
            "Large Story Points > 5": [
                ["issue_types", ["STORY", "BUG"]],
                ["story_points", {"$gt": "5"}]
            ]

        }
        return custom_hygienes_vs_filters_mapping
