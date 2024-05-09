import json
import os
from random import randint
import calendar
import time

velocity_config_id = os.getenv('lead_time_velocity_config_id')


class GenericPayload:
    def __init__(self):
        pass

    def generate_user_login_payload(self, arg_req_username, arg_req_password, arg_req_company):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:
        Returns:
            Json: dynamically generated payload
        """

        payloadDump = {
            "username": arg_req_username,
            "password": arg_req_password,
            "company": arg_req_company
        }
        return payloadDump

    def generate_project_payload(self, arg_project_name, arg_project_description, arg_owner_id, ):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:


        Returns:
            Json: dynamically generated payload
        """
        randint(1, 99999)
        payloadDump = {
            "name": arg_project_name,
            "description": arg_project_description,
            "owner": "",
            "key": randint(1, 99999),
            "owner_id": arg_owner_id
        }
        return payloadDump

    def generate_map_integration_payload(self, arg_integration_id, arg_product_id):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments
        Args:
        Returns:
            Json: dynamically generated payload
        """
        payloadDump = {
            "integration_id": arg_integration_id,
            "product_id": arg_product_id,
            "mappings": {}
        }

        payload = json.dumps(payloadDump)

        return payload

    def generate_create_dashboard_payload(self, arg_name, arg_owner_id,owner_mail,
                                          arg_show_org_unit_selection=False, arg_time_range=False,
                                          arg_investment_profile=False, arg_investment_unit=False, ou_ids=None):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:
            investment_unit:
            investment_profile:
            time_range:
            show_org_unit_selection:
            arg_filters: if any filters are available
            arg_req_integration_ids (list): required integration ids in list format
            arg_metric (str, optional): required metric name. Defaults to "ticket".
            arg_sort_xaxis (str, optional): required sorting option. Defaults to "value_high-low".
            arg_across (str, optional): required across or x-axis. Defaults to "assignee".
            arg_id (str, optional): required sorting nmetrics. Defaults to "ticket_count".
            arg_req_dynamic_fiters (list, optional): required filters in 2-D list with inside list had key and values pair. Defaults to []

        Returns:
            Json: dynamically generated payload
        """
        # for eachDynamicFilter in arg_req_dynamic_fiters:
        #     dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]
        metadata = {"rbac":{"owner":owner_mail,"dashboardPermission":"admin","users":{}}}

        if arg_time_range:
            metadata["dashboard_time_range"] = True
            metadata["dashboard_time_range_filter"] = "last_30_days"

        if arg_investment_profile:
            metadata["effort_investment_profile"] = True
            metadata["effort_investment_profile_filter"] = "b530ea0b-1c76-4b3c-8646-e2c4c0fe402c"

        if arg_investment_unit:
            metadata["effort_investment_unit"] = True
            metadata["effort_investment_unit_filter"] = "%_of_engineers"

        payload = {
            "name": arg_name,
            "default": False,
            "public": False,
            "type": "dashboard",
            "query": {
            },
            "owner_id": arg_owner_id,
            "metadata": metadata,
            "widgets": [],
            "category": ou_ids
        }
        return payload

    def generate_reset_password_payload(self, arg_req_username, arg_req_company):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:


        Returns:
            Json: dynamically generated payload
        """

        payloadDump = {
            "username": arg_req_username,
            "company": arg_req_company
        }
        return payloadDump

    def generate_update_dashboard_payload(self, arg_dashboard_id, arg_req_username, arg_name, arg_product_id,
                                          arg_owner_id,
                                          arg_integration_ids=[], arg_show_org_unit_selection=False,
                                          arg_time_range=False,
                                          arg_investment_profile=False, arg_investment_unit=False):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:
            investment_unit:
            investment_profile:
            time_range:
            show_org_unit_selection:
            arg_filters: if any filters are available
            arg_req_integration_ids (list): required integration ids in list format
            arg_metric (str, optional): required metric name. Defaults to "ticket".
            arg_sort_xaxis (str, optional): required sorting option. Defaults to "value_high-low".
            arg_across (str, optional): required across or x-axis. Defaults to "assignee".
            arg_id (str, optional): required sorting nmetrics. Defaults to "ticket_count".
            arg_req_dynamic_fiters (list, optional): required filters in 2-D list with inside list had key and values pair. Defaults to []



        Returns:
            Json: dynamically generated payload
        """
        # for eachDynamicFilter in arg_req_dynamic_fiters:
        #     dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]
        ts = calendar.timegm(time.gmtime())
        metadata = {"show_org_unit_selection": arg_show_org_unit_selection,
                    "rbac": {
                        "owner": arg_req_username,
                        "isPublic": "public",
                        "users": {}
                    }
                    }

        if arg_time_range:
            metadata["dashboard_time_range"] = True
            metadata["dashboard_time_range_filter"] = "last_30_days"

        if arg_investment_profile:
            metadata["effort_investment_profile"] = True
            metadata["effort_investment_profile_filter"] = "b530ea0b-1c76-4b3c-8646-e2c4c0fe402c"

        if arg_investment_unit:
            metadata["effort_investment_unit"] = True
            metadata["effort_investment_unit_filter"] = "%_of_engineers"

        # metadata["xx"]="val"

        payload = {
            "id": arg_dashboard_id,
            "name": arg_name,
            "default": False,
            "public": True,
            "type": "dashboard",
            "query": {
                "product_id": arg_product_id,
                "integration_ids": arg_integration_ids
            },
            "owner_id": arg_owner_id,
            "metadata": metadata,
            "widgets": [],
            "created_at": ts
        }

        return payload

    def generate_clone_dashboard_payload(self, arg_dashboard_id,
                                         arg_req_username,
                                         arg_name,
                                         arg_product_id,
                                         arg_owner_id,
                                         arg_integration_ids=[],
                                         arg_show_org_unit_selection=False,
                                         arg_time_range=False,
                                         arg_investment_profile=False,
                                         arg_investment_unit=False
                                         ):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:
            investment_unit:
            investment_profile:
            time_range:
            show_org_unit_selection:
            arg_filters: if any filters are available
            arg_req_integration_ids (list): required integration ids in list format
            arg_metric (str, optional): required metric name. Defaults to "ticket".
            arg_sort_xaxis (str, optional): required sorting option. Defaults to "value_high-low".
            arg_across (str, optional): required across or x-axis. Defaults to "assignee".
            arg_id (str, optional): required sorting nmetrics. Defaults to "ticket_count".
            arg_req_dynamic_fiters (list, optional): required filters in 2-D list with inside list had key and values pair. Defaults to []



        Returns:
            Json: dynamically generated payload
        """
        # for eachDynamicFilter in arg_req_dynamic_fiters:
        #     dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]
        ts = calendar.timegm(time.gmtime())
        metadata = {"show_org_unit_selection": arg_show_org_unit_selection,
                    "rbac": {
                        "owner": arg_req_username,
                        "isPublic": "public",
                        "users": {}
                    }
                    }

        if arg_time_range:
            metadata["dashboard_time_range"] = True
            metadata["dashboard_time_range_filter"] = "last_30_days"

        if arg_investment_profile:
            metadata["effort_investment_profile"] = True
            metadata["effort_investment_profile_filter"] = "b530ea0b-1c76-4b3c-8646-e2c4c0fe402c"

        if arg_investment_unit:
            metadata["effort_investment_unit"] = True
            metadata["effort_investment_unit_filter"] = "%_of_engineers"

        # metadata["xx"]="val"

        payload = json.dumps({
            "id": arg_dashboard_id,
            "name": arg_name,
            "default": False,
            "public": True,
            "type": "dashboard",
            "query": {
                "product_id": arg_product_id,
                "integration_ids": arg_integration_ids
            },
            "owner_id": arg_owner_id,
            "metadata": metadata,
            "widgets": [],
            "created_at": ts
        })

        return payload

    def generate_export_dashboard_payload(self, arg_dashboard_id, arg_dashboard_name, arg_owner_id):
        ts = calendar.timegm(time.gmtime())

        payloadDump = {
            "dashboard_id": arg_dashboard_id,
            "file_id": "file-id-down" + str(ts),
            "name": arg_dashboard_name,
            "created_by": arg_owner_id

        }
        return payloadDump

    def generate_project_update_payload(self, arg_project_id, arg_project_name, arg_project_description,
                                        arg_owner_id, arg_project_key):
        payloadDump = {
            "id": arg_project_id,
            "name": arg_project_name,
            "description": arg_project_description,
            "key": arg_project_key,
            "owner_id": arg_owner_id

        }
        return payloadDump

    def generate_create_user_payload(self, arg_user_type,password_enabled,sso_enabled,mfa_enabled):
        ts = calendar.timegm(time.gmtime())
        new_email = "n_u" + arg_user_type + "-" + str(ts) + "@mail.com"
        payloadDump = {
            "email": new_email,
            "first_name": "first" + arg_user_type,
            "last_name": "test_user",
            "saml_auth_enabled": sso_enabled,
            "password_auth_enabled": password_enabled,
            "user_type": arg_user_type,
            "notify_user": True,
            "mfa_enabled": mfa_enabled
        }
        return payloadDump

    def generate_update_user_payload(self, arg_user_type, arg_first_name, arg_last_name, arg_email):

        ts = calendar.timegm(time.gmtime())
        # new_email = "n_u" + arg_user_type + "-" + str(ts) + "@mail.com"

        payloadDump = {
            "email": arg_email,
            "first_name": arg_first_name,
            "last_name": arg_last_name,
            "saml_auth_enabled": False,
            "password_auth_enabled": True,
            "user_type": arg_user_type,
            "notify_user": True,
            "mfa_enabled": False
        }
        payloadDump = json.dumps(payloadDump)
        return payloadDump

    def generate_filter_options_payload(self, arg_filter_type, arg_integration_ids, integrations_inside_only=None):
        payloadDump = {
            "integration_ids": arg_integration_ids,
            "fields": arg_filter_type,
            "filter": {
                "integration_ids": arg_integration_ids
            }
        }
        if integrations_inside_only:
            del payloadDump["integration_ids"]
        return payloadDump

    def generate_create_leadtime_stagereport_payload(self, arg_project_id, arg_custom_stacks=[], arg_metric="ticket",
                                                     arg_sort_xaxis="value_high-low", arg_across="velocity",
                                                     # arg_id="ticket_count",
                                                     arg_interval="", arg_stacks="", arg_req_dynamic_fiters=[],
                                                     arg_ou_user_filter_designation={},
                                                     arg_ou_id=[], arg_rating=[], arg_req_integration_ids=[],
                                                     arg_velocity_config_id=velocity_config_id):
        if len(arg_rating) == 0:
            arg_rating = [
                "good",
                "slow",
                "needs_attention",
                "missing"
            ]
        dynamic_filter_tags_making = {
            "calculation": "ticket_velocity",
            "limit_to_only_applicable_data": False,
            "ratings": arg_rating,
            "velocity_config_id": arg_velocity_config_id,
            "product_id": arg_project_id,
            "integration_ids": arg_req_integration_ids
        }
        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
            "ou_user_filter_designation": {
            }
            # "across_limit": 20,
            # "sort": [
            #     {
            #         "id": arg_id,
            #         "desc": True
            #     }
            # ]
        }
        # if arg_interval != "":
        #     payloadDump["interval"] = arg_interval
        # if arg_stacks != "":
        #     payloadDump["stacks"] = arg_stacks

        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
        return payloadDump

    def generate_leadtime_stagereport_drilldown_payload(self, arg_key, arg_product_id="", arg_across="values",
                                                        arg_req_dynamic_fiters=[],
                                                        arg_ou_user_filter_designation={}, arg_rating=[], arg_ou_id=[],
                                                        arg_req_integration_ids=[],
                                                        arg_velocity_config_id=velocity_config_id):
        if len(arg_rating) == 0:
            arg_rating = [
                "good",
                "slow",
                "needs_attention",
                "missing"
            ]
        dynamic_filter_tags_making = {
            "velocity_config_id": arg_velocity_config_id,
            "limit_to_only_applicable_data": False,
            "ratings": arg_rating,
            "calculation": "ticket_velocity",
            "product_id": arg_product_id,
            "integration_ids": arg_req_integration_ids,
            "histogram_stage_name": arg_key
        }

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "page": 0,
            "page_size": 10000,
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
            "sort": [
                {
                    "id": arg_key,
                    "desc": True
                }
            ]
        }
        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
            payloadDump["ou_exclusions"] = ["values"]

        return payloadDump

    def generate_create_leadtime_trendeport_payload(self, arg_project_id,  # arg_custom_stacks=[],
                                                    arg_metric="ticket", arg_sort_xaxis="value_high-low",
                                                    arg_across="trend", arg_id="ticket_count", arg_interval="",
                                                    arg_stacks="", arg_req_dynamic_fiters=[],
                                                    arg_ou_user_filter_designation={},
                                                    arg_ou_id=[], arg_rating=[], arg_req_integration_ids=[],
                                                    arg_velocity_config_id=velocity_config_id):
        dynamic_filter_tags_making = {
            "calculation": "ticket_velocity",
            "limit_to_only_applicable_data": False,
            "velocity_config_id": arg_velocity_config_id,
            "product_id": arg_project_id,
            "integration_ids": arg_req_integration_ids
        }

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
            "ou_user_filter_designation": {
            }
            # "across_limit": 20,
            # "sort": [
            #     {
            #         "id": arg_id,
            #         "desc": True
            #     }
            # ]
        }
        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
        return payloadDump

    def generate_leadtime_trendreport_drilldown_payload(self, arg_key, arg_product_id="", arg_across="trend",
                                                        arg_req_dynamic_fiters=[], arg_ou_user_filter_designation={},
                                                        arg_ou_id=[], arg_req_integration_ids=[],
                                                        arg_velocity_config_id=velocity_config_id

                                                        ):
        dynamic_filter_tags_making = {
            "velocity_config_id": arg_velocity_config_id,
            "limit_to_only_applicable_data": False,
            "across": arg_across,
            "calculation": "ticket_velocity",
            "product_id": arg_product_id,
            "integration_ids": arg_req_integration_ids,
            "value_trend_keys": [arg_key]
        }

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "page": 0,
            "page_size": 10000,
            "filter": dynamic_filter_tags_making,
            "across": "values",
            "sort": [
                {
                    "id": [
                        {
                            "id": [

                            ],
                            "desc": True
                        }
                    ],
                    "desc": True
                }
            ]
        }
        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id

            payloadDump["ou_exclusions"] = ["values"]
        return payloadDump

    def generate_create_leadtime_singlestat_payload(self, arg_project_id, arg_across="velocity",
                                                    arg_req_dynamic_fiters=[],
                                                    arg_ou_id=[], arg_req_integration_ids=[],
                                                    arg_velocity_config_id=velocity_config_id):
        dynamic_filter_tags_making = {
            "calculation": "ticket_velocity",
            "velocity_config_id": arg_velocity_config_id,
            "product_id": arg_project_id,
            "integration_ids": arg_req_integration_ids
        }
        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]
        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
            "ou_user_filter_designation": {
            }
        }
        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
        return payloadDump

    def generate_create_issue_resolution_time_single_stat_payload(self,
                                                                  arg_project_id,
                                                                  arg_lt,
                                                                  arg_gt,
                                                                  arg_aggregation_type="average",
                                                                  arg_across="issue_created",
                                                                  arg_req_dynamic_fiters=[],
                                                                  arg_ou_user_filter_designation={},
                                                                  arg_ou_id=[],
                                                                  arg_req_integration_ids=[],

                                                                  ):
        dynamic_filter_tags_making = {
            arg_across + "_at": {
                "$gt": arg_gt,
                "$lt": arg_lt
            },
            "agg_type": arg_aggregation_type,
            "integration_ids": arg_req_integration_ids,
            "product_id": arg_project_id,
        }
        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]
        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,

        }

        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
            payloadDump["ou_user_filter_designation"] = {"sprint": ["customfield_10020"]}
        return payloadDump

    def generate_create_issue_resolution_time_report_widget_payload(self,
                                                                    arg_project_id,
                                                                    arg_lt,
                                                                    arg_gt,
                                                                    arg_across="assignee",
                                                                    arg_req_dynamic_fiters=[],
                                                                    arg_ou_user_filter_designation={},
                                                                    arg_ou_id=[],
                                                                    arg_req_integration_ids=[],
                                                                    arg_across_limit=20,
                                                                    arg_id="resolution_time",
                                                                    arg_metric=["median_resolution_time",
                                                                                "number_of_tickets_closed"],
                                                                    arg_sort_xaxis="value_high-low"

                                                                    ):
        dynamic_filter_tags_making = {
            "issue_resolved_at": {
                "$gt": arg_gt,
                "$lt": arg_lt
            },
            "metric": arg_metric,
            "integration_ids": arg_req_integration_ids,
            "product_id": arg_project_id,
            "sort_xaxis": arg_sort_xaxis
        }
        if arg_across != "assignee":
            dynamic_filter_tags_making["custom_fields"] = {"customfield_10020": []}

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
            "across_limit": arg_across_limit,
            "sort": [
                {
                    "id": arg_id, "desc": True
                }
            ]

        }

        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
            payloadDump["ou_user_filter_designation"] = {"sprint": ["customfield_10020"]}
        return payloadDump

    def generate_create_issue_resolution_time_report_drilldown_payload(self,
                                                                       arg_project_id,
                                                                       arg_lt,
                                                                       arg_gt,
                                                                       arg_key,
                                                                       arg_across="assignee",
                                                                       arg_req_dynamic_fiters=[],
                                                                       arg_ou_user_filter_designation={},
                                                                       arg_ou_id=[],
                                                                       arg_req_integration_ids=[],
                                                                       arg_across_limit=20,
                                                                       arg_id="bounces",
                                                                       arg_metric=["median_resolution_time",
                                                                                   "number_of_tickets_closed"],
                                                                       arg_sort_xaxis="value_high-low",
                                                                       arg_ou_exclusion=[]

                                                                       ):
        dynamic_filter_tags_making = {
            "issue_resolved_at": {
                "$gt": arg_gt,
                "$lt": arg_lt
            },
            "include_solve_time": True,
            "metric": arg_metric,
            "integration_ids": arg_req_integration_ids,
            "product_id": arg_project_id,
        }
        if arg_across == "assignee":
            dynamic_filter_tags_making["assignees"] = arg_key
        else:
            dynamic_filter_tags_making["custom_fields"] = {"customfield_10020": [], arg_across: arg_key}

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
        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
            payloadDump["ou_user_filter_designation"] = {"sprint": ["customfield_10020"]}

        if len(arg_ou_exclusion) != 0:
            payloadDump["ou_exclusions"] = [arg_ou_exclusion]

        return payloadDump

    def generate_create_issue_singlestat_payload(self,
                                                 arg_project_id,
                                                 arg_lt,
                                                 arg_gt,
                                                 arg_across="issue_created",
                                                 arg_req_dynamic_fiters=[],
                                                 arg_ou_user_filter_designation={},
                                                 arg_ou_id=[],
                                                 arg_req_integration_ids=[]
                                                 ):

        dynamic_filter_tags_making = {
            arg_across + "_at": {
                "$gt": arg_gt,
                "$lt": arg_lt
            },
            "integration_ids": arg_req_integration_ids,
            "product_id": arg_project_id,
        }

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "across": arg_across,
        }
        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
            payloadDump["ou_user_filter_designation"] = {"sprint": ["customfield_10020"]}

        return payloadDump

    def generate_investment_profile_creation_payload(self, arg_required_profile_name):
        required_payload = {
            "id": "6701f268-7688-4a2f-b374-611679d97eec",
            "default_scheme": False,
            "name": arg_required_profile_name,
            "config": {
                "integration_type": "jira",
                "categories": {
                    "1": {
                        "id": "a935847c-4f95-4879-bd0e-548d912b8eb5",
                        "name": "bugs",
                        "index": 1,
                        "color": "#F2917A",
                        "goals": {
                            "enabled": True,
                            "ideal_range": {
                                "min": 0,
                                "max": 25
                            },
                            "acceptable_range": {
                                "min": 0,
                                "max": 50
                            }
                        },
                        "filter": {
                            "issue_types": [
                                "BUG"
                            ]
                        }
                    },
                    "2": {
                        "id": "13643589-3545-4cd6-bff8-fbfd2ec2d76a",
                        "name": "features",
                        "index": 2,
                        "color": "#EEA248",
                        "goals": {
                            "enabled": True,
                            "ideal_range": {
                                "min": 0,
                                "max": 25
                            },
                            "acceptable_range": {
                                "min": 0,
                                "max": 50
                            }
                        },
                        "filter": {
                            "issue_types": [
                                "NEW FEATURE"
                            ]
                        }
                    },
                    "3": {
                        "id": "e8169225-0459-4d38-8401-82ce9b6a52c5",
                        "name": "improvements",
                        "index": 3,
                        "color": "#CAC035",
                        "goals": {
                            "enabled": True,
                            "ideal_range": {
                                "min": 0,
                                "max": 10
                            },
                            "acceptable_range": {
                                "min": 0,
                                "max": 20
                            }
                        },
                        "filter": {
                            "issue_types": [
                                "IMPROVEMENT"
                            ]
                        }
                    },
                    "4": {
                        "id": "e3af3a3c-0568-4bef-9e99-2888bf34c692",
                        "name": "tasks",
                        "index": 4,
                        "color": "#84C67B",
                        "goals": {
                            "enabled": True,
                            "ideal_range": {
                                "min": 0,
                                "max": 10
                            },
                            "acceptable_range": {
                                "min": 0,
                                "max": 20
                            }
                        },
                        "filter": {
                            "issue_types": [
                                "TASK"
                            ]
                        }
                    },
                    "5": {
                        "id": "6dd1c696-dad4-409c-8d18-8533043f6cc2",
                        "name": "stories",
                        "index": 5,
                        "color": "#30C5E8",
                        "goals": {
                            "enabled": True,
                            "ideal_range": {
                                "min": 0,
                                "max": 15
                            },
                            "acceptable_range": {
                                "min": 0,
                                "max": 30
                            }
                        },
                        "filter": {
                            "issue_types": [
                                "STORY"
                            ]
                        }
                    }
                },
                "uncategorized": {
                    "color": "#FC91AA",
                    "goals": {
                        "enabled": True,
                        "ideal_range": {
                            "min": 0,
                            "max": 15
                        },
                        "acceptable_range": {
                            "min": 0,
                            "max": 30
                        }
                    }
                },
                "active_work": {
                    "issues": {
                        "active_sprints": False,
                        "in_progress": False,
                        "assigned": False
                    }
                }
            },
            "created_at": 1652279561181,
            "updated_at": 1652279561181
        }
        del required_payload["id"]
        return required_payload

    def generate_workflow_profile_tickets_created_payload(self, arg_required_profile_name):

        required_payload = {
            "name": arg_required_profile_name,
            "default_config": False,
            "fixed_stages": [
                {
                    "order": 0,
                    "name": "Lead time to First Commit",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_COMMIT_CREATED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 1,
                    "name": "PR Creation Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_CREATED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 2,
                    "name": "Time to First Comment",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_REVIEW_STARTED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 3,
                    "name": "Approval Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_APPROVED",
                        "params": {
                            "last_approval": [
                                "true"
                            ]
                        }
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                },
                {
                    "order": 4,
                    "name": "Merge Time",
                    "event": {
                        "any_label_added": False,
                        "type": "SCM_PR_MERGED"
                    },
                    "lower_limit_value": 864000,
                    "upper_limit_value": 2592000,
                    "lower_limit_unit": "SECONDS",
                    "upper_limit_unit": "SECONDS"
                }
            ],
            "issue_management_integrations": [
                "jira"
            ],
            "scm_config": {
                "release": {
                    "source_branch": {
                        "$begins": [
                            "release"
                        ]
                    },
                    "target_branch": {
                        "$begins": [
                            "release"
                        ]
                    },
                    "tags": {
                        "$begins": [
                            "release"
                        ]
                    },
                    "labels": {
                        "$begins": [
                            "release"
                        ]
                    }
                },
                "deployment": {
                    "source_branch": {
                        "$begins": [
                            "deploy"
                        ]
                    },
                    "target_branch": {
                        "$begins": [
                            "deploy"
                        ]
                    },
                    "tags": {
                        "$begins": [
                            "deploy"
                        ]
                    },
                    "labels": {
                        "$begins": [
                            "deploy"
                        ]
                    }
                },
                "hotfix": {
                    "source_branch": {
                        "$begins": [
                            "hotfix",
                            "hf"
                        ]
                    },
                    "target_branch": {
                        "$begins": [
                            "hotfix",
                            "hf"
                        ]
                    },
                    "tags": {
                        "$begins": [
                            "hotfix",
                            "hf"
                        ]
                    },
                    "labels": {
                        "$begins": [
                            "hotfix",
                            "hf"
                        ]
                    }
                },
                "defect": {
                    "source_branch": {
                        "$begins": [
                            "bugfix",
                            "fix",
                            "bug"
                        ]
                    },
                    "target_branch": {
                        "$begins": [
                            "bugfix",
                            "fix",
                            "bug"
                        ]
                    },
                    "tags": {
                        "$begins": [
                            "bugfix",
                            "fix",
                            "bug"
                        ]
                    },
                    "labels": {
                        "$begins": [
                            "bugfix",
                            "fix",
                            "bug"
                        ]
                    }
                }
            }
        }
        return required_payload

    def generate_workflow_profile_tickets_created_update_payload(self, arg_existing_profile_payload):

        del arg_existing_profile_payload['created_at']
        del arg_existing_profile_payload['updated_at']
        adding_new_stage_name = "pre_development_custom_stages"
        adding_new_stage_payload_data = [
            {
                "order": 0,
                "name": "test11",
                "event": {
                    "type": "JIRA_STATUS",
                    "values": [
                        "BACKLOG",
                        "BLOCKED",
                        "DEV IN PROGRESS",
                        "DONE",
                        "IN PROGRESS",
                        "IN REVIEW",
                        "INTERNAL REVIEW",
                        "LOST",
                        "MERGED",
                        "OPEN",
                        "READY FOR PROD",
                        "READY FOR QA",
                        "SELECTED FOR DEVELOPMENT",
                        "TEST CREATED",
                        "TODO",
                        "TO DO",
                        "WAITING FOR CUSTOMER",
                        "WAITING ON OTHER ISSUES"
                    ]
                },
                "lower_limit_value": 4,
                "upper_limit_value": 11,
                "lower_limit_unit": "DAYS",
                "upper_limit_unit": "DAYS"
            }
        ]
        arg_existing_profile_payload[adding_new_stage_name] = adding_new_stage_payload_data
        return arg_existing_profile_payload

    def generate_create_sprint_matric_single_stat_report_widget_payload(self,
            arg_lt,
            arg_gt,
            arg_req_dynamic_fiters=[],
            arg_ou_user_filter_designation={},
            arg_ou_id=[],
            arg_req_integration_ids=[],
            arg_agg_type="average",
            arg_include_total_count=False,
            arg_metric=""

    ):
        dynamic_filter_tags_making = {
            "include_issue_keys": True,
            "agg_type": arg_agg_type,
            "completed_at": {
                "$gt": arg_gt,
                "$lt": arg_lt
            },
            "integration_ids": arg_req_integration_ids,

        }
        if arg_include_total_count:
            dynamic_filter_tags_making["include_total_count"] = arg_include_total_count
        if len(arg_metric) != 0:
            dynamic_filter_tags_making["metric"] = arg_metric

        for eachDynamicFilter in arg_req_dynamic_fiters:
            dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

        payloadDump = {
            "filter": dynamic_filter_tags_making,
            "page": 0,
            "page_size": 1000,
        }

        if len(arg_ou_id) != 0:
            payloadDump["ou_ids"] = arg_ou_id
            payloadDump["ou_user_filter_designation"] = {"sprint": ["sprint_report"]}

        return payloadDump

    def generate_dev_prod_profile_dynamic_payload_settings_change(self,
            arg_dev_prod_existing_settings,
            arg_req_feature_values_needs_to_be_updated_dict={},
            arg_req_sub_feature_values_needs_to_be_updated_dict={},
            arg_override_effort_investment_profile=""
    ):
        required_payload = {}
        required_payload["id"] = arg_dev_prod_existing_settings["id"]
        required_payload["name"] = arg_dev_prod_existing_settings["name"]
        required_payload["default_profile"] = False
        required_payload["effort_investment_profile_id"] = arg_dev_prod_existing_settings[
            "effort_investment_profile_id"]
        if arg_override_effort_investment_profile != "":
            required_payload["effort_investment_profile_id"] = arg_override_effort_investment_profile
        if "settings" in arg_dev_prod_existing_settings.keys():
            required_payload["settings"] = arg_dev_prod_existing_settings["settings"]
        required_payload_sections = []
        complete_section_details = arg_dev_prod_existing_settings["sections"]
        new_section_details = []
        for eachSection in complete_section_details:
            del eachSection['id']
            new_features_section = []
            for eachKeyMain in arg_req_feature_values_needs_to_be_updated_dict.keys():
                eachKeyMainValue = arg_req_feature_values_needs_to_be_updated_dict[eachKeyMain]
                if eachKeyMain == eachSection["name"]:
                    eachSection[eachKeyMainValue[0]] = eachKeyMainValue[1]
            sub_features = eachSection["features"]
            for eachSubfeature in sub_features:
                del eachSubfeature["id"]
                for eachKey in arg_req_sub_feature_values_needs_to_be_updated_dict.keys():
                    eachKeyValue = arg_req_sub_feature_values_needs_to_be_updated_dict[eachKey]
                    if eachKey == eachSubfeature["name"]:
                        eachSubfeature[eachKeyValue[0]] = eachKeyValue[1]
                new_features_section.append(eachSubfeature)
            eachSection["features"] = new_features_section
            new_section_details.append(eachSection)
        required_payload["sections"] = new_section_details
        return required_payload

    def generate_customer_all_ticket(self, integration_id, sprint, ou_id):
        payload = {
      "page_size": 100,
      "filter": {
        "metric": "ticket",
        "sort_xaxis": "value_high-low",
        "visualization": "bar_chart",
        "custom_fields": {
            "customfield_10000":
                sprint

        },
        "issue_types": [
          "STORY"
        ],
        "product_id": "1",
        "integration_ids": integration_id
      },
      "ou_ids": [
        ou_id
      ],
      "ou_user_filter_designation": {
        "sprint": [
          "customfield_10000"
        ]
      },
      "ou_exclusions": [
        "issue_types",
        "customfield_10000"
      ]
    }
        return payload

    def generate_workspace_payload(self, arg_project_name, arg_owner_id,integration_id=False ):
        """this funtion will be responsible for generating the
        dynamic payload based on the given input arguments

        Args:


        Returns:
            Json: dynamically generated payload
        """
        randint(1, 99999)
        payloadDump = {
            "name": arg_project_name,
            "key": randint(1, 99999),
            "owner_id": arg_owner_id
        }
        if integration_id:
            payloadDump.update({"integration_ids":integration_id})

        return payloadDump
