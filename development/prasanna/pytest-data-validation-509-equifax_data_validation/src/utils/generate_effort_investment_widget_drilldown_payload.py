import json
import os
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

generic_object = TGhelper()
effort_investment_profile_id = generic_object.env["effort_investment_profile_id"]
arg_ou_id = generic_object.get_org_unit_id(org_unit_name=generic_object.env["org_unit_name"])
sprint_metric_custom_config = generic_object.env["ou_user_filter_designation"]
project_filter = generic_object.env["project_names"]

def generate_create_effort_investment_trend_payload(
        arg_gt,
        arg_lt,
        arg_project_id,
        arg_interval="",
        arg_across="ticket_category",
        arg_req_dynamic_fiters=[],
        arg_ticket_categories=[],
        arg_req_integration_ids=[],
        arg_ou_id=generic_object.get_org_unit_id(org_unit_name=generic_object.env["org_unit_name"])

):
    dynamic_filter_tags_making = {
        "issue_resolved_at": {
            "$gt": arg_gt,
            "$lt": arg_lt
        },
        "ticket_categorization_scheme": effort_investment_profile_id,
        # "product_id": arg_project_id,
        "integration_ids":
            arg_req_integration_ids

    }
    if arg_across == "issue_resolved_at" or arg_across == "assignee":
        dynamic_filter_tags_making["ba_attribution_mode"] = "current_assignee"
    if len(arg_ticket_categories) != 0:
        dynamic_filter_tags_making["ticket_categories"] = [arg_ticket_categories]

    arg_req_dynamic_fiters.append(["projects", project_filter])
    for eachDynamicFilter in arg_req_dynamic_fiters:
        dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

    payloadDump = {
        "filter": dynamic_filter_tags_making,
        "across": arg_across,

    }
    if len(arg_interval) != 0:
        payloadDump["interval"] = arg_interval

    payloadDump["ou_ids"] = arg_ou_id
    payloadDump["ou_user_filter_designation"] = sprint_metric_custom_config

    return payloadDump


def generate_effort_investment_drilldown_payload(
        arg_gt,
        arg_lt,
        arg_project_id,
        arg_across="issue_resolved_at",
        arg_req_dynamic_fiters=[],
        arg_rating=[],
        arg_req_integration_ids=[],
        arg_status_categories="",
        arg_assignee_display_names="",
        arg_ou_id=generic_object.get_org_unit_id(org_unit_name=generic_object.env["org_unit_name"])

):
    dynamic_filter_tags_making = {
        "issue_resolved_at": {
            "$gt": arg_gt,
            "$lt": arg_lt
        },
        "ba_attribution_mode": "current_assignee",
        "ticket_categorization_scheme": effort_investment_profile_id,
        "product_id": arg_project_id,
        "integration_ids": arg_req_integration_ids,
        "status_categories": ["Done"]
    }
    if len(arg_status_categories) != 0:
        dynamic_filter_tags_making["status_categories"] = [arg_status_categories]

    if len(arg_assignee_display_names) != 0:
        dynamic_filter_tags_making["assignee_display_names"] = [arg_assignee_display_names]

    arg_req_dynamic_fiters.append(["projects", project_filter])
    for eachDynamicFilter in arg_req_dynamic_fiters:
        dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

    payloadDump = {
        "page": 0,
        "page_size": 10000,
        "filter": dynamic_filter_tags_making,
        "across": arg_across,
    }
    if len(arg_ou_id) != 0:
        payloadDump["ou_ids"] = arg_ou_id
        payloadDump["ou_user_filter_designation"] = sprint_metric_custom_config

    return payloadDump


def generate_create_effort_investment_single_stat_payload(
        arg_gt,
        arg_lt,
        arg_project_id,
        arg_across="ticket_category",
        arg_req_dynamic_fiters=[],
        arg_req_integration_ids=[],
        arg_ou_id=generic_object.get_org_unit_id(org_unit_name=generic_object.env["org_unit_name"])

):
    dynamic_filter_tags_making = {
        "issue_resolved_at": {
            "$gt": arg_gt,
            "$lt": arg_lt
        },
        "ticket_categorization_scheme": effort_investment_profile_id,
        "product_id": arg_project_id,
        "integration_ids":
            arg_req_integration_ids

    }
    if arg_across == "issue_resolved_at":
        dynamic_filter_tags_making["ba_attribution_mode"] = "current_assignee"

    for eachDynamicFilter in arg_req_dynamic_fiters:
        dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

    payloadDump = {
        "filter": dynamic_filter_tags_making,
        "across": arg_across,

    }

    if arg_ou_id:
        payloadDump["ou_ids"] = arg_ou_id
        payloadDump["ou_user_filter_designation"] = sprint_metric_custom_config

    return payloadDump
