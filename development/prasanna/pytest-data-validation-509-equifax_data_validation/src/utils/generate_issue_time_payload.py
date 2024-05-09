import requests
import json


def generate_create_issue_time_across_report_widget_payload(
        arg_req_integration_ids,
        arg_lt,
        arg_gt,
        arg_product_id="",
        arg_metric="median_time",
        arg_across="none",
        arg_req_dynamic_fiters=[],
        arg_across_limit=20,
        arg_ou_id=[],
):
    dynamic_filter_tags_making = {
        "issue_resolved_at": {"$gt": arg_gt, "$lt": arg_lt},
        "product_id": arg_product_id,
        "integration_ids": arg_req_integration_ids,
        "metric": arg_metric
    }

    for eachDynamicFilter in arg_req_dynamic_fiters:
        dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

    payloadDump = {
        "filter": dynamic_filter_tags_making,
        "across": arg_across,
        "across_limit": arg_across_limit,
    }

    if len(arg_ou_id) != 0:
        payloadDump["ou_ids"] = arg_ou_id
        payloadDump["ou_user_filter_designation"] = {"sprint": ["customfield_10020"]}
    payload = json.dumps(payloadDump)

    return payload


def generate_create_issue_time_across_report_drilldown_payload(
        arg_req_integration_ids,
        arg_lt,
        arg_gt,
        arg_key,
        arg_product_id="",
        arg_metric="median_time",
        arg_across="none",
        arg_req_dynamic_fiters=[],
        arg_ou_id=[],
        arg_id="issue_created_at"

):
    dynamic_filter_tags_making = {
        "include_solve_time":True,
        "issue_resolved_at": {"$gt": arg_gt, "$lt": arg_lt},
        "product_id": arg_product_id,
        "integration_ids": arg_req_integration_ids,
        "metric": arg_metric,
    }

    if arg_across == "none":
        dynamic_filter_tags_making["stages"] = arg_key
    elif arg_across == "issue_type":
        dynamic_filter_tags_making["issue_types"] = arg_key




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
    payload = json.dumps(payloadDump)

    return payload