import requests
import json


def generate_scm_coding_days_widget_payload(
        arg_gt,
        arg_lt,
        arg_req_dynamic_fiters=[],
        arg_ou_ids=[],
        arg_across="repo_id",
        arg_interval="week",
        arg_id="commit_days",
        arg_sort_xaxis="value_high-low",

):
    dynamic_filter_making = {
        "committed_at": {"$gt": arg_gt, "$lt": arg_lt},
        "sort_xaxis": arg_sort_xaxis

    }

    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "filter": dynamic_filter_making,
        "across": arg_across,
        "interval": arg_interval,
        "sort": [{"id": arg_id, "desc": True}],
        "ou_ids": arg_ou_ids
    }

    # if len(arg_ou_ids) > 0:
    #     payload_generated["ou_ids"] = arg_ou_ids

    # breakpoint()

    # payload = json.dumps(payload_generated)

    return payload_generated


def generate_scm_coding_days_drilldown_payload(
        arg_product_id,
        arg_gt,
        arg_lt,
        arg_key,
        arg_required_integration_ids,
        arg_req_dynamic_fiters=[],
        arg_ou_ids=[],
        arg_across="repo_id",
        arg_id="commit_days",
        arg_ou_exclusions="repo_ids"

):
    dynamic_filter_making = {
        "repo_ids": arg_key,
        "integration_ids": arg_required_integration_ids,
        "committed_at": {"$gt": arg_gt, "$lt": arg_lt},
        "product_id": arg_product_id,
        "include_metrics": False,
        "sort": [{"id": arg_id, "desc": True}]

    }

    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "filter": dynamic_filter_making,
        "across": arg_across,
        "page": 0,
        "page_size": 10
    }

    if len(arg_ou_ids) > 0:
        payload_generated["ou_ids"] = arg_ou_ids
        payload_generated["ou_exclusions"] = [arg_ou_exclusions]

    payload = json.dumps(payload_generated)

    return payload
