import requests
import json


def generate_create_scm_pr_response_time_report_widget_payload(
        arg_product_id,
        arg_required_integration_ids,
        arg_req_dynamic_fiters=[],
        arg_ou_id=[],
        arg_across="author",
        arg_code_change_size_config={"small": "23", "medium": "150"},
        arg_comment_density_size_config={"shallow": "1", "good": "5"},
        arg_metric="",
        arg_ou_exclusions="authors"

):
    dynamic_filter_making = {
        "code_change_size_config": arg_code_change_size_config,
        "code_change_size_unit": "files",
        "comment_density_size_config": arg_comment_density_size_config,
        "integration_ids": arg_required_integration_ids,
        "product_id": arg_product_id,

    }
    if len(arg_metric) > 0:
        dynamic_filter_making["metrics"] = arg_metric

    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "across": arg_across,
        "filter": dynamic_filter_making,
    }

    if (len(arg_ou_id) > 0):
        payload_generated["ou_ids"] = arg_ou_id
        # payload_generated["ou_exclusions"] = [arg_ou_exclusions]

    payload = json.dumps(payload_generated)

    return payload


def generate_create_scm_pr_response_time_report_drilldown_payload(
        arg_product_id,
        arg_key,
        arg_required_integration_ids,
        arg_req_dynamic_fiters=[],
        arg_ou_id=[],
        arg_across="author",
        arg_code_change_size_config={"small": "50", "medium": "150"},
        arg_comment_density_size_config={"shallow": "1", "good": "5"},
        arg_ou_exclusions="authors",
        arg_metric="",

):
    dynamic_filter_making = {
        "code_change_size_config": arg_code_change_size_config,
        "code_change_size_unit": "files",
        "comment_density_size_config": arg_comment_density_size_config,
        "integration_ids": arg_required_integration_ids,
        "product_id": arg_product_id,

    }
    if arg_across == "author":
        dynamic_filter_making["authors"] = arg_key
    elif arg_across == "repo_id":
        dynamic_filter_making["repo_ids"] = arg_key

    if len(arg_metric) > 0:
        dynamic_filter_making["metrics"] = arg_metric

    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "across": arg_across,
        "filter": dynamic_filter_making,
        "page": 0,
        "page_size": 10000
    }

    if (len(arg_ou_id) > 0):
        payload_generated["ou_ids"] = arg_ou_id
        payload_generated["ou_exclusions"] = [arg_ou_exclusions]

    payload = json.dumps(payload_generated)

    return payload
