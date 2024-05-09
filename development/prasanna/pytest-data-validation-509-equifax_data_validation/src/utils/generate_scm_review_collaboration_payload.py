import requests
import json


def generate_create_scm_review_collaboration_widget_payload(
        arg_product_id,
        arg_required_integration_ids,
        arg_req_dynamic_fiters=[],
        arg_ou_ids=[],
        arg_missing_pr_merged=True


):
    dynamic_filter_making = {
        "integration_ids": arg_required_integration_ids,
        "product_id": arg_product_id,
        "missing_fields": {"pr_merged": arg_missing_pr_merged}

    }


    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "filter": dynamic_filter_making,
    }

    if (len(arg_ou_ids) > 0):
        payload_generated["ou_ids"] = arg_ou_ids

    payload = json.dumps(payload_generated)

    return payload


def generate_create_scm_review_collaboration_drilldown_payload(
        arg_product_id,
        arg_key,
        arg_required_integration_ids,
        arg_req_dynamic_fiters=[],
        arg_ou_ids=[],
        arg_missing_pr_merged=True,
        arg_ou_exclusions="creators"


):
    dynamic_filter_making = {
        "creators":arg_key,
        "integration_ids": arg_required_integration_ids,
        "product_id": arg_product_id,
        "missing_fields": {"pr_merged": arg_missing_pr_merged}

    }


    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "filter": dynamic_filter_making,
        "page": 0,
        "page_size": 1000
    }

    if (len(arg_ou_ids) > 0):
        payload_generated["ou_ids"] = arg_ou_ids
        payload_generated["ou_exclusions"] = [arg_ou_exclusions]

    payload = json.dumps(payload_generated)

    return payload