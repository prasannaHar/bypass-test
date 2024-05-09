import requests
import json


def generate_scm_rework_widget_payload(
        arg_legacy_interval,
        arg_product_id,
        arg_required_integration_ids,
        arg_req_dynamic_fiters=[],
        arg_assignees=[],
        arg_ou_ids=[],
        arg_across="author",

):
    dynamic_filter_making = {
        "integration_ids": arg_required_integration_ids,
        "legacy_update_interval_config": arg_legacy_interval,
        "product_id": arg_product_id

    }

    # if(len(arg_assignees)>0):
    #
    #     dynamic_filter_making["assignees"] = arg_assignees

    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "filter": dynamic_filter_making,
        "across": arg_across,
    }

    if (len(arg_ou_ids) > 0):
        payload_generated["ou_ids"] = arg_ou_ids

    payload = json.dumps(payload_generated)

    return payload


def generate_scm_rework_drilldown_payload(
        arg_legacy_interval,
        arg_product_id,
        arg_key,
        arg_required_integration_ids,
        arg_req_dynamic_fiters=[],
        arg_ou_ids=[],
        arg_across="author",
        arg_ou_exclusions="authors"
):
    dynamic_filter_making = {
        "authors": arg_key,
        "integration_ids": arg_required_integration_ids,
        "legacy_update_interval_config": arg_legacy_interval,
        "product_id": arg_product_id,
        "include_metrics": False

    }

    # if(len(arg_assignees)>0):
    #
    #     dynamic_filter_making["assignees"] = arg_assignees

    for each_dyn_filter in arg_req_dynamic_fiters:
        dynamic_filter_making[each_dyn_filter[0]] = each_dyn_filter[1]

    payload_generated = {
        "filter": dynamic_filter_making,
        "across": arg_across,
    }

    if len(arg_ou_ids) > 0:
        payload_generated["ou_ids"] = arg_ou_ids
        payload_generated["ou_exclusions"] = [arg_ou_exclusions]

    payload = json.dumps(payload_generated)

    return payload
