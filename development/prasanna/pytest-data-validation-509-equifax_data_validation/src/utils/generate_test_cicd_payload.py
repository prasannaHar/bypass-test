import json
import os
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

generic_object = TGhelper()

cicd_integration_id = os.getenv('cicd_integration_id')


def generate_create_cicd_job_count_single_stat_payload(
        arg_project_id,
        arg_across="trend",
        arg_req_dynamic_fiters=[],
        arg_ou_id=generic_object.get_org_unit_id(org_unit_name=generic_object.env["org_unit_name"]),
        arg_req_integration_ids=[],
        arg_job_start_date=1

):
    dynamic_filter_tags_making = {
        "agg_type": "total",
        "time_period": arg_job_start_date,
        "product_id": arg_project_id,
        "integration_ids": arg_req_integration_ids,
        "cicd_integration_ids": arg_req_integration_ids
    }
    for eachDynamicFilter in arg_req_dynamic_fiters:
        dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

    payloadDump = {
        "filter": dynamic_filter_tags_making,
        "cicd_integration_ids": arg_req_integration_ids,
        "across": arg_across,

    }

    if len(arg_ou_id) != 0:
        payloadDump["ou_ids"] = arg_ou_id

    return payloadDump


def generate_create_cicd_job_count_widget_payload(
        arg_across="cicd_user_id",
        arg_req_dynamic_fiters=[],
        arg_ou_id=generic_object.get_org_unit_id(org_unit_name=generic_object.env["org_unit_name"]),
        arg_req_integration_ids=[],
        arg_id="count",
        arg_sort_xaxis="value_high-low",
        arg_endTime={}

):
    dynamic_filter_tags_making = {
        "end_time": arg_endTime,
        "integration_ids": arg_req_integration_ids,
        "cicd_integration_ids": arg_req_integration_ids,
        "sort_xaxis":arg_sort_xaxis
    }
    for eachDynamicFilter in arg_req_dynamic_fiters:
        dynamic_filter_tags_making[eachDynamicFilter[0]] = eachDynamicFilter[1]

    payloadDump = {
        "filter": dynamic_filter_tags_making,
        "across": arg_across,
        "sort": [
            {
                "id": arg_id,
                "desc": True
            }
        ]

    }

    if len(arg_ou_id) != 0:
        payloadDump["ou_ids"] = arg_ou_id
    return payloadDump
