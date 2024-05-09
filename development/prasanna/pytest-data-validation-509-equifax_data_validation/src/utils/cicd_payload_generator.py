from src.lib.generic_helper.generic_helper import TestGenericHelper

generic_object = TestGenericHelper()


def generate_cicd_job_count_payload(
    arg_across="cicd_user_id",
    arg_stacks=None,
    arg_ou_id=(),
    arg_req_dynamic_filters=(),
    arg_req_integration_ids=(),
    arg_sort_xaxis=None,
    arg_ou_exclusions=None,
    arg_end_time=None,
    arg_sort=None
):
    if arg_end_time is None:
        arg_end_time = dict()

    if arg_req_dynamic_filters is None:
        arg_req_dynamic_filters = []

    dynamic_filter_tags_making = {
        "end_time": arg_end_time,
        "integration_ids": arg_req_integration_ids,
        "cicd_integration_ids": arg_req_integration_ids
    }

    if arg_sort_xaxis:
        dynamic_filter_tags_making["sort_xaxis"] = arg_sort_xaxis

    for each_dynamic_filter in arg_req_dynamic_filters:
        dynamic_filter_tags_making[each_dynamic_filter[0]] = each_dynamic_filter[1]

    payload_dump = {
        "filter": dynamic_filter_tags_making,
        "across": arg_across,
        "cicd_integration_ids": arg_req_integration_ids,
    }

    if arg_sort:
        payload_dump["sort"] = arg_sort
    if arg_ou_exclusions:
        payload_dump["ou_exclusions"] = arg_ou_exclusions

    if arg_stacks is not None:
        payload_dump["stacks"] = arg_stacks

    if arg_ou_id:
        payload_dump["ou_ids"] = [arg_ou_id]

    return payload_dump


def generate_cicd_job_runs_values_payload(
    arg_integration_ids, arg_fields, arg_req_dynamic_filters=None
):
    if arg_req_dynamic_filters is None:
        arg_req_dynamic_filters = []

    dynamic_filter_making = dict()
    for each_dynamic_filter in arg_req_dynamic_filters:
        dynamic_filter_making[each_dynamic_filter[0]] = each_dynamic_filter[1]

    payload_dump = {
        "integration_ids": arg_integration_ids,
        "fields": arg_fields,
        "filter": dynamic_filter_making,
    }
    return payload_dump

def generate_cicd_dora_df_payload(
        arg_ou_ids, time_range
):
    payload_dump = {
        "filter": {
            "time_range": time_range
        },
        "ou_ids": arg_ou_ids,
        "stacks": [
            "pipelines"
        ]
    }

    return payload_dump

