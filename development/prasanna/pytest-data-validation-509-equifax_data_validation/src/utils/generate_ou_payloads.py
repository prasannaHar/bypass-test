## inprogress file need to finalise the changes

import json


def generate_ou_payload_filter_based(
        arg_req_ou_name,
        arg_req_integration_ids,
        arg_projects_filter_values=[],
        arg_components_filter_values=[],
        arg_required_integration_type="jira",

):
    payload_data = [
        {
            "name": arg_req_ou_name,
            "sections": [
                {
                    "id": "f3bfbe10-c475-11ec-93b6-31de598c412a",
                    "integrations": {
                        arg_req_integration_ids: {
                            "type": arg_required_integration_type,
                            "filters": {
                                "projects": arg_projects_filter_values,
                                "components": arg_components_filter_values
                            }
                        }
                    }
                }
            ]
        }
    ]

    payload = json.dumps(payload_data)

    return payload


# ["ITOPS","LEV","LFE"]

# ["ui-levelops","LevelOps Issues","commons-levelops","automation"]

def generate_create_ou_payload_people_based(
        arg_req_ou_name,
        arg_req_integration_ids,
        arg_users,
        arg_required_integration_type="jira",

):
    payload_data = [
        {
            "name": arg_req_ou_name,
            "sections": [
                {
                    "id": "8cdc10e0-da64-11ec-9e7a-9d78d78a5a11",
                    "integrations": {
                        arg_req_integration_ids: {
                            "type": arg_required_integration_type,
                        }
                    },
                    "users": arg_users

                }
            ]
        }
    ]

    payload = json.dumps(payload_data)

    return payload


def generate_get_ou_manager_users(fields):
    payload_data = {
        "fields": fields
    }
    return payload_data


def generate_delete_OU_payload(arg_OU_id):
    payload_data = arg_OU_id
    payload = json.dumps(payload_data)
    return payload


def generate_dynamic_ou_payload_filter_based(
        arg_required_ou_name,
        arg_required_integration_type="github",
        arg_required_filters_key_value_pair={}, category_id=None, dashboardid=None
):
    required_integration_id_details = {
        "type": arg_required_integration_type,
    }

    if len(arg_required_filters_key_value_pair) > 0:
        required_integration_id_details["filters"] = arg_required_filters_key_value_pair

    payload_req = [{
        "name": arg_required_ou_name,
        "ou_category_id": category_id,
        "default_dashboard_id": 0, "parent_ref_id": dashboardid}]
    #     "sections": [
    #         {
    #             "id": "cef00020-dd9a-11ec-9cda-bf0b89bd3364",
    #             "integrations": {
    #                 arg_required_integration_id: required_integration_id_details
    #             }
    #         }
    #     ]
    # }
    return payload_req


def generate_dynamic_ou_payload_user_based(
        arg_required_ou_name,
        arg_required_integration_id,
        arg_required_user_atrribute_filters_key_value_pair={},
        arg_required_integration_type="github",
        arg_required_filters_key_value_pair={},
):
    required_integration_id_details = {
        "type": arg_required_integration_type,
    }

    if len(arg_required_filters_key_value_pair) > 0:
        required_integration_id_details["filters"] = arg_required_filters_key_value_pair

    payload_req = {
        "name": arg_required_ou_name,

        # "sections": [
        # {
        #     "id": "cef00020-dd9a-11ec-9cda-bf0b89bd3364",
        #     "integrations": {
        #         arg_required_integration_id: required_integration_id_details
        #         }
        # }
        # ]
    }

    if len(arg_required_user_atrribute_filters_key_value_pair) > 0:
        payload_req["default_section"] = {
            "dynamic_user_definition": arg_required_user_atrribute_filters_key_value_pair
        }

    required_payload = json.dumps([payload_req])
    return required_payload


def generate_create_ou_payload_interation_basic(
        arg_req_ou_name,
        ou_category_id,
        parent_ref_id):
    payload_data = [
        {
            "name": arg_req_ou_name,
            "ou_category_id": ou_category_id,
            "parent_ref_id": str(parent_ref_id)
        }
    ]
    return payload_data
