

import pytest

import sys
import os

from utils.widget_reusable_functions import retrieve_required_api_response
sys.path.append('../')


from src.utils.payload_tags_vs_db_columns_mapping import *
from src.lib.core_reusable_functions import *

from src.utils.api_reusable_functions import *
from src.utils.generate_widget_drilldown_payloads import *
from src.utils.widget_reusable_functions import *
from src.utils.retrieve_filter_values_reusable_functions import *
from src.utils.OU_helper import *

from copy import deepcopy

tenant_name = os.getenv('tenant_name')
application_url = os.getenv('application_url')
Authorization_token = os.getenv('Authorization_token')
env_name = os.getenv('env_name')
jira_integration_ids = os.getenv('jira_integration_ids')
jira_issues_table_name = os.getenv('jira_issues_table_name')
github_integration_ids = os.getenv('github_integration_ids')
scm_velocity_config_id = os.getenv('scm_velocity_config_id')
org_prefered_user_based_attribute = os.getenv('org_prefered_user_based_attribute')


widget_api_url = application_url + "velocity"

drilldown_api_url = application_url + 'velocity/values'


def test_issue_hygiene_trend_report_report_creation():


    ## generating widget payload
    widget_payload_generation = generate_scm_pr_lead_time_by_stage_widget_payload(
        arg_required_integration_ids=[github_integration_ids],
        arg_velocity_config_id_to_be_used=scm_velocity_config_id,
        
        )



    ## retrieving the widget response
    widget_response = retrieve_required_api_response(
        arg_authorization_token=Authorization_token,
        arg_req_api=widget_api_url,
        arg_req_payload=widget_payload_generation
        )


    data_check_flag = True

    try:

        api_records = (widget_response['records'])

        assert len(api_records) > 0, "unable to create the report"

        

    except:

        data_check_flag = False


    assert data_check_flag == True, "Unable to create the report"






