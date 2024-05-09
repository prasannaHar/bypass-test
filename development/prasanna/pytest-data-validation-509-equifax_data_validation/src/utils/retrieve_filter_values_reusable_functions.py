import json
import logging
import pandas as pd


from src.utils.generate_widget_drilldown_payloads import WidgetDrillDown
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class Filterresuable:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.widgetreusable = WidgetReusable(self.generic)
        self.drilldown_object =WidgetDrillDown(self.generic)

    def scm_pr_lead_time_generate_req_filter_values(self,
        arg_app_url, 
        arg_req_integration_ids, 
        arg_required_filter, 
        arg_retrieve_only_values=False
        ):
        values_api_url = arg_app_url + "scm/prs/values"
        required_payload = self.drilldown_object.generate_scm_pr_lead_time_filter_values_retriever_payload(
            arg_required_integration_ids=arg_req_integration_ids,
            arg_required_filter_name=arg_required_filter
            )
        LOG.info("===  api response generator ===")
        filter_values_api_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=values_api_url,
            arg_req_payload=required_payload)
        LOG.info("===  retrieve last three records ===")
        temp_records = filter_values_api_response['records'][0][arg_required_filter]
        temp_records_df = pd.DataFrame(temp_records)
        temp_records_df = temp_records_df.sort_values(by=['count'], ascending=False)
        temp_records_df = temp_records_df.head(3)

        if arg_retrieve_only_values:
            return temp_records_df['key'].tolist()
        return temp_records_df.to_dict('records')
    
    
    def jira_lead_time_generate_project_filter_values(self,
        arg_app_url,
        arg_req_integration_ids,
        arg_required_filter,
        arg_retrieve_only_values=False
        ):
        values_api_url = arg_app_url + "jiraprojects/values"
    
        required_payload = self.drilldown_object.generate_jira_lead_time_filter_values_retriever_payload(
            arg_required_integration_ids=arg_req_integration_ids,
            arg_required_filter_name=arg_required_filter
            )
        LOG.info("===  api response generator ===")

        filter_values_api_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=values_api_url,
            arg_req_payload=required_payload)

        temp_records = ((filter_values_api_response['records'])[0])[arg_required_filter]
        temp_records_df = pd.DataFrame(temp_records)
        temp_records_df = temp_records_df.sort_values(by=['key'], ascending=False)
        if arg_retrieve_only_values:
            return temp_records_df['key'].tolist()
        return temp_records_df.to_dict('records')
    
    def ou_retrieve_req_org_user_attrib_filter_values(self,arg_app_url, arg_required_user_attrib, arg_retrieve_only_values=False):

        values_api_url = arg_app_url + "org/users/values"
        payload = {
        "fields": [
            arg_required_user_attrib
        ]
        }

        LOG.info("===  api response generator ===")
        filter_values_api_response = self.widgetreusable.retrieve_required_api_response(
            arg_req_api=values_api_url,
            arg_req_payload=payload)
        LOG.info("===  retrieve last three records ===")
        temp_records = filter_values_api_response['records'][0][arg_required_user_attrib]['records']
        temp_records_df = pd.DataFrame(temp_records)
        temp_records_df = temp_records_df.head(5)

        if arg_retrieve_only_values:
            return temp_records_df['key'].tolist()
        return temp_records_df.to_dict('records')
    
    
    
    
        
    
    
    
