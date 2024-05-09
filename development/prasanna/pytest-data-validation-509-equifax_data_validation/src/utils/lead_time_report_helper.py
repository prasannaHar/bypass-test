import calendar
import time
from src.utils.project_helper import project_helper

class Lead_time_Helper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.project_obj = project_helper(generic_helper)


    def create_project_and_dashboard_for_issue_time(self):
        ts = calendar.timegm(time.gmtime())
        project_name = "automation-project-lead_time" + str(ts)
        create_project_response = self.project_obj.create_project_without_integration(arg_project_name=project_name)
        integration_id = self.generic.get_integration_id()
        product_id = create_project_response['id']
        self.project_obj.add_integration(integration_id, product_id)
        # dashboard_name = "automation-dashboard" + str(ts)

        # get_create_dashboard_response = create_dashboard(arg_dashboard_name=dashboard_name, arg_product_id=product_id)
        # dashboard_id = get_create_dashboard_response["id"]

        return create_project_response
