import pytest
import os
import shutil
from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.testcases.CICD.datastore.factory.factory_base import FactoryBase
from src.utils.widget_schemas import Schemas
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve
from src.utils.cicd_job_run_helper import CICDJobRunHelper
from src.utils.api_reusable_functions import ApiReusableFunctions
from src.testcases.mapping_library.mapping_library_helper import Mapping_library_helper


class CICDEnvSetup:
    def __init__(self):
        self.generic_obj = TestGenericHelper()
        self.org_ids = self.generic_obj.env.get("set_ous_cicd")
        self.CICD_setup()
        self.api_helper_obj = ApiReusableFunctions(self.generic_obj)
        self.cicd_job_run_helper = CICDJobRunHelper(
            self.generic_obj
        )
        self.disable_datastore_check = self.generic_obj.env.get(
            "CICD_DISABLE_DATASTORE_CHECK", {})
        # TODO: Below fields require to execute the Pipeline detail API call to fetch its details.
        # Hence, considering it as a P2 test case. Only Count check is enabled for these parameters
        # for CICD Job count -> Drilldown.
        self.filters_to_skip_for_datastore_check = ["stage_name", "stage_status", "parameters",
                          "step_name", "step_status", "parameters", "infrastructures", "deployment_types",
                          "services", "environments"]

    def CICD_setup(self):

        # Helper variable to execute the test cases for a specific product
        # if required.
        # If user exports a variable `export CICD_PRODUCT=CICD_JENKINS`, then
        # the test cases will be executed for only Jenkins product.
        # By default, the test cases will execute for Harness product.
        self.product_name = self.generic_obj.env.get(
            "cicd_product", ["CICD_HARNESS"]
        )
        self.integration_details = {}
        self.cicd_filters = dict()
        self.product_application_names = set([])
        self.prepare_integration_details()

    def prepare_integration_details(self):
        report_obj = ReportTestParametersRetrieve()

        # Iterating over Org units to collect integration details.
        for org_id in self.org_ids:
            self.integration_details[org_id] = {}

            # Iterating over integration details to fetch the integration IDs
            # and test scenarios related to the integration.
            for application in self.product_name:
                report_names = set([])
                application = application.strip()

                product_application_name = self.generic_obj.api_data[
                    application]["INTEGRATION_NAME"]
                self.product_application_names.add(product_application_name)

                report_names.update(self.generic_obj.api_data[
                    application]["CICD_REPORT_NAME"])

                failed_status_values = self.generic_obj.api_data[
                    application]["FAILED_STATUS_VALUES"]

                # Fields which are either not available through API or its value
                # is generated internally by SEI can be added in this list.
                # Such fields will be skipped from datastore check.
                skip_fields_from_datastore_check = self.generic_obj.api_data[
                    application]["SKIP_FIELDS_FROM_DATASTORE_CHECK"]

                # Fetching the Integration ID attached with the Org Unit.
                integration_ids = self.generic_obj.get_integrations_based_on_ou_id(
                    ou_id = org_id, application_name=product_application_name
                )
                if integration_ids:
                    self.integration_details[org_id].update({
                        product_application_name: integration_ids,
                        "failed_status_values": failed_status_values,
                        "skip_fields_from_datastore_check": skip_fields_from_datastore_check
                    })
                else:
                    continue

                # Iterating over the test scenarios specific to integration.
                for report_name in report_names:
                    tc_scenarios_file_type = "cicd_" + product_application_name
                    if not self.cicd_filters.get(report_name):
                        self.cicd_filters[report_name] = {
                            "filter_combinations": []
                        }
                    filter_combinations = report_obj.retrieve_widget_test_parameters(
                        report_name=report_name, report_type=tc_scenarios_file_type,
                        application_name=product_application_name
                    )
                    self.cicd_filters[report_name]["filter_combinations"].extend(filter_combinations)
                    if report_name == "job_count":
                        if not self.cicd_filters[report_name].get("x_axis_field_names"):
                            self.cicd_filters[report_name]["x_axis_field_names"] = []
                        # Filters differ based on the products.
                        # Parsing the CSV file and fetching the values of x-axis filter
                        # based on the 'report' and 'filter' columns for CICD job count widget.
                        self.cicd_filters[report_name]["x_axis_field_names"].extend(
                            report_obj.retrieve_report_filter_values(
                                report_name = report_name,
                                report_type = tc_scenarios_file_type,
                                field_name = "x-axis",
                                application_name=product_application_name
                            )
                        )

class CICDJobCount(CICDEnvSetup):
    def __init__(self):
        super().__init__()

class CICDDoraDF(CICDEnvSetup):
    def __init__(self):
        super().__init__()
        self.mapper_lib = Mapping_library_helper(self.generic_obj)
        self.dora_df_widget_url = self.generic_obj.connection["base_url"] + \
            self.generic_obj.api_data["dora_deployment_frequency"]
        self.dora_df_widget_drilldown_url = self.generic_obj.connection["base_url"] + \
            self.generic_obj.api_data["dora_drilldown_list_api"]

class CICDDoraCFR(CICDEnvSetup):
    def __init__(self):
        super().__init__()
        self.mapper_lib = Mapping_library_helper(self.generic_obj)
        self.dora_cfr_widget_url = self.generic_obj.connection["base_url"] + \
            self.generic_obj.api_data["dora_change_failure_rate"]
        self.dora_df_widget_drilldown_url = self.generic_obj.connection["base_url"] + \
            self.generic_obj.api_data["dora_drilldown_list_api"]


@pytest.fixture(scope="session", autouse=True)
def widget_schema_validation():
    widget_schema_obj = Schemas()
    return widget_schema_obj

@pytest.fixture(scope="session")
def datastore_prep(request):

    # Define a finalizer to delete the temporary directory and its contents
    def delete_temp_directory(datastore_paths):
        for path in datastore_paths.values():
            datastore_dir = os.path.dirname(path["datastore"])
            shutil.rmtree(datastore_dir)
            break

    # Executing the datastore preparation step.
    datastore = FactoryBase()
    datastore_paths = datastore.download_datastore()

    request.addfinalizer(lambda: delete_temp_directory(datastore_paths))

    return datastore_paths
