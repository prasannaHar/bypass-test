import logging
import pytest
import random
import pandas as pd

from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueResolutionTimeReportDataValidation:

    TestParamsObj = ReportTestParametersRetrieve()
    ## report test parametrs for ADO Issues report - tickets end point
    ado_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="issue_resolution_time_report",report_type="ado")    

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params)
    def test_ado_issue_resolution_time_report_widget_vs_drilldown_dataconsistency(self, ado_widget_helper_object, 
                                    reports_test_params, create_generic_object, api_reusable_functions_object,
                                    ado_custom_field_helper_object):
        """
        ADO Issues Report - widget versus drill-down data consistency & filters validation
        Steps:
        1. Call widget API based on the test parameters
        2. Verify the data consistency between total ticket count and stacks tickets count
        3. Retrieve 5 random records from the widget data >> Call with drill-down API
        4. Verify the data consistency between widget and drill-down
        5. Verify selected filters are applied or not by checking drill-down data
        """
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous_ADO"]
        if len(org_ids) == 0:
            pytest.skip("ADO OUs are not configured for this tenant")
        not_executed_list = []
        not_executed_list_filters_validation = []
        mismatch_data_ous_list = []
        invalid_data_ous = []
        testcase_exceution_flag = False
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            try:
                ## fetching the required integration ids
                ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                ## widget creation
                ado_issue_resolution_time_report_payload = ado_widget_helper_object.ado_issue_resolution_time_report_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["resolution_time_report_ADO"]
                LOG.info("payload---{}".format(ado_issue_resolution_time_report_payload))
                report_resp = create_generic_object.execute_api_call(url, "post", data=ado_issue_resolution_time_report_payload)
                ## parsing the widget response data
                report_response_dict = report_resp["records"][0]
                dictkeys = [*report_response_dict]
                report_resp = report_response_dict[dictkeys[0]]
                if report_resp["total_count"] == 0:
                    if ((len(org_ids)-1) == i_org_id) and (not testcase_exceution_flag):
                        pytest.skip("no data present in widget api")
                    else:   continue
                testcase_exceution_flag = True
                ## collecting max of 5 random samples
                random_samples = random.sample(report_resp["records"], min(5,len(report_resp["records"])))
                ## widget v/s drill-down data validation
                for eachrecord in random_samples:
                    key_name = "key"
                    if reports_test_params[1] in ["month", "quarter"]:  key_name = "additional_key"
                    ado_issue_resolution_time_report_drilldown_payload = ado_widget_helper_object.ado_issue_resolution_time_report_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=ou_based_integration_ids, ou_id=[org_id], 
                                                    req_key_val=eachrecord[key_name], pagination_flag=True, 
                                                    ado_custom_field_helper_object=ado_custom_field_helper_object)
                    LOG.info("payload drilldown---{}".format(ado_issue_resolution_time_report_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["workitems_list"]
                    drilldown_resp = create_generic_object.execute_api_call(url_drilldown, "post", 
                                                                            data=ado_issue_resolution_time_report_drilldown_payload)
                    ## widget v/s drill-down data consistency
                    drilldown_count = (drilldown_resp["_metadata"])["total_count"]
                    widget_count = eachrecord["total_tickets"]
                    if drilldown_count != widget_count:
                        mismatch_data_ous_list.append(org_id)
                    ## filters validation
                    drilldown_resp_df = pd.json_normalize(drilldown_resp['records'], max_level=1)
                    execution_flag_filters_validation, data_validation  = ado_widget_helper_object.\
                                        ado_issues_report_drilldown_data_validator(
                                                response_data_df=drilldown_resp_df, 
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                api_reusable_functions_object=api_reusable_functions_object,
                                                ado_custom_field_helper_object=ado_custom_field_helper_object,
                                                filter_indexes=[3,4])
                    if not execution_flag_filters_validation:
                        not_executed_list_filters_validation.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord[key_name]))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("drilldown data validation - not executed OUs List {}".format(set(not_executed_list_filters_validation)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_filters_validation) == 0, "not executed OUs- list is {}".format(set(not_executed_list_filters_validation))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))

