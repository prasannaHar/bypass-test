import logging
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueHygieneReportDataValidation:

    TestParamsObj = ReportTestParametersRetrieve()
    ## report test parametrs for ADO Issues report - tickets end point
    ado_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="issue_hygiene_report",report_type="ado")
    ## sanity test case addition - tickets end point
    ado_test_params.append(
        pytest.param(('project', 'none', 'none', 'workitem_created_at', 'none', 'workitem_projects', 'none'), 
        marks=pytest.mark.sanity))

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params)
    def test_ado_issues_hygiene_report_widget_vs_drilldown(self, ado_widget_helper_object, 
                        reports_test_params, create_generic_object, ado_custom_field_helper_object):
        """
        ADO Issues Hygiene Report - widget versus drill-down data consistency & filters validation
        Steps:
        1. Call widget API based on the test parameters
        2. Call drill-down API based on the test parameters
        3. Verify the data consistency between widget and drill-down
        """
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous_ADO"]
        hygiene_types = create_generic_object.api_data["hygiene_categories"]
        if len(org_ids) == 0:
            pytest.skip("ADO OUs are not configured for this tenant")
        not_executed_list = []
        mismatch_data_ous_list = []
        testcase_exceution_flag = False
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            for each_hygiene in hygiene_types:
                try:
                    ## fetching the required integration ids
                    ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                    ## widget creation
                    ado_issue_hygiene_report_payload = ado_widget_helper_object.ado_issues_report_payload_generate(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                    ado_custom_field_helper_object=ado_custom_field_helper_object, 
                                                    hygiene=each_hygiene)
                    url = create_generic_object.connection["base_url"] + create_generic_object.api_data["ADO_tickets_report"]
                    LOG.info("payload---{}".format(ado_issue_hygiene_report_payload))
                    report_resp = create_generic_object.execute_api_call(url, "post", data=ado_issue_hygiene_report_payload)
                    ## parsing the widget response data
                    report_response_dict = report_resp["records"][0]
                    dictkeys = [*report_response_dict]
                    report_resp = report_response_dict[dictkeys[0]]
                    if report_resp["total_count"] == 0:
                        if ((len(org_ids)-1) == i_org_id) and (not testcase_exceution_flag):
                            pytest.skip("no data present in widget api")
                        else:   continue
                    testcase_exceution_flag = True
                    ## retrieve drill-down data
                    ado_issue_hygiene_report_drilldown_payload = ado_widget_helper_object.ado_issues_report_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=ou_based_integration_ids, ou_id=[org_id], 
                                                    req_key_val="", pagination_flag=True, 
                                                    ado_custom_field_helper_object=ado_custom_field_helper_object, 
                                                    hygiene=each_hygiene)
                    LOG.info("payload drilldown---{}".format(ado_issue_hygiene_report_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["workitems_list"]
                    drilldown_resp = create_generic_object.execute_api_call(url_drilldown, "post", 
                                                                            data=ado_issue_hygiene_report_drilldown_payload)
                    ## widget v/s drill-down data consistency
                    drilldown_count = (drilldown_resp["_metadata"])["total_count"]
                    ## retrieve widget count
                    widget_data_df = pd.json_normalize(report_response_dict["project"]["records"])
                    widget_count = widget_data_df["total_tickets"].sum()
                    if drilldown_count != widget_count:
                        mismatch_data_ous_list.append(org_id)

                except Exception as ex: 
                    LOG.info("exeception occured {}".format(ex))
                    not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))

