import logging
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCmPRsReportDataValidation:

    TestParamsObj = ReportTestParametersRetrieve()
    ## report test parametrs for ADO Issues report - tickets end point
    ado_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="sprint_metrics_single_stat",report_type="ado")

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params)
    def test_ado_sprint_metrics_single_stat_widget_data_validation(self, ado_sprint_report_helper_object, 
                                    reports_test_params, create_generic_object, ado_custom_field_helper_object):
        """
        ADO Sprint Metrics Single Stat Report - widget versus drill-down data consistency & filters validation
        Steps:
        1. Call widget API based on the test parameters
        2. make sure widget api is getting called without any issues
        """
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous_ADO"]
        if len(org_ids) == 0:
            pytest.skip("ADO OUs are not configured for this tenant")
        not_executed_list = []
        testcase_exceution_flag = False
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            try:
                ## fetching the required integration ids
                ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                ## widget creation
                ado_sprint_metrics_trend_payload = ado_sprint_report_helper_object.ado_sprint_metrics_single_stat_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                ado_custom_field_helper_object=ado_custom_field_helper_object,
                                                time_range_filter2=True)
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["sprint_metrics_report_ADO"]
                LOG.info("payload---{}".format(ado_sprint_metrics_trend_payload))
                report_resp = create_generic_object.execute_api_call(url, "post", data=ado_sprint_metrics_trend_payload)
                if report_resp["count"] == 0:
                    if ((len(org_ids)-1) == i_org_id) and (not testcase_exceution_flag):
                        pytest.skip("no data present in widget api")
                    else:   continue
                testcase_exceution_flag = True
            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
