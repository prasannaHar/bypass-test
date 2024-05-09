import logging
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve
import random

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCmPRsReportDataValidation:

    TestParamsObj = ReportTestParametersRetrieve()
    ## report test parametrs for ADO Issues report - tickets end point
    ado_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="sprint_metrics_trend",report_type="ado")
    ado_test_params_p1 = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="sprint_metrics_trend_p1",report_type="ado")

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params)
    def test_ado_sprint_metrics_trend_widget_vs_drilldown_dataconsistency_tickets_sprint(self, 
                                    ado_sprint_report_helper_object, 
                                    reports_test_params, create_generic_object,
                                    ado_custom_field_helper_object):
        """
        ADO Sprint Metrics Trend Report - widget versus drill-down data consistency & filters validation
        Steps:
        1. Call widget API based on the test parameters
        2. Retrieve 5 random records from the widget data >> Call with drill-down API
        3. Verify the data consistency between widget and drill-down
        4. Verify selected filters are applied or not by checking drill-down data
        """
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous_ADO"]
        if len(org_ids) == 0:
            pytest.skip("ADO OUs are not configured for this tenant")
        not_executed_list = []
        not_executed_list_keys_validation = []
        not_executed_list_filters_validation = []
        keys_mismatch_data_ous_list = []
        mismatch_data_ous_list = []
        invalid_data_ous = []
        testcase_exceution_flag = False
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            try:
                ## fetching the required integration ids
                ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                ## widget creation
                ado_sprint_metrics_trend_payload = ado_sprint_report_helper_object.ado_sprint_metrics_trend_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["sprint_metrics_report_ADO"]
                LOG.info("payload---{}".format(ado_sprint_metrics_trend_payload))
                report_resp = create_generic_object.execute_api_call(url, "post", data=ado_sprint_metrics_trend_payload)
                if report_resp["count"] == 0:
                    if ((len(org_ids)-1) == i_org_id) and (not testcase_exceution_flag):
                        pytest.skip("no data present in widget api")
                    else:   continue
                testcase_exceution_flag = True
                ## collecting max of 5 random samples
                random_samples = random.sample(report_resp["records"], min(5,len(report_resp["records"])))
                ## widget v/s drill-down data validation
                for eachrecord in random_samples:
                    key_name = "key"
                    if reports_test_params[0] in ["month", "quarter", "sprint"]:  key_name = "additional_key"
                    ado_sprint_metrics_trend_drilldown_payload = ado_sprint_report_helper_object.ado_sprint_metrics_trend_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=ou_based_integration_ids, ou_id=[org_id], 
                                                    req_key_val=eachrecord[key_name], pagination_flag=True, 
                                                    ado_custom_field_helper_object=ado_custom_field_helper_object)
                    LOG.info("payload drilldown---{}".format(ado_sprint_metrics_trend_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["sprint_metrics_report_ADO"]
                    drilldown_resp = create_generic_object.execute_api_call(url_drilldown, "post", 
                                                                            data=ado_sprint_metrics_trend_drilldown_payload)
                    drilldown_resp_records = drilldown_resp['records'][0]
                    ## widget v/s drill-down data consistency
                    drilldown_count = drilldown_resp_records['total_workitems']
                    widget_count = eachrecord["total_workitems"]
                    if drilldown_count != widget_count:
                        mismatch_data_ous_list.append(org_id)
                    ## keys v/s total_work items data validation
                    execution_flag_keys_validation, keys_data_validation  = ado_sprint_report_helper_object.\
                                        ado_sprint_report_keys_data_validator(
                                                drilldown_response_data=drilldown_resp_records)                    
                    if not execution_flag_keys_validation:
                        not_executed_list_keys_validation.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not keys_data_validation:
                        keys_mismatch_data_ous_list.append(str(org_id) + "_" + str(eachrecord[key_name]))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("keys data validation - not executed OUs List {}".format(set(not_executed_list_keys_validation)))
        LOG.info("drilldown data validation - not executed OUs List {}".format(set(not_executed_list_filters_validation)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("keys data validation failed OUs- list is {}".format(set(keys_mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_keys_validation) == 0, "keys data validation not executed OUs- list is {}".format(set(not_executed_list_keys_validation))
        assert len(not_executed_list_filters_validation) == 0, "not executed OUs- list is {}".format(set(not_executed_list_filters_validation))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(keys_mismatch_data_ous_list) == 0, "keys data validation failed OUs- list is {}".format(set(keys_mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params_p1)
    def test_ado_sprint_metrics_trend_widget_vs_drilldown_dataconsistency_tickets_interval(self, ado_sprint_report_helper_object, 
                                    reports_test_params, create_generic_object, ado_custom_field_helper_object):
        """
        ADO Sprint Metrics Trend Report - widget versus drill-down data consistency & filters validation
        Steps:
        1. Call widget API based on the test parameters
        2. Retrieve 5 random records from the widget data >> Call with drill-down API
        3. Verify the data consistency between widget and drill-down
        4. Verify selected filters are applied or not by checking drill-down data
        """
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous_ADO"]
        if len(org_ids) == 0:
            pytest.skip("ADO OUs are not configured for this tenant")
        not_executed_list = []
        not_executed_list_data_consistency = []
        mismatch_data_ous_list = []
        testcase_exceution_flag = False
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            try:
                ## fetching the required integration ids
                ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                ## widget creation
                ado_sprint_metrics_trend_payload = ado_sprint_report_helper_object.ado_sprint_metrics_trend_payload_generate(
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
                ## drill-down data generation
                ado_sprint_metrics_trend_drilldown_payload = ado_sprint_report_helper_object.ado_sprint_metrics_trend_payload_generate_drilldown(
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id], 
                                                pagination_flag=True,
                                                ado_custom_field_helper_object=ado_custom_field_helper_object,
                                                sprint_report=False, time_range_filter2=True)
                LOG.info("payload drilldown---{}".format(ado_sprint_metrics_trend_drilldown_payload))
                url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["sprint_metrics_report_ADO"]
                drilldown_resp = create_generic_object.execute_api_call(url_drilldown, "post", 
                                                                        data=ado_sprint_metrics_trend_drilldown_payload)
                ## Widget vs drilldown data consistenecy
                execution_flag_dataconsistency, result_flag_data_consistency = ado_sprint_report_helper_object.ado_sprint_trend_widget_vs_drilldown_data_validator(
                                report_resp, drilldown_resp,
                                req_columns=['key', 'additional_key', "integration_id", "sprint_id",
                                         "committed_story_points","commit_delivered_story_points", 
                                         "delivered_story_points","creep_story_points", 
                                         "delivered_creep_story_points", "total_workitems", 
                                         "total_unestimated_workitems" ],
                                sort_column_name=['key', 'additional_key', "integration_id"],
                                unique_id="Sprint Metrics Trend Report"+str(org_id) + reports_test_params[0])
                if not execution_flag_dataconsistency:
                    not_executed_list_data_consistency.append(str(org_id))
                if not result_flag_data_consistency:
                    mismatch_data_ous_list.append(str(org_id))
            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("widget v/s drilldown data consistency - not executed OUs List {}".format(set(not_executed_list_data_consistency)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_data_consistency) == 0, "widget vs drilldown data consistency - not executed OUs- list is {}".format(set(not_executed_list_data_consistency))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
