import logging
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve
import random
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCmPRsReportDataValidation:

    TestParamsObj = ReportTestParametersRetrieve()
    ## report test parametrs for ADO Issues report - tickets end point
    ado_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="issues_report",report_type="ado")
    ## sanity test case addition - tickets end point
    ado_test_params.append(
        pytest.param(('assignee', 'none', 'ticket', 'workitem_created_at', 'none', 'workitem_projects', 'none'), 
        marks=pytest.mark.sanity))
    ## report test parametrs for ADO Issues report - stort points end point
    ado_test_params_story_points = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="issues_report_story",report_type="ado")
    ## report test parametrs for ADO Issues report - effort end point
    ado_test_params_effort = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="issues_report_effort",report_type="ado")

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params)
    def test_ado_issues_report_widget_vs_drilldown_dataconsistency_tickets(self, ado_widget_helper_object, 
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
        not_executed_list_stacks_validation = []
        invalid_stacks_data_ous = []
        testcase_exceution_flag = False
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            try:
                ## fetching the required integration ids
                ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                ## widget creation
                ado_issues_report_payload = ado_widget_helper_object.ado_issues_report_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["ADO_tickets_report"]
                LOG.info("payload---{}".format(ado_issues_report_payload))
                report_resp = create_generic_object.execute_api_call(url, "post", data=ado_issues_report_payload)
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
                ## widget data validation - stacks information
                stack = reports_test_params[len(reports_test_params)-1]
                stacks_to_be_skipped = create_generic_object.api_data["ado_stacks_validations_to_be_skipped"]
                if (len(report_resp["records"]) > 0) and ( stack not in stacks_to_be_skipped ) and (reports_test_params[0] != stack):
                    execution_flag_stacks_validation, stacks_data_validation  = ado_widget_helper_object.ado_issues_report_stacks_data_validator(
                                                response_data=random_samples, req_metric="total_tickets")
                    if not execution_flag_stacks_validation:
                        not_executed_list_stacks_validation.append(str(org_id) + "_" + str(stack))
                    if not stacks_data_validation:
                        invalid_stacks_data_ous.append(str(org_id) + "_" + str(stack))
                ## widget v/s drill-down data validation
                for eachrecord in random_samples:
                    key_name = "key"
                    if reports_test_params[1] in ["month", "quarter"]:  key_name = "additional_key"
                    ado_issues_report_drilldown_payload = ado_widget_helper_object.ado_issues_report_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=ou_based_integration_ids, ou_id=[org_id], 
                                                    req_key_val=eachrecord[key_name], pagination_flag=True, 
                                                    ado_custom_field_helper_object=ado_custom_field_helper_object)
                    LOG.info("payload drilldown---{}".format(ado_issues_report_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["workitems_list"]
                    drilldown_resp = create_generic_object.execute_api_call(url_drilldown, "post", 
                                                                            data=ado_issues_report_drilldown_payload)
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
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                    if not execution_flag_filters_validation:
                        not_executed_list_filters_validation.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord[key_name]))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("drilldown data validation - not executed OUs List {}".format(set(not_executed_list_filters_validation)))
        LOG.info("stacks data validation not executed OUs List {}".format(set(not_executed_list_stacks_validation)))
        LOG.info("stacks data validation failed OUs- list is {}".format(set(invalid_stacks_data_ous)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_filters_validation) == 0, "not executed OUs- list is {}".format(set(not_executed_list_filters_validation))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))
        assert len(not_executed_list_stacks_validation) == 0, "stacks data validation not executed OUs List- list is {}".format(set(not_executed_list))
        assert len(invalid_stacks_data_ous) == 0, "stacks data validation failed OUs- list is {}".format(set(invalid_data_ous))

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params_story_points)
    def test_ado_issues_report_widget_vs_drilldown_dataconsistency_story_points(self, ado_widget_helper_object, 
                                            reports_test_params, create_generic_object, create_widget_helper_object,
                                            api_reusable_functions_object,ado_custom_field_helper_object):
        """
        ADO Issues Report - widget versus drill-down data consistency & filters validation
        Steps:
        1. Call widget API based on the test parameters
        2. Verify the data consistency between total story points and stacks story points
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
        not_executed_list_data_consistency = []
        not_executed_list_filters_validation = []
        mismatch_data_ous_list = []
        invalid_data_ous = []
        testcase_exceution_flag = False
        not_executed_list_stacks_validation = []
        invalid_stacks_data_ous = []
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            try:
                ## fetching the required integration ids
                ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                ## widget creation
                ado_issues_report_payload = ado_widget_helper_object.ado_issues_report_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["ado_im_story_point_report"]
                LOG.info("payload---{}".format(ado_issues_report_payload))
                report_resp = create_generic_object.execute_api_call(url, "post", data=ado_issues_report_payload)
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
                ## widget data validation - stacks information
                stack = reports_test_params[len(reports_test_params)-1]
                stacks_to_be_skipped = create_generic_object.api_data["ado_stacks_validations_to_be_skipped"]
                if (len(report_resp["records"]) > 0) and ( stack not in stacks_to_be_skipped ) and (reports_test_params[0] != stack):
                    execution_flag_stacks_validation, stacks_data_validation  = ado_widget_helper_object.ado_issues_report_stacks_data_validator(
                                                response_data=random_samples, req_metric="total_story_points")
                    if not execution_flag_stacks_validation:
                        not_executed_list_stacks_validation.append(str(org_id) + "_" + str(stack))
                    if not stacks_data_validation:
                        invalid_stacks_data_ous.append(str(org_id) + "_" + str(stack))
                ## widget v/s drill-down data validation
                for eachrecord in random_samples:
                    key_name = "key"
                    if reports_test_params[1] in ["month", "quarter"]:  key_name = "additional_key"
                    ado_issues_report_drilldown_payload = ado_widget_helper_object.ado_issues_report_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=ou_based_integration_ids, ou_id=[org_id], 
                                                    req_key_val=eachrecord[key_name],
                                                    ado_custom_field_helper_object=ado_custom_field_helper_object)
                    LOG.info("payload drilldown---{}".format(ado_issues_report_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["workitems_list"]
                    drilldown_resp_df = create_widget_helper_object.generate_paginated_drilldown_data(
                                                                url=url_drilldown, 
                                                                payload=ado_issues_report_drilldown_payload)
                    widget_count = eachrecord["total_story_points"]
                    ## widget v/s drill-down data consistency
                    execution_flag_consistency, data_consistency = ado_widget_helper_object.\
                        ado_issues_report_widget_versus_drilldown_data_consistency_check(
                                    widget_val=widget_count,drilldown_resp_df = drilldown_resp_df, 
                                    req_column="story_point")
                    if not execution_flag_consistency:
                        not_executed_list_data_consistency.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not data_consistency:
                        mismatch_data_ous_list.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    ## filters validation
                    execution_flag_filters_validation, data_validation  = ado_widget_helper_object.\
                                        ado_issues_report_drilldown_data_validator(
                                                response_data_df=drilldown_resp_df, 
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                api_reusable_functions_object=api_reusable_functions_object,
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                    if not execution_flag_filters_validation:
                        not_executed_list_filters_validation.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord[key_name]))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("stacks data validation not executed OUs List {}".format(set(not_executed_list_stacks_validation)))
        LOG.info("stacks data validation failed OUs- list is {}".format(set(invalid_stacks_data_ous)))
        LOG.info("data consistency not executed OUs List {}".format(set(not_executed_list_data_consistency)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("filters validation not executed OUs List {}".format(set(not_executed_list_filters_validation)))
        LOG.info("filters data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_stacks_validation) == 0, "stacks data validation not executed OUs List- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_data_consistency) == 0, "data consistency not executed OUs- list is {}".format(set(not_executed_list_data_consistency))
        assert len(not_executed_list_filters_validation) == 0, "filters validation not executed OUs List- list is {}".format(set(not_executed_list_filters_validation))
        assert len(invalid_stacks_data_ous) == 0, "stacks data validation failed OUs- list is {}".format(set(invalid_data_ous))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "filters data validation failed OUs- list is {}".format(set(invalid_data_ous))

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", ado_test_params_effort)
    def test_ado_issues_report_widget_vs_drilldown_dataconsistency_effort(self, ado_widget_helper_object, 
                                            reports_test_params, create_generic_object, create_widget_helper_object,
                                            api_reusable_functions_object, ado_custom_field_helper_object):        
        """
        ADO Issues Report - widget versus drill-down data consistency & filters validation
        Steps:
        1. Call widget API based on the test parameters
        2. Verify the data consistency between total effort and stacks effort
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
        not_executed_list_data_consistency = []
        not_executed_list_filters_validation = []
        mismatch_data_ous_list = []
        invalid_data_ous = []
        testcase_exceution_flag = False
        not_executed_list_stacks_validation = []
        invalid_stacks_data_ous = []
        for i_org_id in range(len(org_ids)):
            org_id = org_ids[i_org_id]
            try:
                ## fetching the required integration ids
                ou_based_integration_ids = create_generic_object.get_integrations_based_on_ou_id(org_id)
                ## widget creation
                ado_issues_report_payload = ado_widget_helper_object.ado_issues_report_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids, ou_id=[org_id],
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["ado_im_effort_report"]
                LOG.info("payload---{}".format(ado_issues_report_payload))
                report_resp = create_generic_object.execute_api_call(url, "post", data=ado_issues_report_payload)
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
                ## widget data validation - stacks information
                stack = reports_test_params[len(reports_test_params)-1]
                stacks_to_be_skipped = create_generic_object.api_data["ado_stacks_validations_to_be_skipped"]
                if (len(report_resp["records"]) > 0) and ( stack not in stacks_to_be_skipped ) and (reports_test_params[0] != stack):
                    execution_flag_stacks_validation, stacks_data_validation  = ado_widget_helper_object.ado_issues_report_stacks_data_validator(
                                                response_data=random_samples, req_metric="total_effort")
                    if not execution_flag_stacks_validation:
                        not_executed_list_stacks_validation.append(str(org_id) + "_" + str(stack))
                    if not stacks_data_validation:
                        invalid_stacks_data_ous.append(str(org_id) + "_" + str(stack))
                ## widget v/s drill-down data validation
                for eachrecord in random_samples:
                    key_name = "key"
                    if reports_test_params[1] in ["month", "quarter"]:  key_name = "additional_key"
                    ado_issues_report_drilldown_payload = ado_widget_helper_object.ado_issues_report_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=ou_based_integration_ids, ou_id=[org_id], 
                                                    req_key_val=eachrecord[key_name],
                                                    ado_custom_field_helper_object=ado_custom_field_helper_object)
                    LOG.info("payload drilldown---{}".format(ado_issues_report_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["workitems_list"]
                    drilldown_resp_df = create_widget_helper_object.generate_paginated_drilldown_data(
                                                                url=url_drilldown, 
                                                                payload=ado_issues_report_drilldown_payload)
                    widget_count = eachrecord["total_effort"]
                    ## widget v/s drill-down data consistency
                    execution_flag_consistency, data_consistency = ado_widget_helper_object.\
                        ado_issues_report_widget_versus_drilldown_data_consistency_check(
                                    widget_val=widget_count,drilldown_resp_df = drilldown_resp_df, 
                                    req_column="attributes.effort")
                    if not execution_flag_consistency:
                        not_executed_list_data_consistency.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not data_consistency:
                        mismatch_data_ous_list.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    ## filters validation
                    execution_flag_filters_validation, data_validation  = ado_widget_helper_object.\
                                        ado_issues_report_drilldown_data_validator(
                                                response_data_df=drilldown_resp_df, 
                                                required_test_params=reports_test_params,
                                                integration_ids=ou_based_integration_ids,ou_id=[org_id],
                                                api_reusable_functions_object=api_reusable_functions_object,
                                                ado_custom_field_helper_object=ado_custom_field_helper_object)
                    if not execution_flag_filters_validation:
                        not_executed_list_filters_validation.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord[key_name]))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("stacks data validation not executed OUs List {}".format(set(not_executed_list_stacks_validation)))
        LOG.info("stacks data validation failed OUs- list is {}".format(set(invalid_stacks_data_ous)))
        LOG.info("data consistency not executed OUs List {}".format(set(not_executed_list_data_consistency)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("filters validation not executed OUs List {}".format(set(not_executed_list_filters_validation)))
        LOG.info("filters data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_stacks_validation) == 0, "stacks data validation not executed OUs List- list is {}".format(set(not_executed_list))
        assert len(not_executed_list_data_consistency) == 0, "data consistency not executed OUs- list is {}".format(set(not_executed_list_data_consistency))
        assert len(not_executed_list_filters_validation) == 0, "filters validation not executed OUs List- list is {}".format(set(not_executed_list_filters_validation))
        assert len(invalid_stacks_data_ous) == 0, "stacks data validation failed OUs- list is {}".format(set(invalid_data_ous))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "filters data validation failed OUs- list is {}".format(set(invalid_data_ous))
