import logging
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve
import random
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCMCodingDaysReportDataValidation:

    TestParamsObj = ReportTestParametersRetrieve()
    scm_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="scm_coding_days_report",report_type="scm")
    ## sanity test case addition
    scm_test_params.append(
        pytest.param(("repo_id", "avg_coding_day", "week", "committed_at", "projects", "none"), 
        marks=pytest.mark.sanity))

    @pytest.mark.regression
    @pytest.mark.usefixtures
    @pytest.mark.parametrize("reports_test_params", scm_test_params)
    def test_scm_coding_days_report_data_validation(self, create_scm_coding_days_object, reports_test_params, 
                                             get_integration_obj, create_generic_object, 
                                             api_reusable_functions_object, create_widget_helper_object):        
        """Validate alignment of scm_coding_days_report"""
        coding_days_config = create_generic_object.api_data["scm_coding_days_config"]
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous"]
        not_executed_list = []
        not_executed_list_data_consistency = []
        invalid_widget_data_ous = []
        mismatch_data_ous_list = []
        invalid_data_ous = []
        for org_id in org_ids:
            try:
                ## widget creation
                scm_coding_days_report_payload = create_scm_coding_days_object.scm_coding_days_report_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=get_integration_obj, ou_id=[org_id])
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_coding_days_report"]
                LOG.info("payload---{}".format(scm_coding_days_report_payload))
                coding_days_report_resp = create_generic_object.execute_api_call(url, "post", data=scm_coding_days_report_payload)
                if coding_days_report_resp["count"]  == 0:
                    pytest.skip("no data present in widget api")
                ## widget data validation 
                coding_days_report_resp_df = pd.json_normalize(coding_days_report_resp['records'], max_level=1)
                coding_days_report_resp_df_invalid = coding_days_report_resp_df[coding_days_report_resp_df['mean']>coding_days_config[reports_test_params[2]]]
                if len(coding_days_report_resp_df_invalid) > 0:
                    invalid_widget_data_ous.append(str(org_id))
                ## collecting max of 5 random samples
                random_samples = random.sample(coding_days_report_resp["records"], min(5,len(coding_days_report_resp["records"])))
                ## widget v/s drill-down data validation
                for eachrecord in random_samples:
                    scm_coding_days_report_drilldown_payload = create_scm_coding_days_object.scm_coding_days_report_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=get_integration_obj, ou_id=[org_id], 
                                                    req_key_val=eachrecord['key'])
                    LOG.info("payload drilldown---{}".format(scm_coding_days_report_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["scm_commits_list_drilldown"]
                    coding_days_report_resp_drilldown_df = create_widget_helper_object.generate_paginated_drilldown_data(
                                                                url=url_drilldown, 
                                                                payload=scm_coding_days_report_drilldown_payload)
                    ## widget v/s drill-down data consistency
                    execution_flag_consistency, data_consistency = create_scm_coding_days_object.\
                        scm_coding_days_widget_versus_drilldown_data_consistency_check(
                                    required_test_params=reports_test_params,
                                    widget_vals=[eachrecord["mean"],eachrecord["median"]],
                                    drilldown_resp_df = coding_days_report_resp_drilldown_df)
                    ## drilldown data validation
                    execution_flag, data_validation  = create_scm_coding_days_object.scm_coding_days_report_drilldown_data_validator(
                                                response_data_df=coding_days_report_resp_drilldown_df,
                                                required_test_params=reports_test_params, integration_ids=get_integration_obj,
                                                api_reusable_functions_object=api_reusable_functions_object)
                    if not execution_flag:
                        not_executed_list.append(str(org_id) + "_" + str(eachrecord['key']))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord['key']))
                    if not data_consistency:
                        mismatch_data_ous_list.append(str(org_id) + "_" + str(eachrecord['key']))
                    if not execution_flag_consistency:
                        not_executed_list_data_consistency.append(str(org_id) + "_" + str(eachrecord['key']))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("data consistency not executed OUs List {}".format(set(not_executed_list_data_consistency)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        LOG.info("widget data validation failed OUs {}".format(set(invalid_widget_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_widget_data_ous) == 0, "widget data validation failed OUs- list is {}".format(set(invalid_widget_data_ous))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))
        assert len(not_executed_list_data_consistency) == 0, "data consistency not executed OUs- list is {}".format(set(not_executed_list_data_consistency))
