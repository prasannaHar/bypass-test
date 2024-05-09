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
    scm_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="scm_prs_report",report_type="scm")
    ## sanity test case addition
    scm_test_params.append(
        pytest.param(("repo_id","none","pr_merged_at","none","none","projects","files","value_high-low"),
        marks=pytest.mark.sanity))

    @pytest.mark.regression
    @pytest.mark.usefixtures
    @pytest.mark.parametrize("reports_test_params", scm_test_params)
    def test_scm_prs_report_drilldown_data_validation(self, create_scm_prs_report_object, reports_test_params, 
                                             get_integration_obj, create_generic_object, api_reusable_functions_object):        
        """Validate alignment of scm_prs_report"""
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous"]
        not_executed_list = []
        mismatch_data_ous_list = []
        invalid_data_ous = []
        for org_id in org_ids:
            try:
                ## widget creation
                scm_prs_report_payload = create_scm_prs_report_object.scm_prs_report_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=get_integration_obj, ou_id=[org_id])
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_prs-report"]
                LOG.info("payload---{}".format(scm_prs_report_payload))
                prs_report_resp = create_generic_object.execute_api_call(url, "post", data=scm_prs_report_payload)
                if prs_report_resp["count"] == 0: pytest.skip("no data present in widget api")
                ## collecting max of 5 random samples
                random_samples = random.sample(prs_report_resp["records"], min(5,len(prs_report_resp["records"])))
                ## widget v/s drill-down data validation
                for eachrecord in random_samples:
                    key_name = "key"
                    if reports_test_params[1] in ["month", "quarter"]:  key_name = "additional_key"
                    scm_prs_report_drilldown_payload = create_scm_prs_report_object.scm_prs_report_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=get_integration_obj, ou_id=[org_id], 
                                                    req_key_val=eachrecord[key_name])        
                    LOG.info("payload drilldown---{}".format(scm_prs_report_drilldown_payload))
                    url_drilldown = create_generic_object.connection["base_url"] +create_generic_object.api_data["scm_pr_list_drilldown"]
                    prs_report_resp_drilldown = create_generic_object.execute_api_call(url_drilldown, "post", 
                                                                            data=scm_prs_report_drilldown_payload)
                    ## widget v/s drill-down data consistency
                    drilldown_count = (prs_report_resp_drilldown["_metadata"])["total_count"]
                    widget_count = eachrecord["count"]
                    if drilldown_count != widget_count:
                        mismatch_data_ous_list.append(org_id)
                    ## drilldown data validation
                    prs_report_resp_drilldown_df = pd.json_normalize(prs_report_resp_drilldown['records'], 
                                                                max_level=1)
                    execution_flag, data_validation  = create_scm_prs_report_object.scm_prs_report_drilldown_data_validator(
                                                response_data_df=prs_report_resp_drilldown_df,
                                                required_test_params=reports_test_params, integration_ids=get_integration_obj,
                                                api_reusable_functions_object=api_reusable_functions_object)
                    if not execution_flag:
                        not_executed_list.append(str(org_id) + "_" + str(eachrecord[key_name]))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord[key_name]))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))
