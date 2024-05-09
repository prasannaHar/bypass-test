import logging
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve
import random
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCmPRsSingeStatDataValidation:

    TestParamsObj = ReportTestParametersRetrieve()
    scm_test_params = TestParamsObj.retrieve_widget_test_parameters(
                            report_name="scm_prs_single_stat",report_type="scm")
    ## sanity test case addition
    scm_test_params.append(
        pytest.param(("pr_created", "pr_created_at", "repo_ids", "none", "none"),
        marks=pytest.mark.sanity))

    @pytest.mark.regression
    @pytest.mark.usefixtures
    @pytest.mark.parametrize("reports_test_params", scm_test_params)
    def test_scm_prs_single_stat_data_validation(self, create_scm_prs_report_object, reports_test_params, 
                                             get_integration_obj, create_generic_object):        
        """Validate alignment of scm_prs_single_stat"""
        LOG.info("==== create widget with following test paramas ====")
        LOG.info("test params---{}".format(reports_test_params))
        org_ids = create_generic_object.env["set_ous"]
        not_executed_list = []
        for org_id in org_ids:
            try:
                ## widget creation
                scm_prs_single_stat_payload = create_scm_prs_report_object.scm_prs_single_stat_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=get_integration_obj, ou_id=[org_id])
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_prs-report"]
                LOG.info("payload---{}".format(scm_prs_single_stat_payload))
                prs_report_resp = create_generic_object.execute_api_call(url, "post", data=scm_prs_single_stat_payload)
                if prs_report_resp["count"] == 0: pytest.skip("no data present in widget api")
            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
