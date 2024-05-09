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
                            report_name="scm_prs_lead_time_by_stage",report_type="scm")
    ## sanity test case addition
    scm_test_params.append(
        pytest.param(("pr_created_at", "repo_ids", "none"),
        marks=pytest.mark.sanity))
    
    exclude_n_partial_tests = [ (True, True),(True, False),
                                (False, True),(False, False)]

    @pytest.mark.regression
    @pytest.mark.parametrize("reports_test_params", scm_test_params)
    def test_scm_pr_lead_time_by_stage_report_data_validation(self, create_generic_object, 
                                                              create_widget_helper_object,
                                                            create_scm_pr_lead_time_helper_object,
                                                            get_integration_obj, reports_test_params,
                                                            api_reusable_functions_object):
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
                payload = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_payload_generate(
                                                required_test_params=reports_test_params,
                                                integration_ids=get_integration_obj, ou_id=[org_id],
                                                api_reusable_functions_object=api_reusable_functions_object)
                url = create_generic_object.connection["base_url"] + create_generic_object.api_data["velocity"]
                LOG.info("payload---{}".format(payload))
                resp = create_generic_object.execute_api_call(url, "post", data=payload)
                if len(resp['records']) == 0:
                    pytest.skip("unable to create the report , No Data Available")
                ## widget v/s drill-down data validation
                for eachrecord in resp['records']:
                    widget_mean, widget_median, widget_p90, widget_p95 = eachrecord["mean"], eachrecord["median"], eachrecord["p90"], eachrecord["p95"]
                    drilldown_payload = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_payload_generate_drilldown(
                                                    required_test_params=reports_test_params,
                                                    integration_ids=get_integration_obj, ou_id=[org_id],
                                                    req_key_val=eachrecord['key'], api_reusable_functions_object=api_reusable_functions_object)
                    drilldown_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["velocity_values"]
                    LOG.info("drilldown_payload---{}".format(drilldown_payload))
                    drilldown_resp_df = create_widget_helper_object.generate_paginated_drilldown_data(
                                                                url=drilldown_url,payload=drilldown_payload,
                                                                pr_lead_time=True)
                    ## widget v/s drill-down data consistency
                    mean, median, p90,p95 = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_retrive_mean_median_p90_p95(
                                                response_data_df=drilldown_resp_df, key_name=eachrecord['key'])
                    if (widget_mean-mean) != 0:
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_mean")
                    if not (-3600 <= (widget_median-median) <= 3600):
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_median")
                    if not (-60 <= (widget_p90-p90) <= 60):
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_p90")
                    if not (-60 <= (widget_p95-p95) <= 60):
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_p95")
                    ## drilldown data validation
                    drilldown_resp = create_generic_object.execute_api_call(drilldown_url, "post", data=drilldown_payload)
                    drilldown_resp_df = pd.json_normalize(drilldown_resp['records'], 
                                                                max_level=1)
                    if len(drilldown_resp_df) > 0:
                        execution_flag, data_validation  = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_drilldown_data_validator(
                                                    response_data_df=drilldown_resp_df)
                    else:
                        pytest.skip("Unable to Create the Report. No Data is Available")
                    if not execution_flag:
                        not_executed_list.append(str(org_id) + "_" + str(eachrecord['key']))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord['key']))

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))


    @pytest.mark.regression
    @pytest.mark.parametrize("exclude_n_partial_tests", exclude_n_partial_tests)
    def test_scm_pr_lead_time_by_stage_report_partial_match_exclude(self, create_scm_prs_report_object,exclude_n_partial_tests,
                                            create_widget_helper_object, create_scm_pr_lead_time_helper_object,
                                             get_integration_obj, create_generic_object, api_reusable_functions_object):        
        """Validate SCM PR Lead time by stage Report - Source branch filter partial match with Exclude option
        Steps Involved:
        1. call widget api
        2. call drill-down for any one of the stage
        3. get the available source branch column values
        4. generate the contains/starts with pattern
        5. call widget api with source branch partial match filter
        6. call drilldown api with source branch partial match exclude filter
        7. make there is no common data between 5 & 6 steps
        6. make sure combintation of data received in step -5, 6 is subset of data received in step2
        7. verify widget v/s drilldown data consistency for the source branch with partial match filter
        8. verify widget v/s drilldown data consistency for the source branch with partial match exclude filter
        """
        LOG.info("==== create widget with following test paramas ====")
        reports_test_params_prs_report = ("source_branch","none","pr_merged_at","none","none","repo_ids","files","value_high-low")
        reports_test_params_pr_lead_time = ("pr_merged_at", "source_branches", "repo_ids")
        if exclude_n_partial_tests[1] == True:
            reports_test_params_pr_lead_time = ("pr_merged_at", "source_branch", "repo_ids")

        LOG.info("test params scm prs report ---{}".format(reports_test_params_prs_report))
        LOG.info("test params scm pr lead time report---{}".format(reports_test_params_pr_lead_time))

        org_ids = create_generic_object.env["set_ous"]
        not_executed_list = []
        mismatch_data_ous_list = []
        invalid_data_ous = []

        for org_id in org_ids:
            try:
                ## Retrieving the filter pattern
                ## widget creation
                scm_prs_report_payload = create_scm_prs_report_object.scm_prs_report_payload_generate(
                                                required_test_params=reports_test_params_prs_report,
                                                integration_ids=get_integration_obj, ou_id=[org_id])
                url_prs_report = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_prs-report"]
                LOG.info("payload---{}".format(scm_prs_report_payload))
                prs_report_resp = create_generic_object.execute_api_call(url_prs_report, "post", data=scm_prs_report_payload)
                if prs_report_resp["count"] == 0: pytest.skip("no data present in scm prs report widget api")
                prs_report_resp_df = pd.json_normalize(prs_report_resp['records'], max_level=1)
                available_key_vals = prs_report_resp_df["key"].values.tolist()
                ## SCM PR Lead time validations
                LOG.info("validating exclude filters scenarios")
                ## widget creation
                adv_filter_config = {"exclude":exclude_n_partial_tests[0],"partial_match": exclude_n_partial_tests[1], "filter_vals":available_key_vals}
                payload_pr_lead_time = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_payload_generate(
                                                required_test_params=reports_test_params_pr_lead_time,
                                                integration_ids=get_integration_obj, ou_id=[org_id], 
                                                api_reusable_functions_object=api_reusable_functions_object, 
                                                adv_config = adv_filter_config)
                LOG.info("exclude filter payload---{}".format(payload_pr_lead_time))
                url_pr_lead_time = create_generic_object.connection["base_url"] + create_generic_object.api_data["velocity"]
                resp_exclude_filter = create_generic_object.execute_api_call(url_pr_lead_time, "post", data=payload_pr_lead_time)
                if len(resp_exclude_filter['records']) == 0:
                    pytest.skip("unable to create PR lead time report, No Data Available")
                ## widget v/s drill-down data validation
                for eachrecord in resp_exclude_filter['records']:
                    widget_mean, widget_median, widget_p90, widget_p95 = eachrecord["mean"], eachrecord["median"], eachrecord["p90"], eachrecord["p95"]
                    drilldown_payload_pr_lead_time = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_payload_generate_drilldown(
                                                    required_test_params=reports_test_params_pr_lead_time,
                                                    integration_ids=get_integration_obj, ou_id=[org_id],
                                                    req_key_val=eachrecord['key'],
                                                    api_reusable_functions_object=api_reusable_functions_object, 
                                                    adv_config = adv_filter_config)
                    LOG.info("drilldown_payload exclude filter---{}".format(drilldown_payload_pr_lead_time))
                    drilldown_url_pr_lead_time = create_generic_object.connection["base_url"] + create_generic_object.api_data["velocity_values"]
                    drilldown_resp_df_pr_lead_time = create_widget_helper_object.generate_paginated_drilldown_data(
                                                                url=drilldown_url_pr_lead_time,payload=drilldown_payload_pr_lead_time,
                                                                pr_lead_time=True)

                    ## widget v/s drill-down data consistency
                    mean, median, p90,p95 = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_retrive_mean_median_p90_p95(
                                                response_data_df=drilldown_resp_df_pr_lead_time, key_name=eachrecord['key'])
                    if (widget_mean-mean) != 0:
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_mean")
                    if not (-3600 <= (widget_median-median) <= 3600):
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_median")
                    if not (-60 <= (widget_p90-p90) <= 60):
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_p90")
                    if not (-60 <= (widget_p95-p95) <= 60):
                        mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_p95")
                    ## drilldown data validation
                    drilldown_resp = create_generic_object.execute_api_call(drilldown_url_pr_lead_time, "post", data=drilldown_payload_pr_lead_time)
                    drilldown_resp_df_pr_lead_time = pd.json_normalize(drilldown_resp['records'], 
                                                                max_level=1)
                    if len(drilldown_resp_df_pr_lead_time) > 0:
                        execution_flag, data_validation  = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_drilldown_data_validator(
                                                    response_data_df=drilldown_resp_df_pr_lead_time)
                    else:
                        pytest.skip("Unable to Create the Report. No Data is Available")
                    if not execution_flag:
                        not_executed_list.append(str(org_id) + "_" + str(eachrecord['key']))
                    if not data_validation:
                        invalid_data_ous.append(str(org_id) + "_" + str(eachrecord['key']))

            except Exception as ex:
                breakpoint()
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))


    @pytest.mark.regression
    def test_scm_pr_lead_time_by_stage_report_partial_match_exclude_vs_without_exclude(self, create_scm_prs_report_object,
                                            create_widget_helper_object, create_scm_pr_lead_time_helper_object,
                                             get_integration_obj, create_generic_object, api_reusable_functions_object):        
        """Validate SCM PR Lead time by stage Report - Source branch filter partial match with Exclude option
        Steps Involved:
        1. call widget api
        2. call drill-down for any one of the stage
        3. get the available source branch column values
        4. generate the contains/starts with pattern
        5. call widget api with source branch partial match filter
        6. call drilldown api with source branch partial match exclude filter
        7. make there is no common data between 5 & 6 steps
        6. make sure combintation of data received in step -5, 6 is subset of data received in step2
        7. verify widget v/s drilldown data consistency for the source branch with partial match filter
        8. verify widget v/s drilldown data consistency for the source branch with partial match exclude filter
        """
        LOG.info("==== create widget with following test paramas ====")
        reports_test_params_prs_report = ("source_branch","none","pr_merged_at","none","none","repo_ids","files","value_high-low")
        reports_test_params_pr_lead_time = ("pr_merged_at", "source_branch", "repo_ids")
        LOG.info("test params scm prs report ---{}".format(reports_test_params_prs_report))
        LOG.info("test params scm pr lead time report---{}".format(reports_test_params_pr_lead_time))

        org_ids = create_generic_object.env["set_ous"]
        not_executed_list = []
        mismatch_data_ous_list = []
        invalid_data_ous = []
        partial_match_mismatch_ous = []

        for org_id in org_ids:
            try:
                ## Retrieving the filter pattern
                ## widget creation
                scm_prs_report_payload = create_scm_prs_report_object.scm_prs_report_payload_generate(
                                                required_test_params=reports_test_params_prs_report,
                                                integration_ids=get_integration_obj, ou_id=[org_id])
                url_prs_report = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_prs-report"]
                LOG.info("payload---{}".format(scm_prs_report_payload))
                prs_report_resp = create_generic_object.execute_api_call(url_prs_report, "post", data=scm_prs_report_payload)
                if prs_report_resp["count"] == 0: pytest.skip("no data present in scm prs report widget api")
                prs_report_resp_df = pd.json_normalize(prs_report_resp['records'], max_level=1)
                available_key_vals = prs_report_resp_df["key"].values.tolist()
                ## SCM PR Lead time validations -- partial match with exclude
                LOG.info("SCM PR Lead time validations -- partial match with exclude")
                ## widget creation
                req_combinations = [(True, True), (False, True)]
                result_dfs = []
                for each_combination in req_combinations:
                    adv_filter_config = {"exclude":each_combination[0],"partial_match":each_combination[1], "filter_vals":available_key_vals}
                    payload_pr_lead_time = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_payload_generate(
                                                    required_test_params=reports_test_params_pr_lead_time,
                                                    integration_ids=get_integration_obj, ou_id=[org_id], 
                                                    api_reusable_functions_object=api_reusable_functions_object, 
                                                    adv_config = adv_filter_config)
                    LOG.info("exclude filter payload---{}".format(payload_pr_lead_time))
                    url_pr_lead_time = create_generic_object.connection["base_url"] + create_generic_object.api_data["velocity"]
                    resp_exclude_filter = create_generic_object.execute_api_call(url_pr_lead_time, "post", data=payload_pr_lead_time)
                    if len(resp_exclude_filter['records']) == 0:
                        pytest.skip("unable to create PR lead time report, No Data Available")
                    ## widget v/s drill-down data validation
                    for eachrecord in resp_exclude_filter['records']:
                        widget_mean, widget_median, widget_p90, widget_p95 = eachrecord["mean"], eachrecord["median"], eachrecord["p90"], eachrecord["p95"]
                        drilldown_payload_pr_lead_time = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_payload_generate_drilldown(
                                                        required_test_params=reports_test_params_pr_lead_time,
                                                        integration_ids=get_integration_obj, ou_id=[org_id],
                                                        req_key_val=eachrecord['key'],
                                                        api_reusable_functions_object=api_reusable_functions_object, 
                                                        adv_config = adv_filter_config)
                        LOG.info("drilldown_payload exclude filter---{}".format(drilldown_payload_pr_lead_time))
                        drilldown_url_pr_lead_time = create_generic_object.connection["base_url"] + create_generic_object.api_data["velocity_values"]
                        drilldown_resp_df_pr_lead_time = create_widget_helper_object.generate_paginated_drilldown_data(
                                                                    url=drilldown_url_pr_lead_time,payload=drilldown_payload_pr_lead_time,
                                                                    pr_lead_time=True)

                        ## widget v/s drill-down data consistency
                        mean, median, p90,p95 = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_retrive_mean_median_p90_p95(
                                                    response_data_df=drilldown_resp_df_pr_lead_time, key_name=eachrecord['key'])
                        if (widget_mean-mean) != 0:
                            mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_mean")
                        if not (-3600 <= (widget_median-median) <= 3600):
                            mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_median")
                        if not (-60 <= (widget_p90-p90) <= 60):
                            mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_p90")
                        if not (-60 <= (widget_p95-p95) <= 60):
                            mismatch_data_ous_list.append(org_id + "_" + eachrecord['key'] + "_p95")
                        ## drilldown data validation
                        drilldown_resp = create_generic_object.execute_api_call(drilldown_url_pr_lead_time, "post", data=drilldown_payload_pr_lead_time)
                        drilldown_resp_df_pr_lead_time = pd.json_normalize(drilldown_resp['records'], 
                                                                    max_level=1)
                        if len(drilldown_resp_df_pr_lead_time) > 0:
                            execution_flag, data_validation  = create_scm_pr_lead_time_helper_object.scm_pr_lead_time_by_stage_drilldown_data_validator(
                                                        response_data_df=drilldown_resp_df_pr_lead_time)
                        else:
                            pytest.skip("Unable to Create the Report. No Data is Available")
                        if not execution_flag:
                            not_executed_list.append(str(org_id) + "_" + str(eachrecord['key']))
                        if not data_validation:
                            invalid_data_ous.append(str(org_id) + "_" + str(eachrecord['key']))
                        result_dfs.append(drilldown_resp_df_pr_lead_time)
                        break
                ## partial match exclude vs partial match filters validation
                list1 = set(result_dfs[0]["key"].values.tolist())
                list2 = set(result_dfs[1]["key"].values.tolist())
                common_elements = list(list1.intersection(list2))
                if len(common_elements)>0:
                    partial_match_mismatch_ous.append(org_id)

            except Exception as ex:
                LOG.info("exeception occured {}".format(ex))
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("widget v/s drilldown mismatch - OUs list {}".format(set(mismatch_data_ous_list)))
        LOG.info("drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(mismatch_data_ous_list) == 0, "mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))
        assert len(invalid_data_ous) == 0, "drilldown data validation failed OUs- list is {}".format(set(invalid_data_ous))
        assert len(partial_match_mismatch_ous) == 0, "partial match exclude filter v/s partial match mismatch data OUs- list is {}".format(set(mismatch_data_ous_list))

