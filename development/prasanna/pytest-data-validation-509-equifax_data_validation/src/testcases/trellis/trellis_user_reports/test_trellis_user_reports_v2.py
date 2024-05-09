import logging
import pytest
import pandas as pd

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.testcases.trellis.trellis_user_reports.trellis_user_reports_helper_v2 import TrellisUserReportHelperV2 as Trellishelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TestTrellisUserReports:
    generic_object = TGhelper()
    tchelper_object = Trellishelper(generic_object)
    trellis_test_params = tchelper_object.trellis_user_report_params()

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.parametrize("interval, collection, contributors", trellis_test_params)
    def test_trellis_user_data_consistency_widget_drilldown(self, create_generic_object,
                                trellis_helper_object, trellis_scores_helper_object,
                                trellis_profile_helper_object,
                                trellis_profile_mapping_helper_object,
                                trellis_user_report_v2_helper_object,
                                trellis_validations_helper_object,
                                interval, collection, contributors):
        """Trellis user reports data validation widget v/s drill-down"""
        LOG.info("==== Trellis User Reports data validation across widget and drill-down ====")

        ## contributors check -- if contributors list is empty retrieve contributors list from collection
        if len(contributors)==0:
            ## retreive contributors list from trellis score report widget
            contributors = trellis_helper_object.trellis_retrieve_contributor_ids(
                                    collection_id=collection, interval=interval)
        invalid_scores_users = []
        invalid_metric_and_factor_score_users = []
        missing_metric_and_factor_users = []
        metrics_result_analysis = pd.DataFrame()

        trellis_v2_flag = False
        if pytest.tenant_name in create_generic_object.api_data["trellis_v2_tenants"]:
            trellis_v2_flag = True

        trellis_profile = ""
        for each_contributor in contributors:
            ## retreive trellis profile id
            if trellis_v2_flag:
                ## retreive user contributor role
                trellis_profile_response = trellis_profile_mapping_helper_object.\
                                    retrieve_contributor_trellis_profile(
                                        collection_id=collection, 
                                        contributor_uuid=each_contributor)
                trellis_profile = trellis_profile_response["id"]
            else:
                trellis_profile = trellis_profile_mapping_helper_object.\
                                    retrieve_ou_trellis_profile_v1(
                                        collection_id=collection)
            ## retrieve user trellis report
            trellis_user_report = trellis_helper_object.retrieve_trellis_user_report(
                                                ou_user_id=each_contributor,
                                                trellis_profile=trellis_profile, 
                                                interval=interval)
            ## trellis invalid scores validation
            invalid_scores_check = trellis_validations_helper_object.trellis_invalid_scores_validation(
                                                        trellis_user_report=trellis_user_report)
            if invalid_scores_check:
                invalid_scores_users.append([interval, collection, each_contributor])
            ## trellis user report - metrics and factor scores validation
            score_validation_failed_factors, score_validation_failed_metrics = \
                trellis_validations_helper_object.trellis_metric_and_factor_scores_validation(
                                        trellis_user_report=trellis_user_report, 
                                        trellis_profile=trellis_profile, 
                                        week_interval=interval)
            if ( (len(score_validation_failed_factors) > 0) or (len(score_validation_failed_metrics) > 0) ):
                invalid_metric_and_factor_score_users.append([interval, collection, each_contributor, 
                                        score_validation_failed_factors, score_validation_failed_metrics])
            ## trellis user report - missing factors and metrics validation
            missing_factors, missing_metrics = trellis_validations_helper_object.\
                    trellis_validate_missing_factors_and_metrics(
                                        trellis_user_report=trellis_user_report, 
                                        trellis_profile=trellis_profile, interval=interval)
            if ( (len(missing_factors) > 0) or (len(missing_metrics) > 0)):
                missing_metric_and_factor_users.append([interval, collection, each_contributor, missing_factors, missing_metrics])

            ## trellis user report data validation
            each_user_metric_value_validation_results = trellis_user_report_v2_helper_object.\
                                                trellis_user_report_widget_vs_drilldown_metric_value_analysis(
                                                    trellis_helper_object=trellis_helper_object,
                                                    trellis_validations_helper_object=trellis_validations_helper_object,
                                                    trellis_profile_helper_object=trellis_profile_helper_object,
                                                    trellis_user_report=trellis_user_report, 
                                                    ou_user_id=each_contributor, 
                                                    trellis_profile=trellis_profile, interval=interval)
            each_user_metric_value_validation_results = each_user_metric_value_validation_results.drop_duplicates(
                                                subset=None, keep='first', inplace=False, ignore_index=False)
            metrics_result_analysis = pd.concat([metrics_result_analysis, each_user_metric_value_validation_results])
        metrics_result_analysis.to_csv("../result/complete_details.csv", index=False)
        ## retrieve failed metrics details
        metrics_result_analysis_failed = metrics_result_analysis.loc[metrics_result_analysis['Status'] == "Fail"]
        if len(metrics_result_analysis_failed)!=0:
            metrics_result_analysis_failed.to_csv("../result/failure_details.csv", index=False)

        assert len(invalid_scores_users) == 0, "scores validation failed"
        assert len(invalid_metric_and_factor_score_users) == 0, "metric and feature scores validation failed"
        assert len(missing_metric_and_factor_users) == 0, "metric and factors are missing"
        assert metrics_result_analysis_failed.shape[0] == 0, metrics_result_analysis_failed
