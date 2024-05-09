import logging
import pytest
import pandas as pd
from src.lib.core_reusable_functions import epoch_timeStampsGenerationForRequiredTimePeriods as TPhelper
from src.utils.dev_prod_reusable_functions import retrieve_drilldown_api_response as TrellisDrilldownData
from src.utils.dev_prod_reusable_functions import widget_value_calculation_from_drilldown
from src.utils.dev_prod_reusable_functions import trellis_profile_metric_name_retriever

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIndividualRawStatsDataConsistency:
    generic_object = TGhelper()
    req_interval = generic_object.env["trellis_intervals"]

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_individual_raw_stats_report_widget_vs_drilldown_data_consistency(self, create_widget_helper_object, 
                                                ou_helper_object, create_generic_object, create_customer_object,
                                                req_interval):
        """Validate individual raw stats widget functionality - negative values check"""
        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        invalid_ou_data_list = []
        data_mistmatch_list = []
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": req_interval}
                response_data = create_widget_helper_object.create_individual_raw_stats(filters=filters)
                ou_users = ou_helper_object.retrieve_ou_assocaited_users(ou_id=org_id)
                executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(
                                                    response=response_data,inside_records=True,
                                                    associated_users_check=True,
                                                    raw_stats=True, ou_users=ou_users)
                if executed_flag and valid_data_flag:
                    response_data = (response_data["records"][0])["records"]
                    for each_org_user in response_data:
                        eachuser_raw_stats = each_org_user['raw_stats']
                        for eachfeature in eachuser_raw_stats:                            
                            if eachfeature['name'].startswith('High Impact'):
                                continue
                            ## drilldown api response
                            drilldown_data = TrellisDrilldownData(
                                application_url= create_generic_object.connection["base_url"], 
                                ou_user_id=each_org_user["org_user_id"],
                                interval=req_interval,feature_name=eachfeature['name'])
                            ## related profile feature name retrieve
                            profile_feature_name = trellis_profile_metric_name_retriever(metric_name=eachfeature['name'], interval=req_interval)
                            calculated_widget_value = widget_value_calculation_from_drilldown(
                                arg_drill_down_response=drilldown_data,arg_metric_name=profile_feature_name,
                                arg_interval=req_interval,raw_stats=True)
                            actual_widget_value = 0
                            if "count" in eachfeature.keys():
                                actual_widget_value = eachfeature['count']
                            diff_val = actual_widget_value - calculated_widget_value
                            if not (-0.5 <= (actual_widget_value - calculated_widget_value) <= 0.5):
                                data_mistmatch_list.append([org_id, each_org_user["org_user_id"], 
                                        req_interval, eachfeature['name'], actual_widget_value, 
                                        calculated_widget_value, diff_val ])
                                ## [ou name, org user id,  inteval, raw stat, 
                                # actual widget value, calculated widget value, diff]
                if not executed_flag:
                    not_executed_list.append(org_id)
                if not valid_data_flag:
                    invalid_ou_data_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("OUs list with missed associated users data {}".format(set(invalid_ou_data_list)))
        LOG.info("data mismatches {}".format(str(data_mistmatch_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(invalid_ou_data_list) == 0, "invalid data OUs- list is {}".format(set(invalid_ou_data_list))
        assert len(data_mistmatch_list) == 0, "inconsistent data list is {}".format(data_mistmatch_list)


    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_individual_raw_stats_report_relativescore(self, create_widget_helper_object,req_interval, 
                                                ou_helper_object, create_generic_object, create_customer_object):
        """Validate individual raw stats widget functionality - negative values check"""
        org_ids = create_generic_object.env["set_trellis_ous"]
        drilldown_intervals = create_generic_object.api_data["trellis_rawstats_drilldown_intereval_mapping"]
        not_executed_list = []
        invalid_ou_data_list = []
        api_failures_list = []
        no_data_list = []
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": req_interval}
                response_data = create_widget_helper_object.create_individual_raw_stats(filters=filters)
                ou_users = ou_helper_object.retrieve_ou_assocaited_users(ou_id=org_id)
                executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(
                                                    response=response_data,inside_records=True,
                                                    associated_users_check=True,
                                                    raw_stats=True, ou_users=ou_users)
                if executed_flag and valid_data_flag:
                    response_data = (response_data["records"][0])["records"]
                    for each_org_user in response_data:
                        filters = {"agg_interval":drilldown_intervals[req_interval],
                            "ou_ref_ids":[org_id],"no_comparison":True,
                            "report_requests":[
                                {"id":each_org_user["org_user_id"],"id_type":"ou_user_ids"}]}
                        resp = create_widget_helper_object.create_trellis_relative_score(filters=filters,page_size=False)
                        if not resp:
                            api_failures_list.append([org_id, each_org_user["org_user_id"], req_interval])
                        elif not (resp["count"]>=1):
                            no_data_list.append([org_id, each_org_user["org_user_id"], req_interval])
                if not executed_flag:
                    not_executed_list.append(org_id)
                if not valid_data_flag:
                    invalid_ou_data_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("OUs list with missed associated users data {}".format(set(invalid_ou_data_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(invalid_ou_data_list) == 0, "invalid data OUs- list is {}".format(set(invalid_ou_data_list))
        assert len(api_failures_list) == 0, "relative score api failures list is {}".format(api_failures_list)
        assert len(no_data_list) == 0, "relative score api no data list {}".format(api_failures_list)
