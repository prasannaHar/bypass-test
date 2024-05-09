import logging
import pytest
import pandas as pd

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIndividualRawStatsReport:
    generic_object = TGhelper()
    rbacusertype = generic_object.api_data["trellis_rbac_user_types"]
    req_interval = generic_object.env["trellis_intervals"]

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_individual_raw_stats_report_vs_raw_stats_by_org_units(self, create_widget_helper_object,req_interval,
                                                                    create_generic_object, create_customer_object):
        """Validate individual raw stats widget functionality - negative values check"""
        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        invalid_ou_data_list = []
        
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": req_interval}
                response_data = create_widget_helper_object.create_individual_raw_stats(filters=filters)
                executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(
                    response=response_data,inside_records=True,invalid_values_check=True,raw_stats=True)
                org_response_data = create_widget_helper_object.create_raw_stats_by_org_units(filters=filters)
                org_executed_flag, org_valid_data_flag = create_customer_object.trellis_data_validator(
                    response=org_response_data,invalid_values_check=True,raw_stats=True)
                if valid_data_flag and org_valid_data_flag:
                    ## individual raw stats data DF
                    response_data = (response_data["records"][0])["records"]
                    response_df = pd.json_normalize(response_data)
                    response_df = response_df.fillna(0)
                    ## raw stats by org units data DF - required ou
                    org_response_data = org_response_data["records"]
                    org_response_df = pd.json_normalize(org_response_data)
                    ou_name = create_generic_object.get_integrations_based_on_ou_id(
                                            ou_id=org_id, get_name=True)
                    org_response_df_filtered = org_response_df[org_response_df["name"] == ou_name]
                    for index, row in org_response_df_filtered.iterrows():
                        for col_name, cell_value in row.iteritems():
                            if col_name.startswith("raw_stats."):
                                user_raw_stats = (response_df[col_name].sum())/len(response_df)
                                difference = user_raw_stats - cell_value
                                if not (-1.5 <= difference <= 1.5):
                                    invalid_ou_data_list.append(org_id)
                                    break
                    for eachcolumn in response_df.columns:
                        if eachcolumn.startswith("raw_stats."):
                            response_data
                if (not valid_data_flag) or (not org_valid_data_flag):
                    invalid_ou_data_list.append(org_id)                    
                if (not executed_flag) or (not org_executed_flag):
                    not_executed_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("Invalid Raw stats values OUs List {}".format(set(invalid_ou_data_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(invalid_ou_data_list) == 0, "invalid data OUs- list is {}".format(set(invalid_ou_data_list))
