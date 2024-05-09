import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisOUScoreCard:
    generic_object = TGhelper()
    rbacusertype = generic_object.api_data["trellis_rbac_user_types"]

    @pytest.mark.trellisrbac
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_ou_score_card_tc001_relative_score_rbac(self, create_widget_helper_object, create_generic_object,
                                                ou_helper_object, rbacusertype):
        """Validate ou score card page - relative score widget functionality"""
        org_ids = create_generic_object.env["set_trellis_ous"]
        if rbacusertype in ["ADMIN_TRELLIS_OUs", "ORGADMIN"]:
            user_email = create_generic_object.retrieve_user_email(user_type=rbacusertype)
            org_ids = ou_helper_object.retrieve_user_managed_ous(
                                            email=user_email, user_managed_trellis_ous="YES")
        org_ids = org_ids[:1]
        not_executed_list = []
        flag = False
        for org_id in org_ids:
            try:
                ou_uuid = ou_helper_object.retrieve_ou_uuid(ou_id=org_id)
                filters = {"report_requests": [{"id_type": "org_ids", "org_ids": [ou_uuid]}], "agg_interval": "month"}
                relative_score_widget = create_widget_helper_object.create_trellis_relative_score(filters=filters, user_type=rbacusertype)
                if rbacusertype in ["ADMIN", "PUBLIC_DASHBOARD"]:
                    if relative_score_widget.status_code == 403: flag = True
                elif relative_score_widget: flag = True
                if not flag:
                    not_executed_list.append(org_id + " " + rbacusertype)
            except Exception as ex:
                not_executed_list.append(org_id + " " + rbacusertype)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=2)
    def test_ou_score_card_tc001_relative_score(self, create_widget_helper_object, create_generic_object,
                                                ou_helper_object):
        """Validate ou score card page - relative score widget functionality"""
        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        flag = False
        for org_id in org_ids:
            try:
                ou_uuid = ou_helper_object.retrieve_ou_uuid(ou_id=org_id)
                filters = {"report_requests": [{"id_type": "org_ids", "org_ids": [ou_uuid]}], "agg_interval": "month"}
                relative_score_widget = create_widget_helper_object.create_trellis_relative_score(filters=filters)
                if relative_score_widget: flag = True
                if not flag:
                    not_executed_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))

    @pytest.mark.trellisrbac
    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_ou_score_card_tc002_trellis_score_report_rbac(self, create_widget_helper_object, create_generic_object,
                                                      ou_helper_object, create_customer_object, rbacusertype):
        """Validate ou score card - trellis score report functionality"""

        org_ids = create_generic_object.env["set_trellis_ous"]
        if rbacusertype in ["ADMIN_TRELLIS_OUs", "ORGADMIN"]:
            user_email = create_generic_object.retrieve_user_email(user_type=rbacusertype)
            org_ids = ou_helper_object.retrieve_user_managed_ous(
                                            email=user_email, user_managed_trellis_ous="YES")
        not_executed_list = []
        for org_id in org_ids:
            try:
                ou_uuid = ou_helper_object.retrieve_ou_uuid(ou_id=org_id)
                filters = {"ou_ids": [ou_uuid], "interval": "LAST_MONTH"}
                response_data = create_widget_helper_object.create_trellis_score_report(filters=filters, user_type=rbacusertype)
                executed_flag = True
                if rbacusertype in ["ADMIN", "PUBLIC_DASHBOARD"]:
                    if response_data.status_code != 403: executed_flag = False
                else:
                    executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(response=response_data)
                if not executed_flag:
                    not_executed_list.append(org_id + " " + rbacusertype)
            except Exception as ex:
                not_executed_list.append(org_id + " " + rbacusertype)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=4)
    def test_ou_score_card_tc002_trellis_score_report(self, create_widget_helper_object, create_generic_object,
                                                      ou_helper_object, create_customer_object):
        """Validate ou score card - trellis score report functionality"""

        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        for org_id in org_ids:
            try:
                ou_uuid = ou_helper_object.retrieve_ou_uuid(ou_id=org_id)
                filters = {"ou_ids": [ou_uuid], "interval": "LAST_MONTH"}
                response_data = create_widget_helper_object.create_trellis_score_report(filters=filters)
                executed_flag = True
                executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(response=response_data)
                if not executed_flag:
                    not_executed_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=5)
    def test_ou_score_card_tc003_trellis_score_report_invalid_scores_check(self, create_widget_helper_object,
                                                                           create_generic_object, ou_helper_object,
                                                                           create_customer_object):
        """Validate ou score card - trellis score report functionality - invalid scores check"""

        columns = create_generic_object.api_data["trellis_score_user_columns"]
        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        invalid_ou_data_list = []
        for org_id in org_ids:
            try:
                ou_uuid = ou_helper_object.retrieve_ou_uuid(ou_id=org_id)
                filters = {"ou_ids": [ou_uuid], "interval": "LAST_MONTH"}
                response_data = create_widget_helper_object.create_trellis_score_report(filters=filters)
                executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(response=response_data,
                                                                                               invalid_values_check=True,
                                                                                               columns=columns)
                if not executed_flag:
                    not_executed_list.append(org_id)
                if not valid_data_flag:
                    invalid_ou_data_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info("Invalid Raw stats values OUs List {}".format(set(invalid_ou_data_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(invalid_ou_data_list) == 0, "invalid data OUs- list is {}".format(set(invalid_ou_data_list))
