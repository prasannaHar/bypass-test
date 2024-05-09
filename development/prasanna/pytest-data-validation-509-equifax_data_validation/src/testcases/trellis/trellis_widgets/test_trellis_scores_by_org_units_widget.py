import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisScoreReport:
    generic_object = TGhelper()
    rbacusertype = generic_object.api_data["trellis_rbac_user_types"]
    req_interval = generic_object.env["trellis_intervals"]

    @pytest.mark.trellisrbac
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_trellis_score_report_by_org_units_tc001_widget_creation_rbac(self, create_widget_helper_object, rbacusertype,
                                                                     create_generic_object, create_customer_object, ou_helper_object):
        """Validate trellis score report by org units functionality"""

        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": "LAST_MONTH"}
                response_data = create_widget_helper_object.create_trellis_scores_by_org_units(filters=filters, user_type=rbacusertype)
                executed_flag = True
                if rbacusertype in ["ADMIN", "PUBLIC_DASHBOARD"]:
                    ## admin and public dashboard users trellis scores access - without trelis scopes access
                    if response_data.status_code != 403: executed_flag = False
                elif rbacusertype in ["ADMIN_TRELLIS_OUs", "ORGADMIN"]:
                    user_email = create_generic_object.retrieve_user_email(user_type=rbacusertype)
                    user_managed_trellis_ous = ou_helper_object.retrieve_user_managed_ous(
                                                    email=user_email, user_managed_trellis_ous="YES")
                    user_non_managed_trellis_ous = ou_helper_object.retrieve_user_managed_ous(
                                                    email=user_email, user_managed_trellis_ous="NO")
                    if len(user_managed_trellis_ous) > 0:
                        ## org admin user trellis scores access - user managed ous - without trellis scopes access
                        filters = {"ou_ref_ids": [user_managed_trellis_ous[0]], "interval": "LAST_MONTH"}
                        response_data = create_widget_helper_object.create_trellis_scores_by_org_units(filters=filters, user_type=rbacusertype)
                        executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(response=response_data)
                    if len(user_non_managed_trellis_ous) > 0:
                        ## org admin user trellis scores access - user non managed ous - without trellis scopes access
                        filters = {"ou_ref_ids": [user_non_managed_trellis_ous[0]], "interval": "LAST_MONTH"}
                        response_data = create_widget_helper_object.create_trellis_scores_by_org_units(filters=filters, user_type=rbacusertype)
                        if response_data.status_code != 403: executed_flag = False
                        
                else:
                    executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(response=response_data)
                if not executed_flag:
                    not_executed_list.append(org_id+" "+rbacusertype)
            except Exception as ex:
                not_executed_list.append(org_id+" "+rbacusertype)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_trellis_score_report_by_org_units_tc001_widget_creation(self, create_widget_helper_object,req_interval,
                                                                     create_generic_object, create_customer_object):
        """Validate trellis score report by org units functionality"""

        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": req_interval}
                response_data = create_widget_helper_object.create_trellis_scores_by_org_units(filters=filters)
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
    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_trellis_score_report_by_org_units_tc002_invalid_scores_check(self,req_interval,
                                                                          create_widget_helper_object,
                                                                          create_generic_object,
                                                                          create_customer_object):
        """Validate trellis score report by org units functionality"""

        columns = create_generic_object.api_data["trellis_score_org_columns"]
        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        invalid_ou_data_list = []
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": req_interval}
                response_data = create_widget_helper_object.create_trellis_scores_by_org_units(filters=filters)
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

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=4)
    @pytest.mark.parametrize("req_interval", req_interval)
    def test_trellis_score_report_by_org_units_tc003_ou_and_associated_ou_scores_check(self,req_interval,
                                                                                       create_widget_helper_object,
                                                                                       create_generic_object,
                                                                                       ou_helper_object,
                                                                                       create_customer_object):
        """Validate trellis score report by org units functionality"""

        org_ids = create_generic_object.env["set_trellis_ous"]
        not_executed_list = []
        invalid_ou_data_list = []
        for org_id in org_ids:
            try:
                filters = {"ou_ref_ids": [org_id], "interval": req_interval}
                response_data = create_widget_helper_object.create_trellis_scores_by_org_units(filters=filters)
                ou_uuid = ou_helper_object.retrieve_ou_uuid(ou_id=org_id)
                child_ou_uuids = ou_helper_object.retrieve_child_ous(ou_id=org_id)
                executed_flag, valid_data_flag = create_customer_object.trellis_data_validator(response=response_data,
                                                                                               child_ous_check=True,
                                                                                               ou_uuid=ou_uuid,
                                                                                               child_ou_uuids=child_ou_uuids)
                if not executed_flag:
                    not_executed_list.append(org_id)
                if not valid_data_flag:
                    invalid_ou_data_list.append(org_id)
            except Exception as ex:
                not_executed_list.append(org_id)

        LOG.info("not executed OUs List {}".format(set(not_executed_list)))
        LOG.info(
            "data validation - OUs and Associated OUs check - incorrect OUs List {}".format(set(invalid_ou_data_list)))
        assert len(not_executed_list) == 0, "not executed OUs- list is {}".format(set(not_executed_list))
        assert len(invalid_ou_data_list) == 0, "invalid data OUs- list is {}".format(set(invalid_ou_data_list))
