import logging
import pytest

from src.utils.dev_prod_reusable_functions import dev_prod_fetch_input_data
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.utils.dev_prod_reusable_functions import retrieve_dev_user_profile_api_response as trellisresponse

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisScoreReport:
    generic_object = TGhelper()
    rbacusertype = generic_object.api_data["trellis_rbac_user_types"]

    @pytest.mark.trellisrbac
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_trellis_user_score_card_tc001_relative_score_rbac(self, create_widget_helper_object,
                                                          create_generic_object, rbacusertype):
        """Validate relative score widget functionality"""

        LOG.info("==== create widget with available filter ====")
        ou_user_time_periods, ou_user_ids_list = \
            dev_prod_fetch_input_data(arg_config_file_data=create_generic_object.env["dps_test_config"])
        user_id, profile_id = (ou_user_ids_list[0]).split(":")
        filters = {"report_requests": [{"id_type": "ou_user_ids", "id": user_id}], "agg_interval": "month",
                   "dev_productivity_profile_id": profile_id}
        relative_score_widget = create_widget_helper_object.create_trellis_relative_score(filters=filters, user_type=rbacusertype)
        if rbacusertype in ["ADMIN", "PUBLIC_DASHBOARD"]:
            assert relative_score_widget.status_code == 403, "Relative score widget access should be denied for ADMIN, PUBLIC_DASHBOARD users"
        else:
            assert relative_score_widget, "widget is not created"

    @pytest.mark.trellisrbac
    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_trellis_user_score_card_tc002_pr_activity_rbac(self, create_widget_helper_object,
                                                       create_generic_object, rbacusertype):
        """Validate pr activity widget functionality"""

        LOG.info("==== create widget with available filter ====")
        ou_user_time_periods, ou_user_ids_list = \
            dev_prod_fetch_input_data(arg_config_file_data=create_generic_object.env["dps_test_config"])
        user_id, profile_id = (ou_user_ids_list[0]).split(":")
        gt, lt = create_generic_object.get_epoc_time(value=4, type="days")
        filters = {"user_id_type": "ou_user_ids", "user_id": user_id, "time_range": {"$gt": gt, "$lt": lt},
                   "dev_productivity_profile_id": profile_id}
        pr_activity_widget = create_widget_helper_object.create_trellis_pr_activity(filters=filters, user_type=rbacusertype)
        if rbacusertype in ["ADMIN", "PUBLIC_DASHBOARD"]:
            assert pr_activity_widget.status_code == 403, "PR activity widget access should be denied for ADMIN, PUBLIC_DASHBOARD users"
        else: 
            assert pr_activity_widget, "widget is not created"

    @pytest.mark.trellisrbac
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_trellis_user_score_card_tc003_developer_metrics_rbac(self, create_generic_object, rbacusertype):
        """Validate relative score widget functionality"""

        LOG.info("==== create widget with available filter ====")
        ou_user_time_periods, ou_user_ids_list = \
            dev_prod_fetch_input_data(arg_config_file_data=create_generic_object.env["dps_test_config"])
        user_id, profile_id = (ou_user_ids_list[0]).split(":")

        dev_productivity_user_overview = trellisresponse(
            application_url=create_generic_object.connection["base_url"], 
            ou_user_id=user_id,
            ou_associated_trelis_profile_id=profile_id, 
            timeperiod_interval=ou_user_time_periods[0],
            user_type=rbacusertype)
        if rbacusertype in ["ADMIN", "PUBLIC_DASHBOARD"]:
            assert dev_productivity_user_overview.status_code == 403, "Relative score widget access should be denied for ADMIN, PUBLIC_DASHBOARD users"
        else:
            assert dev_productivity_user_overview, "failed to retrieve trellis response"

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=4)
    def test_trellis_user_score_card_tc001_relative_score(self, create_widget_helper_object,
                                                          create_generic_object):
        """Validate relative score widget functionality"""

        LOG.info("==== create widget with available filter ====")
        ou_user_time_periods, ou_user_ids_list = \
            dev_prod_fetch_input_data(arg_config_file_data=create_generic_object.env["dps_test_config"])
        user_id, profile_id = (ou_user_ids_list[0]).split(":")
        filters = {"report_requests": [{"id_type": "ou_user_ids", "id": user_id}], "agg_interval": "month",
                   "dev_productivity_profile_id": profile_id}
        relative_score_widget = create_widget_helper_object.create_trellis_relative_score(filters=filters)

        assert relative_score_widget, "widget is not created"

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=5)
    def test_trellis_user_score_card_tc002_pr_activity(self, create_widget_helper_object,
                                                       create_generic_object):
        """Validate pr activity widget functionality"""

        LOG.info("==== create widget with available filter ====")
        ou_user_time_periods, ou_user_ids_list = \
            dev_prod_fetch_input_data(arg_config_file_data=create_generic_object.env["dps_test_config"])
        user_id, profile_id = (ou_user_ids_list[0]).split(":")
        gt, lt = create_generic_object.get_epoc_time(value=4, type="days")
        filters = {"user_id_type": "ou_user_ids", "user_id": user_id, "time_range": {"$gt": gt, "$lt": lt},
                   "dev_productivity_profile_id": profile_id}
        pr_activity_widget = create_widget_helper_object.create_trellis_pr_activity(filters=filters)

        assert pr_activity_widget, "widget is not created"

    @pytest.mark.trellistcs
    @pytest.mark.trellistcsreadonly
    @pytest.mark.run(order=6)
    def test_trellis_user_score_card_tc003_developer_metrics(self, create_generic_object):
        """Validate relative score widget functionality"""

        LOG.info("==== create widget with available filter ====")
        ou_user_time_periods, ou_user_ids_list = \
            dev_prod_fetch_input_data(arg_config_file_data=create_generic_object.env["dps_test_config"])
        user_id, profile_id = (ou_user_ids_list[0]).split(":")

        dev_productivity_user_overview = trellisresponse(
            application_url=create_generic_object.connection["base_url"], 
            ou_user_id=user_id,
            ou_associated_trelis_profile_id=profile_id, 
            timeperiod_interval=ou_user_time_periods[0])
        
        assert dev_productivity_user_overview, "failed to retrieve trellis response"

