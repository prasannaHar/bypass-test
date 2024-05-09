import json
import logging
import pandas as pd
import pytest
import random
import itertools

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.utils.dev_prod_reusable_functions import dev_prod_fetch_input_data
from src.lib.core_reusable_functions import epoch_timeStampsGenerationForRequiredTimePeriods as TPhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisJiraFeatureDrilldowns:
    generic_object = TGhelper()
    feature = generic_object.api_data["trellis_jira_metrics"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("feature", feature)
    def test_trellis_jira_feature_drilldowns_es(self, feature, create_generic_object,
                                                create_customer_object):

        ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
            arg_config_file_data=create_generic_object.env["dps_test_config"])
        required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))
        zero_list = []
        list_not_match = []
        not_executed_list = []
        feature_specific_columns = ["story_points_portion", "ticket_portion"]
        if feature == "Average time spent working on Issues": feature_specific_columns = ["assignee_time"]
        columns = create_generic_object.api_data["trellis_jira_drilldown_columns"]
        columns.extend(feature_specific_columns)
        for eachrecord in required_tests:
            try:
                userid_and_profile_id = (eachrecord[0]).split(":")
                no_of_months, gt, lt = TPhelper(eachrecord[1])

                filters = {"user_id_type": "ou_user_ids", "user_id_list": [userid_and_profile_id[0]],
                           "feature_name": feature,
                           "time_range": {"$gt": str(gt), "$lt": str(lt)}, "partial": {},
                           "dev_productivity_profile_id": userid_and_profile_id[1]}
                payload = {"filter": filters, "sort": [], "across": ""}
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                    "trellis_feature_list"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                    "trellis_feature_list"] + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)

                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix_trellis(es_response,
                                                                                                   db_response,
                                                                                                   sort_column_name='id',
                                                                                                   columns=columns
                                                                                                   )
                if not flag:
                    list_not_match.append(eachrecord)
                if not zero_flag:
                    zero_list.append(eachrecord)
            except Exception as ex:
                not_executed_list.append(eachrecord)
                LOG.info(ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))
