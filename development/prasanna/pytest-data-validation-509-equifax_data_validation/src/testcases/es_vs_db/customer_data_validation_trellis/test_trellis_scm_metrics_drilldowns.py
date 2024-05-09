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


class TestTrellisSCMFeatureDrilldowns:
    generic_object = TGhelper()
    feature = generic_object.api_data["trellis_scm_metrics_breadth"]
    feature_prs = generic_object.api_data["trellis_scm_metrics_prs"]
    feature_commits = generic_object.api_data["trellis_scm_metrics_commits"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("feature", feature)
    def test_trellis_scm_feature_tech_repo_breadth_drilldowns_es(self, feature, create_generic_object,
                                                                 create_customer_object):

        ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
            arg_config_file_data=create_generic_object.env["dps_test_config"])
        required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))
        zero_list = []
        list_not_match = []
        not_executed_list = []
        columns = create_generic_object.api_data["trellis_proficincy_drilldown_columns"]
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

    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("feature_prs", feature_prs)
    def test_trellis_scm_feature_prs_drilldowns_es(self, feature_prs, create_generic_object,
                                                   create_customer_object):

        ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
            arg_config_file_data=create_generic_object.env["dps_test_config"])
        required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))
        zero_list = []
        list_not_match = []
        not_executed_list = []
        columns = create_generic_object.api_data["trellis_scm_pr_drilldown_columns"]
        for eachrecord in required_tests:
            try:
                userid_and_profile_id = (eachrecord[0]).split(":")
                no_of_months, gt, lt = TPhelper(eachrecord[1])

                filters = {"user_id_type": "ou_user_ids", "user_id_list": [userid_and_profile_id[0]],
                           "feature_name": feature_prs,
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

    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("feature_commits", feature_commits)
    def test_trellis_scm_feature_commits_drilldowns_es(self, feature_commits, create_generic_object,
                                                       create_customer_object):

        ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
            arg_config_file_data=create_generic_object.env["dps_test_config"])
        required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))
        zero_list = []
        list_not_match = []
        not_executed_list = []
        columns = create_generic_object.api_data["trellis_scm_commits_drilldown_columns"]
        for eachrecord in required_tests:
            try:
                userid_and_profile_id = (eachrecord[0]).split(":")
                no_of_months, gt, lt = TPhelper(eachrecord[1])

                filters = {"user_id_type": "ou_user_ids", "user_id_list": [userid_and_profile_id[0]],
                           "feature_name": feature_commits,
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


    @pytest.mark.run(order=4)
    def test_trellis_dev_prod_report_es(self, create_generic_object,
                                        create_customer_object,create_trellis_helper_object):
        ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
            arg_config_file_data=create_generic_object.env["dps_test_config"])
        required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for eachrecord in required_tests:
            try:
                userid_and_profile_id = (eachrecord[0]).split(":")
                no_of_months, gt, lt = TPhelper(eachrecord[1])
                filter_es = create_trellis_helper_object.filter_dev_prod_user(force_source='es', gt=gt, lt=lt,
                                                                       userid=[userid_and_profile_id[0]],
                                                                       profile_id=userid_and_profile_id[1])
                filter_db = create_trellis_helper_object.filter_dev_prod_user(force_source='db', gt=gt, lt=lt,
                                                                       userid=[userid_and_profile_id[0]],
                                                                       profile_id=userid_and_profile_id[1])

                payload_es = {"filter": filter_es, "page": 0, "page_size": 10, "sort": []}
                payload_db = {"filter": filter_db, "page": 0, "page_size": 10, "sort": []}
                LOG.info("payload_es {} ".format(json.dumps(payload_es)))
                LOG.info("payload_db {} ".format(json.dumps(payload_db)))
                es_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                    "dev_prod_user_report"]+"?there_is_no_cache=true"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload_es)

                db_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                    "dev_prod_user_report"]+"?there_is_no_cache=true"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload_db)
                # print(json.dumps(es_response))
                # print(json.dumps(db_response))

                flag, zero_flag = create_customer_object.comparing_es_vs_db_user_reports(es_response, db_response)
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
