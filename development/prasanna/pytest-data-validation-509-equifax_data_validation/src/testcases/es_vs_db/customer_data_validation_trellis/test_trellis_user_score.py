import json
import logging
from copy import deepcopy
import pytest
import itertools
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.utils.dev_prod_reusable_functions import dev_prod_fetch_input_data

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisSCMFeatureDrilldowns:
    generic_object = TGhelper()
    ou_ids = generic_object.env["trellis_ou_ids"]
    intervals = generic_object.env["trellis_intervals"]
    ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
        arg_config_file_data=generic_object.env["dps_test_config"])
    required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))

    @pytest.mark.run(order=1)
    # @pytest.mark.parametrize("feature", feature)
    @pytest.mark.parametrize("interval", intervals)
    @pytest.mark.parametrize("required_test", required_tests)
    def test_trellis_dev_es(self, required_test, interval, create_generic_object, create_customer_object):

        zero_list = []
        list_not_match = []
        not_executed_list = []

        # breakpoint()

        try:
            userid_and_profile_id = (required_test[0]).split(":")

            filters = {"interval": interval,
                       "dev_productivity_profile_id": userid_and_profile_id[1]}

            payload = {"filter": filters, "page": 0,
                       "page_size": 100}
            LOG.info("payload {} ".format(json.dumps(payload)))
            es_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "trellis_reports_fixed_intervals_users"] + userid_and_profile_id[
                              0] + "?there_is_no_cache=true&force_source=es"
            es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

            db_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "trellis_reports_fixed_intervals_users"] + userid_and_profile_id[
                              0] + "?there_is_no_cache=true&force_source=db"
            db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)

            if len(es_response) == 0 and len(db_response) == 0:
                LOG.info("ES and db not having data")
                zero_flag = False

            elif len(es_response) == 0 :
                LOG.info("ES not having data")
                zero_flag = False

            else:
                flag, zero_flag = create_customer_object.comparing_es_vs_db_user_reports(db_response, es_response)
                # breakpoint()

            if not flag:
                list_not_match.append(required_test)
            if not zero_flag:
                zero_list.append(required_test)

        except Exception as ex:
            not_executed_list.append(required_test)
            LOG.info(ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.parametrize("ou_id", ou_ids)
    @pytest.mark.parametrize("interval", intervals)
    def test_individual_raw_stats_es_vs_db(self, ou_id, interval, create_generic_object, create_customer_object):
        ou_user_time_periods, ou_user_ids_list = dev_prod_fetch_input_data(
            arg_config_file_data=create_generic_object.env["dps_test_config"])
        required_tests = list(itertools.product(ou_user_ids_list, ou_user_time_periods))
        zero_list = []
        list_not_match = []
        not_executed_list = []
        # ou_id = '10'
        # interval = "LAST_MONTH"

        try:

            filters = {"interval": interval,
                       "ou_ref_ids": [ou_id]}

            """"filter":{"ou_ref_ids":["45"],"interval":"LAST_MONTH"}"""
            payload = {"filter": filters}
            LOG.info("payload {} ".format(json.dumps(payload)))
            es_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "individual_raw_stats"] + "?there_is_no_cache=true&force_source=es"
            es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

            db_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "individual_raw_stats"] + "?there_is_no_cache=true&force_source=db"
            db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)

            flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix_trellis_1(db_response,
                                                                                                 es_response,
                                                                                                 unique_identifier="ou_id: " + ou_id + "-interval: " + interval)

            if not flag:
                list_not_match.append(ou_id)
            if not zero_flag:
                zero_list.append(ou_id)
        except Exception as ex:
            not_executed_list.append(ou_id)
            LOG.info(ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.parametrize("ou_id", ou_ids)
    @pytest.mark.parametrize("interval", intervals)
    def test_fixed_intervals_org_list_es_vs_db(self, ou_id, interval, create_generic_object, create_customer_object):
        # ou_id = '45'
        # interval = "LAST_MONTH"
        zero_list = []
        list_not_match = []
        not_executed_list = []

        try:

            filters = {"interval": interval,
                       "ou_ref_ids": [ou_id]}

            """"filter":{"ou_ref_ids":["45"],"interval":"LAST_MONTH"}"""
            payload = {"filter": filters}
            LOG.info("payload {} ".format(json.dumps(payload)))
            es_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "trellis_scores_by_org_unit_report"] + "?there_is_no_cache=true&force_source=es"
            es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

            db_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "trellis_scores_by_org_unit_report"] + "?there_is_no_cache=true&force_source=db"
            db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)

            if len(es_response['records'][0]) == 0 and len(db_response['records'][0]) == 0:
                LOG.info("ES and DB not having data")
                zero_flag = False

            elif len(es_response['records'][0]) == 0 and len(db_response['records'][0]) != 0:
                LOG.info("ES  not having data")
                zero_flag = False
            else:
                flag, zero_flag = create_customer_object.comparing_es_vs_db_user_reports(db_response, es_response,
                                                                                         unique_identifier="ou_id: " + ou_id + "-interval: " + interval)

            if not flag:
                list_not_match.append(ou_id)
            if not zero_flag:
                zero_list.append(ou_id)
        except Exception as ex:
            not_executed_list.append(ou_id)
            LOG.info(ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.parametrize("ou_id", ou_ids)
    @pytest.mark.parametrize("interval", intervals)
    def test_fixed_intervals_raw_stat_orgs_es_vs_db(self, ou_id, interval, create_generic_object,
                                                    create_customer_object):
        # ou_id = '45'
        # interval = "LAST_MONTH"
        zero_list = []
        list_not_match = []
        not_executed_list = []

        try:

            filters = {"interval": interval,
                       "ou_ref_ids": [ou_id]}

            """"filter":{"ou_ref_ids":["45"],"interval":"LAST_MONTH"}"""
            payload = {"filter": filters}
            LOG.info("payload {} ".format(json.dumps(payload)))
            es_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "raw_stats_by_org_units"] + "?there_is_no_cache=true&force_source=es"
            es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

            db_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "raw_stats_by_org_units"] + "?there_is_no_cache=true&force_source=db"
            db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)

            if len(es_response['records'][0]) == 0 and len(db_response['records'][0]) == 0:
                LOG.info("ES and DB not having data")
                zero_flag = False

            elif len(es_response['records'][0]) == 0 and len(db_response['records'][0]) != 0:
                LOG.info("ES  not having data")
                zero_flag = False
            else:

                flag, zero_flag = create_customer_object.comparing_es_vs_db_user_reports(db_response, es_response,
                                                                                         unique_identifier="ou_id: " + ou_id + "-interval: " + interval)

            if not flag:
                list_not_match.append(ou_id)
            if not zero_flag:
                zero_list.append(ou_id)
        except Exception as ex:
            not_executed_list.append(ou_id)
            LOG.info(ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))

    @pytest.mark.parametrize("ou_id", ou_ids)
    @pytest.mark.parametrize("interval", intervals)
    def test_fixed_intervals_user_list_es_vs_db(self, ou_id, interval, create_generic_object,
                                                create_customer_object):
        # ou_id = '33'
        # interval = "LAST_MONTH"
        zero_list = []
        list_not_match = []
        not_executed_list = []

        try:

            filters = {"interval": interval,
                       "ou_ref_ids": [ou_id],
                       "ou_ids": [],
                       "user_id_type": "ou_user_ids", "user_id_list": []}

            """{"page":0,"page_size":9,"sort":null,"filter":{"interval":"LAST_MONTH","ou_ids":[],"ou_ref_ids":["10"],
            "user_id_type":"ou_user_ids","user_id_list":[]}}"""

            payload = {"filter": filters}
            LOG.info("payload {} ".format(json.dumps(payload)))
            # breakpoint()
            es_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "trellis_scores_report"] + "?there_is_no_cache=true&force_source=es"
            es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
            es_str = json.dumps(es_response, sort_keys=True)
            es_response = json.loads(es_str)
            db_base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
                "trellis_scores_report"] + "?there_is_no_cache=true&force_source=db"
            db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
            db_str = json.dumps(db_response, sort_keys=True)
            db_response = json.loads(db_str)

            sorted_db_response = deepcopy(db_response)
            sorted_db_response['records'] = sorted(db_response['records'], key=lambda k: k['full_name'], reverse=True)
            sorted_es_response = deepcopy(es_response)
            sorted_es_response['records'] = sorted(es_response['records'], key=lambda k: k['full_name'], reverse=True)

            if len(es_response['records'][0]) == 0 and len(db_response['records'][0]) == 0:
                LOG.info("ES and DB not having data")
                zero_flag = False

            elif len(es_response['records'][0]) == 0 and len(db_response['records'][0]) != 0:
                LOG.info("ES  not having data")
                zero_flag = False
            else:

                flag, zero_flag = create_customer_object.comparing_es_vs_db_user_reports(sorted_db_response,
                                                                                         sorted_es_response,
                                                                                         unique_identifier="ou_id: " + ou_id + "-interval: " + interval)

            if not flag:
                list_not_match.append(ou_id)
            if not zero_flag:
                zero_list.append(ou_id)
        except Exception as ex:
            not_executed_list.append(ou_id)
            LOG.info(ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed-: " " list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-:" + "  {}".format(
            set(list_not_match))
