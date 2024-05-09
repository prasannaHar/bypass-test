import json
import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCMValueendPoints:

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=1)
    def test_non_custom_prs_field(self, create_generic_object, create_customer_object):
        list_not_match = []
        zero_list = []
        not_executed_list = []
        get_integration_obj = create_generic_object.get_integration_id()
        non_custom_values = create_generic_object.api_data["SCM_custom_field_prs"]
        for key, values in non_custom_values.items():
            try:
                LOG.info("{} - {}".format(key, values))
                payload_list = {"integration_ids": get_integration_obj, "fields": [values],
                                "filter": {"integration_ids": get_integration_obj}}
                LOG.info("payload - {}".format(json.dumps(payload_list)))
                es_list_baseurl = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_non_custom_field"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_list_baseurl, "post", data=payload_list)

                db_list_baseurl = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_non_custom_field"] + "?there_is_no_cache=true&force_source=db "
                db_response = create_generic_object.execute_api_call(db_list_baseurl, "post", data=payload_list)

                if len(es_response["records"][0][values]) != len(db_response["records"][0][values]):
                    zero_list.append(key)
                elif es_response["records"][0][values] != db_response["records"][0][values]:
                    list_not_match.append(key)
            except Exception as ex:
                not_executed_list.append(str(key))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "Not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))
        assert len(zero_list) == 0, "No values returned from api -{}".format(set(zero_list))

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=2)
    def test_non_custom_commit_field(self, create_generic_object, create_customer_object):
        list_not_match = []
        zero_list = []
        not_executed_list = []
        get_integration_obj = create_generic_object.get_integration_id()
        non_custom_values = create_generic_object.api_data["SCM_custom_field_commit"]
        for key, values in non_custom_values.items():
            try:
                LOG.info("{} - {}".format(key, values))
                payload_list = {"integration_ids": get_integration_obj, "fields": [values],
                                "filter": {"integration_ids": get_integration_obj}}
                LOG.info("payload - {}".format(json.dumps(payload_list)))
                es_list_baseurl = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issue_values"] + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_list_baseurl, "post", data=payload_list)

                db_list_baseurl = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_issue_values"] + "?there_is_no_cache=true&force_source=db "
                db_response = create_generic_object.execute_api_call(db_list_baseurl, "post", data=payload_list)

                if len(es_response["records"][0][values]) != len(db_response["records"][0][values]):
                    zero_list.append(key)
                elif es_response["records"][0][values] != db_response["records"][0][values]:
                    list_not_match.append(key)
            except Exception as ex:
                not_executed_list.append(str(key))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "Not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=3)
    def test_values_field_returning_no_values(self, create_generic_object, create_customer_object):
        list_not_match = []
        zero_list = []
        not_executed_list = []
        # intergration_name=create_generic_object.env["scm-intergration_name"]
        # breakpoint()
        get_integration_obj = create_generic_object.get_integration_list(application_name="github")
        non_custom_values = create_generic_object.api_data["SCM_custom_field_prs"]
        for key, values in non_custom_values.items():
            try:
                LOG.info("{} - {}".format(key, values))
                payload_list = {"integration_ids": get_integration_obj, "fields": [values],
                                "filter": {"integration_ids": get_integration_obj}}
                LOG.info("payload - {}".format(json.dumps(payload_list)))
                es_list_baseurl = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data["scm_non_custom_field"]
                response = create_generic_object.execute_api_call(es_list_baseurl, "post", data=payload_list)
                LOG.info("Response -----{}".format(response))
                LOG.info("response[records][0][values]---{}".format(response["records"][0][values]))
                if response["records"][0][values]==[]:
                    zero_list.append(key)

            except Exception as ex:
                not_executed_list.append(str(key))

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "Not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))
        assert len(zero_list) == 0, "No values returned from api -{}".format(set(zero_list))
