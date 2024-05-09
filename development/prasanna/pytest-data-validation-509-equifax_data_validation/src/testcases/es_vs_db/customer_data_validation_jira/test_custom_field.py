import inspect
import json
import logging

import numpy as np
import pandas as pd
import pytest
from flatten_json import flatten_json

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestValueendPoints:
    @pytest.mark.run(order=1)
    def test_custom_field(self, create_generic_object, create_customer_object):
        list_not_match =[]
        zero_list = []
        not_executed_list = []
        value_baseurl = create_generic_object.connection[
                            "base_url"] + create_generic_object.api_data["integration_configs_list"]
        get_integration_obj = create_generic_object.get_integration_id()
        value_payload = {"filter": {"integration_ids": get_integration_obj}}
        values_resp = create_generic_object.execute_api_call(value_baseurl, "post", data=value_payload)
        if values_resp["records"]:
            custom_values = values_resp["records"][0]["config"]["agg_custom_fields"]
            for values in custom_values:
                try:
                    LOG.info("{} - {}".format(values["name"], values["key"]))
                    payload_list = {"integration_ids":get_integration_obj,"fields":[values["key"]],"filter":{"integration_ids":get_integration_obj}}
                    LOG.info("payload - {}".format(json.dumps(payload_list)))
                    es_list_baseurl = create_generic_object.connection[
                                "base_url"] + "jira_issues/custom_field/values" + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_list_baseurl, "post", data=payload_list)

                    db_list_baseurl = create_generic_object.connection[
                                          "base_url"] + "jira_issues/custom_field/values" + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_list_baseurl, "post", data=payload_list)

                    if len(es_response["records"][0][values["key"]]) ==0 and len(db_response["records"][0][values["key"]])==0:
                        zero_list.append(values["name"])
                    elif len(es_response["records"][0][values["key"]]) != len(db_response["records"][0][values["key"]]):
                        key_df = pd.DataFrame([values["name"]])
                        key_df.to_csv(
                            "log_updates/" + str(inspect.stack()[0][3]) + '.csv', header=True,
                            index=False, mode='a')
                        response_df = pd.json_normalize(db_response["records"][0][values["key"]])
                        response_es = pd.json_normalize(es_response["records"][0][values["key"]])
                        val1 = pd.merge(response_df, response_es, on="key",
                                        how='outer', indicator=True)
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].reset_index(drop=True).to_csv(
                                "log_updates/" + str(inspect.stack()[0][3]) + '.csv', header=True, index=False,
                                mode='a')
                        # list_not_match.append(key)
                        list_not_match.append(values["name"])
                except Exception as ex:
                    not_executed_list.append(str(values["name"]))
                    # LOG.info("ex---",ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "Not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=2)
    def test_non_custom_field(self, create_generic_object, create_customer_object):
        list_not_match = []
        zero_list = []
        not_executed_list = []
        get_integration_obj = create_generic_object.get_integration_id()
        non_custom_values = create_generic_object.api_data["non_custom_field"]
        for key, values in non_custom_values.items():
            try:
                LOG.info("{} - {}".format(key, values))
                payload_list = {"integration_ids": get_integration_obj, "fields": [values],
                                "filter": {"integration_ids": get_integration_obj}}
                LOG.info("payload - {}".format(json.dumps(payload_list)))
                es_list_baseurl = create_generic_object.connection[
                                      "base_url"] + "jira_issues/values" + "?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_list_baseurl, "post", data=payload_list)

                db_list_baseurl = create_generic_object.connection[
                                      "base_url"] + "jira_issues/values" + "?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_list_baseurl, "post", data=payload_list)

                if len(es_response["records"][0][values]) == 0 and len(db_response["records"][0][values]) == 0:
                    zero_list.append(key)
                elif len(es_response["records"][0][values]) != len(db_response["records"][0][values]):
                    key_df = pd.DataFrame([key])
                    key_df.to_csv(
                        "log_updates/" + str(inspect.stack()[0][3]) + '.csv', header=True,
                        index=False, mode='a')
                    response_df = pd.json_normalize(db_response["records"][0][values])
                    response_es = pd.json_normalize(es_response["records"][0][values])
                    val1 = pd.merge(response_df, response_es, on="key",
                                    how='outer', indicator=True)
                    if len(val1[val1['_merge'] == 'left_only']) != 0:
                        val1[val1['_merge'] == 'left_only'].reset_index(drop=True).to_csv(
                            "log_updates/" + str(inspect.stack()[0][3]) + '.csv', header=True, index=False, mode='a')
                    list_not_match.append(key)
            except Exception as ex:
                not_executed_list.append(str(key))
                # LOG.info("ex---", ex)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "Not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))
