import json
import logging
import pytest
import pandas as pd
import random

from src.utils.generate_Api_payload import GenericPayload
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

api_payload = GenericPayload()

hygiene_types = ["large stories"]
not_active = ["22\\.2", "22\\.1", "21\\.4"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestHygieneCustomerTenent:
    generic_object = TGhelper()
    filter_type_custom_field = generic_object.get_aggregration_fields(only_custom=True)

    @pytest.mark.run(order=1)
    @pytest.mark.propelslist22
    def test_all_tickets(self, create_generic_object, create_ou_object, create_customer_object, get_integration_obj,
                         filter_type_custom_field=filter_type_custom_field):
        """Validate All tickets in propels"""
        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]

        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                LOG.info("payload {} ".format(json.dumps(payload)))
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=2)
    @pytest.mark.propelslist
    def test_orphan_story(self, create_generic_object, create_ou_object, create_customer_object, get_integration_obj,
                          filter_type_custom_field=filter_type_custom_field):
        """Validate to the Orphan Story  in propels"""
        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                payload["filter"]["missing_fields"] = {"customfield_10001": True}
                LOG.info("payload {} ".format(json.dumps(payload)))
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=3)
    @pytest.mark.propelslist
    def test_missing_SP(self, create_generic_object, create_ou_object, create_customer_object, get_integration_obj,
                        filter_type_custom_field=filter_type_custom_field):
        """Validate to the missing SP  in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                payload["filter"]["missing_fields"] = {"customfield_10008": True}
                LOG.info("payload {} ".format(json.dumps(payload)))
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=4)
    @pytest.mark.propelslist
    def test_missing_fix_version(self, create_generic_object, create_ou_object, create_customer_object,
                                 get_integration_obj, filter_type_custom_field=filter_type_custom_field):
        """Validate to the Missing Fix version  in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                payload["filter"]["exclude"] = {
                    "statuses": ["COMPLETE", "CLOSED", "DONE", "RESOLVED", "WON'T FIX", "COMPLETED"],
                    "custom_fields": {}}
                payload["filter"]["missing_fields"] = {"fix_version": True}
                LOG.info("payload {} ".format(json.dumps(payload)))
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=5)
    @pytest.mark.propelslist
    def test_large_stories(self, create_generic_object, create_ou_object, create_customer_object, get_integration_obj,
                           filter_type_custom_field=filter_type_custom_field):
        """Validate to the Large Stories in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                payload["filter"]["story_points"] = {"$gt": "5"}
                LOG.info("payload {} ".format(json.dumps(payload)))
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=6)
    @pytest.mark.propelslist
    def test_poor_description(self, create_generic_object, create_ou_object, create_customer_object,
                              get_integration_obj, filter_type_custom_field=filter_type_custom_field):
        """Validate to the POOR_DESCRIPTION in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                payload["filter"]["hygiene_types"] = ["POOR_DESCRIPTION"]
                LOG.info("payload {} ".format(json.dumps(payload)))
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=7)
    @pytest.mark.propelslist
    def test_no_components(self, create_generic_object, create_ou_object, create_customer_object, get_integration_obj,
                           filter_type_custom_field=filter_type_custom_field):
        """Validate to the No Components in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                payload["filter"]["hygiene_types"] = ["NO_COMPONENTS"]
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=8)
    @pytest.mark.propelslist
    def test_ac_missing(self, create_generic_object, create_ou_object, create_customer_object, get_integration_obj,
                        filter_type_custom_field=filter_type_custom_field):
        """Validate to the AC Missing in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            value.append(eachissueType['key'])

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                   val)
                payload["filter"]["missing_fields"] = {"customfield_10117": True}
                LOG.info("payload {} ".format(json.dumps(payload)))
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                           sort_column_name="additional_key",
                                                                                           columns=['key',
                                                                                                    'additional_key'
                                                                                                    'median', 'min',
                                                                                                    'max',
                                                                                                    'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=9)
    @pytest.mark.parametrize("hygiene_types", hygiene_types)
    def test_hygine_list_active(self, hygiene_types, create_generic_object, widgetreusable_object, drilldown_object,
                                create_customer_object, get_integration_obj):

        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id", "active"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        eachHygieneType = hygiene_types
        for val in df_sprint.index:
            if str(df_sprint["active"][val]):
                try:
                    if eachHygieneType == "NO_COMPONENTS":
                        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                            arg_required_integration_ids=get_integration_obj,
                            arg_required_hygiene_types=[eachHygieneType],
                            arg_ou_ids=[val],
                            arg_across="project", arg_ou_exclusion="hygiene_types"

                        )
                        widget_payload_generation["filter"]["issue_types"] = ["STORY"]
                        widget_payload_generation["filter"]["last_sprint"] = False
                        widget_payload_generation["filter"]["product_id"] = "2"
                        widget_payload_generation["filter"]["sprint_states"] = ["ACTIVE"]
                    elif eachHygieneType == "POOR_DESCRIPTION":
                        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                            arg_required_integration_ids=get_integration_obj,
                            arg_required_hygiene_types=[eachHygieneType],
                            arg_ou_ids=[val],
                            arg_across="project", arg_ou_exclusion="hygiene_types"
                        )
                        widget_payload_generation["filter"]["issue_types"] = ["STORY"]
                        widget_payload_generation["filter"]["last_sprint"] = False
                        widget_payload_generation["filter"]["product_id"] = "2"
                        widget_payload_generation["filter"]["sprint_states"] = ["ACTIVE"]

                    elif eachHygieneType == "large stories":
                        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                            arg_required_integration_ids=get_integration_obj,
                            arg_ou_ids=[val],
                            arg_across="project", arg_ou_exclusion="hygiene_types"
                        )
                        widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                        widget_payload_generation["filter"]["story_points"] = {"$gt": "5"}
                        widget_payload_generation["filter"]["hygiene_types"] = []

                    elif eachHygieneType == "story points":
                        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                            arg_required_integration_ids=get_integration_obj,
                            arg_ou_ids=[val],
                            arg_across="project", arg_ou_exclusion="hygiene_types"
                        )
                        widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                        widget_payload_generation["filter"]["missing_fields"] = {"customised_10008": True}
                        widget_payload_generation["filter"]["hygiene_types"] = []

                    elif eachHygieneType == "orphan":
                        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                            arg_required_integration_ids=get_integration_obj,
                            arg_ou_ids=[val],
                            arg_across="project", arg_ou_exclusion="hygiene_types"
                        )
                        widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                        widget_payload_generation["filter"]["missing_fields"] = {"customfield_10008": True}
                        widget_payload_generation["filter"]["hygiene_types"] = []

                    elif eachHygieneType == "missing version":
                        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                            arg_required_integration_ids=get_integration_obj,
                            arg_ou_ids=[val],
                            arg_across="project", arg_ou_exclusion="hygiene_types"
                        )
                        widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                        widget_payload_generation["filter"]["missing_fields"] = {"customfield_10008": True}
                        widget_payload_generation["filter"]["hygiene_types"] = []

                    es_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=create_generic_object.connection[
                                        "base_url"] + 'jira_issues/list' + "?there_is_no_cache=true&force_source=es",
                        arg_req_payload=widget_payload_generation
                    )
                    db_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=create_generic_object.connection[
                                        "base_url"] + 'jira_issues/list' + "?there_is_no_cache=true&force_source=db",
                        arg_req_payload=widget_payload_generation
                    )
                    flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                               sort_column_name="assignee",
                                                                                               columns=['id', 'summary',
                                                                                                        'assignee',
                                                                                                        'reporter',
                                                                                                        'epic',
                                                                                                        'created_at',
                                                                                                        'story_points'],
                                                                                               unique_id="hygiene_types with ou id: {}".format(
                                                                                                   str(hygiene_types) + "" + str(
                                                                                                       val)))
                    if not flag:
                        list_not_match.append(val)
                    if not zero_flag:
                        zero_list.append(val)
                except Exception as ex:
                    not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(
            not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
            set(list_not_match))

    @pytest.mark.run(order=10)
    @pytest.mark.parametrize("hygiene_types", hygiene_types)
    def test_hygine_list_not_active(self, hygiene_types, create_generic_object, widgetreusable_object, drilldown_object,
                                    create_customer_object, get_integration_obj):

        df_sprint = pd.read_csv("./testcases/Hygiene_customer_tenent/ous_sprint.csv")
        df_sprint = pd.DataFrame(df_sprint, columns=["ou_id", "active"])
        df_sprint = df_sprint.drop_duplicates()
        zero_list = []
        list_not_match = []
        not_executed_list = []
        eachHygieneType = hygiene_types
        for dat in not_active:
            for val in df_sprint.index:
                if not df_sprint["active"][val]:
                    try:
                        if eachHygieneType == "NO_COMPONENTS":
                            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                                arg_required_integration_ids=get_integration_obj,
                                arg_required_hygiene_types=[eachHygieneType],
                                arg_ou_ids=[val],
                                arg_across="project", arg_ou_exclusion="hygiene_types"

                            )
                            widget_payload_generation["filter"]["issue_types"] = ["STORY"]
                            widget_payload_generation["filter"]["partial_match"] = {
                                "customfield_10000": {"$contains": dat}}
                            widget_payload_generation["filter"]["product_id"] = "2"

                        elif eachHygieneType == "POOR_DESCRIPTION":
                            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                                arg_required_integration_ids=get_integration_obj,
                                arg_required_hygiene_types=[eachHygieneType],
                                arg_ou_ids=[val],
                                arg_across="project", arg_ou_exclusion="hygiene_types"
                            )
                            widget_payload_generation["filter"]["issue_types"] = ["STORY"]
                            widget_payload_generation["filter"]["partial_match"] = {
                                "customfield_10000": {"$contains": dat}}
                            widget_payload_generation["filter"]["product_id"] = "2"

                        elif eachHygieneType == "large stories":
                            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                                arg_required_integration_ids=get_integration_obj,
                                arg_ou_ids=[val],
                                arg_across="project", arg_ou_exclusion="hygiene_types"
                            )
                            widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                            widget_payload_generation["filter"]["partial_match"] = {
                                "customfield_10000": {"$contains": dat}}
                            widget_payload_generation["filter"]["story_points"] = {"$gt": "5"}
                            widget_payload_generation["filter"]["hygiene_types"] = []

                        elif eachHygieneType == "story points":
                            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                                arg_required_integration_ids=get_integration_obj,
                                arg_ou_ids=[val],
                                arg_across="project", arg_ou_exclusion="hygiene_types"
                            )
                            widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                            widget_payload_generation["filter"]["missing_fields"] = {"customised_10008": True}
                            widget_payload_generation["filter"]["hygiene_types"] = []
                            widget_payload_generation["filter"]["partial_match"] = {
                                "customfield_10000": {"$contains": dat}}

                        elif eachHygieneType == "orphan":
                            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                                arg_required_integration_ids=get_integration_obj,
                                arg_ou_ids=[val],
                                arg_across="project", arg_ou_exclusion="hygiene_types"
                            )
                            widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                            widget_payload_generation["filter"]["missing_fields"] = {"customfield_10008": True}
                            widget_payload_generation["filter"]["hygiene_types"] = []
                            widget_payload_generation["filter"]["partial_match"] = {
                                "customfield_10000": {"$contains": dat}}

                        elif eachHygieneType == "missing version":
                            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                                arg_required_integration_ids=get_integration_obj,
                                arg_ou_ids=[val],
                                arg_across="project", arg_ou_exclusion="hygiene_types"
                            )
                            widget_payload_generation["filter"]["exclude"] = {"custom_fields": {}}
                            widget_payload_generation["filter"]["missing_fields"] = {"customfield_10008": True}
                            widget_payload_generation["filter"]["hygiene_types"] = []

                        es_response = widgetreusable_object.retrieve_required_api_response(
                            arg_req_api=create_generic_object.connection[
                                            "base_url"] + 'jira_issues/list' + "?there_is_no_cache=true&force_source=es",
                            arg_req_payload=widget_payload_generation
                        )
                        db_response = widgetreusable_object.retrieve_required_api_response(
                            arg_req_api=create_generic_object.connection[
                                            "base_url"] + 'jira_issues/list' + "?there_is_no_cache=true&force_source=db",
                            arg_req_payload=widget_payload_generation
                        )

                        flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                                   sort_column_name="assignee",
                                                                                                   columns=['id', 'summary',
                                                                                                            'assignee',
                                                                                                            'reporter',
                                                                                                            'epic',
                                                                                                            'created_at',
                                                                                                            'story_points'],
                                                                                                   unique_id="hygiene_types with ou id: {}".format(
                                                                                                       str(hygiene_types) + "" + str(
                                                                                                           val)))
                        if not flag:
                            list_not_match.append(val)
                        if not zero_flag:
                            zero_list.append(val)
                    except Exception as ex:
                        not_executed_list.append(val)

                LOG.info("No data list {}".format(set(zero_list)))
                LOG.info("Not match list {}".format(list_not_match))
                LOG.info("not executed List {}".format(set(not_executed_list)))
                assert len(
                    not_executed_list) == 0, "OU is not executed- for Across : list is {}".format(
                    set(not_executed_list))
                assert len(list_not_match) == 0, " Not Matching List- for Across : {}".format(
                    set(list_not_match))
