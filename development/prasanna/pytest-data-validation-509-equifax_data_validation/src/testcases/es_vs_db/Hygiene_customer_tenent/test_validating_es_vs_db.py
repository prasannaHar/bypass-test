import logging
import pytest
import pandas as pd
from src.utils.generate_Api_payload import GenericPayload
import random
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper



api_payload = GenericPayload()

hygiene_types = ["NO_COMPONENTS","POOR_DESCRIPTION", "large stories", "story points", "orphan",
                 "missing version"]
not_active = ["22\\.2", "22\\.1", "21\\.4"]
status_code = ["Active", "Inactive"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestCustomerTenent:
    generic_object = TGhelper()
    filter_type_custom_field = generic_object.get_aggregration_fields(only_custom=True)

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("status_code", status_code)
    def test_all_tickets(self, status_code, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + value)
    
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("status_code", status_code)
    def test_orphan_story(self, status_code,create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):
        """Validate to the Orphan Story  in propels"""
        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    payload["filter"]["missing_fields"] = {"customfield_10001": True}
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
    
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + "/" + value)
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    @pytest.mark.run(order=3)
    @pytest.mark.parametrize("status_code", status_code)
    def test_missing_SP(self, status_code,create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):
        """Validate to the missing SP  in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    payload["filter"]["missing_fields"] = {"customfield_10008": True}
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
    
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + "/" + value)
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    @pytest.mark.run(order=4)
    @pytest.mark.parametrize("status_code", status_code)
    def test_missing_fix_version(self, status_code,create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):
        """Validate to the Missing Fix version  in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    payload["filter"]["exclude"] = {
                        "statuses": ["COMPLETE", "CLOSED", "DONE", "RESOLVED", "WON'T FIX", "COMPLETED"],
                        "custom_fields": {}}
                    payload["filter"]["missing_fields"] = {"fix_version": True}
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
    
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + "/" + value)
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    @pytest.mark.run(order=5)
    @pytest.mark.parametrize("status_code", status_code)
    def test_large_stories(self, status_code,create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):
        """Validate to the Large Stories in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    payload["filter"]["story_points"] = {"$gt": "5"}
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
    
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + "/" + value)
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    @pytest.mark.run(order=6)
    @pytest.mark.parametrize("status_code", status_code)
    def test_poor_description(self, status_code,create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):
        """Validate to the POOR_DESCRIPTION in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    payload["filter"]["status_code"] = ["POOR_DESCRIPTION"]
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
    
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + "/" + value)
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    @pytest.mark.run(order=7)
    @pytest.mark.parametrize("status_code", status_code)
    def test_no_components(self, status_code, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):
        """Validate to the No Components in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    payload["filter"]["status_code"] = ["NO_COMPONENTS"]
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
    
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + "/" + value)
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    @pytest.mark.run(order=8)
    @pytest.mark.parametrize("status_code", status_code)
    def test_ac_missing(self, status_code, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj,filter_type_custom_field=filter_type_custom_field):
        """Validate to the AC Missing in propels"""

        filter_option = [x.split("-") for x in filter_type_custom_field if "Sprint" in x]
        filter_option = filter_option[0][1]
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []
        for eachissueType in all_filter_records[0]:
            value.append(eachissueType['key'])
        value = random.sample(value, min(3, len(value)))

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        for dat in not_active:
            for val in df_sprint:
                try:
                    payload = api_payload.generate_customer_all_ticket(get_integration_obj, value,
                                                                       val)
                    if status_code == "Active":
                        payload["filter"]["sprint_states"] = ["ACTIVE"]
                    elif status_code == "Inactive":
                        payload["filter"]["partial_match"] = {
                            "customfield_10000": {"$contains": dat}}
                    payload["filter"]["missing_fields"] = {"customfield_10117": True}
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
    
                    base_url = create_generic_object.connection[
                                   "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                    if len(es_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-es")
                    else:
                        es_df = pd.json_normalize(es_response['records'])
                        es_df = es_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        es_df = es_df.sort_values('additional_key')
                        es_df = pd.DataFrame(es_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                    if len(db_response['records']) == 0:
                        zero_list.append(val + "/" + value + "-db")
                    else:
                        db_df = pd.json_normalize(db_response['records'])
                        db_df = db_df.applymap(lambda s: s.strip() if type(s) == str else s)
                        db_df = db_df.sort_values('additional_key')
                        db_df = pd.DataFrame(db_df, columns=['additional_key', 'mean_story_points', 'total_tickets',
                                                             'total_story_points', 'key'])
                        val1 = pd.merge(es_df, db_df,
                                        on=['additional_key', 'mean_story_points', 'total_tickets', 'total_story_points',
                                            'key'],
                                        how='outer', indicator=True)
                        LOG.info("es data - {}".format(val1[val1['_merge'] == 'right_only']))
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                    if es_response["count"] != db_response["count"]:
                        list_not_match.append(val + "/" + value)
                except Exception as ex:
                    not_executed_list.append(val + "/" + value)
            LOG.info("No data list {}".format(zero_list))
            LOG.info("not executed List {}".format(not_executed_list))
            assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
            assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)
