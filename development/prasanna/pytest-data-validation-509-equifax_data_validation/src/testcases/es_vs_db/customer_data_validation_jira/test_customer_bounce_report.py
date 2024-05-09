import json
import logging
import pytest
import random

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestBounceCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["issue-resolution-across_type"]
    filter_type = generic_object.api_data["constant_filters_jira"]
    filter_type_custom_field = generic_object.get_aggregration_fields(only_custom=True)
    filter_type = filter_type + list(filter_type_custom_field)
    dashboard_partial = ["or", "partial_match"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    def test_bounce_report_es(self, across_type, drilldown_object, create_generic_object, create_customer_object,
                              get_integration_obj):

        project_names = create_generic_object.env["project_names"]

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                required_filters_needs_tobe_applied = ["exclude", "projects"]
                filter_value = [{"statuses": ["CLOSED", "DONE", "RESOLVED"]}, project_names]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])

                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="bounces",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_across=across_type,
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'max',
                                                                                     'total_tickets'], unique_id="across_type with ou id: {}".format(str(across_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))

    @pytest.mark.run(order=2)
    def test_bounce_report_trends_es(self, drilldown_object, create_generic_object, create_customer_object,
                                     get_integration_obj):
        project_names = create_generic_object.env["project_names"]
        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                required_filters_needs_tobe_applied = ["exclude", "projects"]
                filter_value = [{"statuses": ["CLOSED", "DONE", "RESOLVED"]}, project_names]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])
                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="trend",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_across="trend",
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'median',
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
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))

    @pytest.mark.run(order=3)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_bounce_report_es_filters(self, filter_type, drilldown_object, create_generic_object,
                                      create_customer_object,
                                      get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        if "customfield" in filter_type:
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]

        else:
            if filter_type == "statuses":
                filter_option = "status"
            elif filter_type == "priorities":
                filter_option = "priority"
            elif filter_type == "projects":
                filter_option = "project_name"
            elif filter_type == "status_categories":
                filter_option = "status_category"
            else:
                filter_option = filter_type[:-1]

        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        if len(get_filter_response['records'][0][filter_option]) == 0:
            pytest.skip("Filter Doesnot have Any values")
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []

        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            if filter_option == "assignee":
                if eachissueType['additional_key'] != "_UNASSIGNED_":
                    value.append(eachissueType['additional_key'])
            else:
                value.append(eachissueType['key'])

        for val in df_sprint:
            try:
                if "customfield" in filter_option:
                    required_filters_needs_tobe_applied = ["custom_fields"]
                    filter_value = [{filter_option: value}]
                else:
                    required_filters_needs_tobe_applied = [filter_type]
                    filter_value = [value]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])

                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="bounces",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'max',
                                                                                     'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))

    @pytest.mark.run(order=4)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_bounce_report_es_exclude_filters(self, filter_type, drilldown_object, create_generic_object,
                                              create_customer_object,
                                              get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        if "customfield" in filter_type:
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]

        else:
            if filter_type == "statuses":
                filter_option = "status"
            elif filter_type == "priorities":
                filter_option = "priority"
            elif filter_type == "projects":
                filter_option = "project_name"
            elif filter_type == "status_categories":
                filter_option = "status_category"
            else:
                filter_option = filter_type[:-1]

        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        if len(get_filter_response['records'][0][filter_option]) == 0:
            pytest.skip("Filter Doesnot have Any values")
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []

        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            if filter_option == "assignee":
                if eachissueType['additional_key'] != "_UNASSIGNED_":
                    value.append(eachissueType['additional_key'])
            else:
                value.append(eachissueType['key'])

        for val in df_sprint:
            try:
                if "customfield" in filter_option:
                    required_filters_needs_tobe_applied = ["exclude"]
                    filter_value = [{"custom_fields": {filter_option: value}}]
                else:
                    required_filters_needs_tobe_applied = ["exclude"]
                    filter_value = [{filter_type: value}]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])

                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="bounces",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'max',
                                                                                     'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))

    @pytest.mark.run(order=5)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("dashboard_partial", dashboard_partial)
    def test_bounce_report_es_dashboard_partial_filters(self, dashboard_partial, drilldown_object,
                                                        create_generic_object,
                                                        create_customer_object,
                                                        get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        for val in df_sprint:
            try:
                required_filters_needs_tobe_applied = [dashboard_partial]
                if dashboard_partial == "or":
                    filter_value = [{"statuses": ["BACKLOG", "BLOCKED", "DONE"]}]
                else:
                    partial = create_generic_object.env["project_names"][0][:2]
                    filter_value = [{"project": {"$begins": partial}}]
                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])

                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="bounces",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'max',
                                                                                     'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(dashboard_partial)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))

    @pytest.mark.run(order=6)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_bounce_report_trends_es_filters(self, filter_type, drilldown_object, create_generic_object,
                                             create_customer_object,
                                             get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        if "customfield" in filter_type:
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]

        else:
            if filter_type == "statuses":
                filter_option = "status"
            elif filter_type == "priorities":
                filter_option = "priority"
            elif filter_type == "projects":
                filter_option = "project_name"
            elif filter_type == "status_categories":
                filter_option = "status_category"
            else:
                filter_option = filter_type[:-1]

        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        if len(get_filter_response['records'][0][filter_option]) == 0:
            pytest.skip("Filter Doesnot have Any values")
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []

        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            if filter_option == "assignee":
                if eachissueType['additional_key'] != "_UNASSIGNED_":
                    value.append(eachissueType['additional_key'])
            else:
                value.append(eachissueType['key'])

        for val in df_sprint:
            try:
                if "customfield" in filter_option:
                    required_filters_needs_tobe_applied = ["custom_fields"]
                    filter_value = [{filter_option: value}]
                else:
                    required_filters_needs_tobe_applied = [filter_type]
                    filter_value = [value]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])
                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="trend",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_across="trend",
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'median',
                                                                                     'max',
                                                                                     'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))

    @pytest.mark.run(order=7)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_bounce_report_trends_es_exclude_filters(self, filter_type, drilldown_object, create_generic_object,
                                                     create_customer_object,
                                                     get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        if "customfield" in filter_type:
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]

        else:
            if filter_type == "statuses":
                filter_option = "status"
            elif filter_type == "priorities":
                filter_option = "priority"
            elif filter_type == "projects":
                filter_option = "project_name"
            elif filter_type == "status_categories":
                filter_option = "status_category"
            else:
                filter_option = filter_type[:-1]

        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=[filter_option],
                                                                       arg_integration_ids=get_integration_obj)
        if len(get_filter_response['records'][0][filter_option]) == 0:
            pytest.skip("Filter Doesnot have Any values")
        all_filter_records = [get_filter_response['records'][0][filter_option]]
        value = []

        ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
        for eachissueType in ran_value:
            if filter_option == "assignee":
                if eachissueType['additional_key'] != "_UNASSIGNED_":
                    value.append(eachissueType['additional_key'])
            else:
                value.append(eachissueType['key'])

        for val in df_sprint:
            try:
                if "customfield" in filter_option:
                    required_filters_needs_tobe_applied = ["exclude"]
                    filter_value = [{"custom_fields": {filter_option: value}}]
                else:
                    required_filters_needs_tobe_applied = ["exclude"]
                    filter_value = [{filter_type: value}]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])
                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="trend",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_across="trend",
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'median',
                                                                                     'max',
                                                                                     'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))

    @pytest.mark.run(order=8)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("dashboard_partial", dashboard_partial)
    def test_bounce_report_trends_es_dashboard_partial_filters(self, dashboard_partial, drilldown_object,
                                                               create_generic_object,
                                                               create_customer_object,
                                                               get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []

        for val in df_sprint:
            try:
                required_filters_needs_tobe_applied = [dashboard_partial]
                if dashboard_partial == "or":
                    filter_value = [{"statuses": ["BACKLOG", "BLOCKED", "DONE"]}]
                else:
                    partial = create_generic_object.env["project_names"][0][:2]
                    filter_value = [{"project": {"$begins": partial}}]
                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])
                payload = drilldown_object.generate_issues_report_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_product_id=create_generic_object.env["project_id"],
                    arg_id="trend",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_across="trend",
                    arg_ou_id=[val]
                )
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/bounce_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key', 'min',
                                                                                     'median',
                                                                                     'max',
                                                                                     'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(dashboard_partial)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(list_not_match) == 0, "Not matching list is {}".format(set(list_not_match))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
