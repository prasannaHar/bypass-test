import json
import logging
import pandas as pd
import pytest
import random

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesCustomer:
    generic_object = TGhelper()
    across_type = generic_object.api_data["issue-resolution-across_type"]
    across_type_ss = generic_object.api_data["issue-resolution-across_type_ss"]
    filter_type = generic_object.api_data["constant_filters_jira"]
    filter_type_custom_field = generic_object.get_aggregration_fields(only_custom=True)
    filter_type = filter_type + list(filter_type_custom_field)
    dashboard_partial = ["or", "partial_match"]

    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", across_type)
    def test_issue_resolution_time_report_es(self, across_type, drilldown_object, create_generic_object,
                                            create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        gt, lt = create_generic_object.get_epoc_time(value=4)
        for val in df_sprint:
            try:
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]

                filters = {"projects": create_generic_object.env["project_names"], "statuses": ["DONE", "CLOSED", "RESOLVED"],
                           "issue_created_at": {"$gt": gt, "$lt": lt},
                           "exclude": {"issue_types": ["EPIC"]}, "product_id": create_generic_object.env["project_id"],
                           "integration_ids": get_integration_obj}

                payload = {"filter": filters, "across": across_type,
                           "across_limit": 20, "ou_ids": [val],
                           }

                if len(interval_val) != 0:
                    payload["interval"] = interval_val
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="across_type with ou id: {}".format(str(across_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("across_type_ss", across_type_ss)
    def test_issue_resolution_time_single_stat_report_es(self, across_type_ss, drilldown_object, create_generic_object,
                                                         create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
        for val in df_sprint:
            for each_agg_type in agg_type:
                try:
                    filters = {"agg_type": each_agg_type, across_type_ss + "_at": {"$gt": gt, "$lt": lt},
                               "projects": create_generic_object.env["project_names"],
                               "product_id": create_generic_object.env["project_id"],
                               "integration_ids": get_integration_obj}

                    payload = {"filter": filters, "across": across_type_ss,
                               "ou_ids": [val],
                               }
                    LOG.info("payload {} ".format(json.dumps(payload)))
                    es_base_url = create_generic_object.connection[
                                      "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                                columns=['key', 'additional_key',
                                                                                         'median', "min", "max", "mean",
                                                                                         "p90", 'total_tickets'], unique_id="across_type_ss with ou id: {}".format(str(across_type_ss)+" "+ str(val)))
                    if not flag:
                        list_not_match.append(val)
                    if not zero_flag:
                        zero_list.append(val)
                except Exception as ex:
                    not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=3)
    def test_issues_resolution_time_trend_report_es(self, create_generic_object,
                                                    create_customer_object, get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
        for val in df_sprint:
            try:
                filters = {"projects": create_generic_object.env["project_names"], "statuses": ["DONE", "CLOSED", "RESOLVED"],
                           "issue_created_at": {"$gt": gt, "$lt": lt},
                           "exclude": {"issue_types": ["EPIC"]}, "product_id": create_generic_object.env["project_id"],
                           "integration_ids": get_integration_obj}
                payload = {"filter": filters, "across": "trend",
                           "across_limit": 20, "ou_ids": [val],
                           }
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="ou_id is :"+val)
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)
        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=4)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_issue_resolution_time_report_es_filters(self, filter_type, drilldown_object, create_generic_object,
                                                     create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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

                filters = {"metric": ["median_resolution_time", "number_of_tickets_closed"],
                           "sort_xaxis": "value_high-low",
                           "issue_created_at": {"$gt": gt, "$lt": lt}, "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "assignee",
                           "across_limit": 20, "ou_ids": [val]
                           }
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true" \
                                                "&force_source=es "
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true" \
                                                "&force_source=db "
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=5)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_issue_resolution_time_report_es_exclude_filters(self, filter_type, drilldown_object, create_generic_object,
                                                             create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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

                filters = {"metric": ["median_resolution_time", "number_of_tickets_closed"],
                           "sort_xaxis": "value_high-low",
                           "issue_created_at": {"$gt": gt, "$lt": lt}, "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "assignee",
                           "across_limit": 20, "ou_ids": [val]
                           }
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=6)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("dashboard_partial", dashboard_partial)
    def test_issue_resolution_time_report_es_dashboard_partial_filters(self, dashboard_partial, drilldown_object,
                                                                       create_generic_object,
                                                                       create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)

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

                filters = {"metric": ["median_resolution_time", "number_of_tickets_closed"],
                           "sort_xaxis": "value_high-low",
                           "issue_created_at": {"$gt": gt, "$lt": lt}, "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "assignee",
                           "across_limit": 20, "ou_ids": [val]
                           }
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(dashboard_partial)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=7)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_issue_resolution_time_single_stat_report_es_filters(self, filter_type, drilldown_object,
                                                                 create_generic_object,
                                                                 create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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

                filters = {"agg_type": "average", "issue_created_at": {"$gt": gt, "$lt": lt},
                           "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "issue_created",
                           "ou_ids": [val]}

                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=8)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_issue_resolution_time_single_stat_report_es_exclude_filters(self, filter_type, drilldown_object,
                                                                         create_generic_object,
                                                                         create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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

                filters = {"agg_type": "average", "issue_created_at": {"$gt": gt, "$lt": lt},
                           "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "issue_created",
                           "ou_ids": [val]}

                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=9)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("dashboard_partial", dashboard_partial)
    def test_issue_resolution_time_single_stat_report_es_dashboard_partial_filters(self, dashboard_partial,
                                                                                   drilldown_object,
                                                                                   create_generic_object,
                                                                                   create_customer_object,
                                                                                   get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]

        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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

                filters = {"agg_type": "average", "issue_created_at": {"$gt": gt, "$lt": lt},
                           "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "issue_created",
                           "ou_ids": [val]}

                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(dashboard_partial)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=10)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_issues_resolution_time_trend_report_es_filters(self, filter_type, create_generic_object,
                                                            create_customer_object, get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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
                filters = {"issue_created_at": {"$gt": gt, "$lt": lt},
                           "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "trend",
                           "across_limit": 20, "ou_ids": [val],
                           }
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)
        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=11)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("filter_type", filter_type)
    def test_issues_resolution_time_trend_report_es_exclude_filters(self, filter_type, create_generic_object,
                                                                    create_customer_object, get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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
                filters = {"issue_created_at": {"$gt": gt, "$lt": lt},
                           "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "trend",
                           "across_limit": 20, "ou_ids": [val],
                           }
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="filter with ou id: {}".format(str(filter_type)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)
        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

    @pytest.mark.run(order=12)
    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("dashboard_partial", dashboard_partial)
    def test_issues_resolution_time_trend_report_es_dashboard_partial_filters(self, dashboard_partial,
                                                                              create_generic_object,
                                                                              create_customer_object,
                                                                              get_integration_obj):
        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt, lt = create_generic_object.get_epoc_time(value=4)
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
                filters = {"issue_created_at": {"$gt": gt, "$lt": lt},
                           "integration_ids": get_integration_obj}

                filters.update({req_filter_names_and_value_pair[0][0]: req_filter_names_and_value_pair[0][1]})

                payload = {"filter": filters, "across": "trend",
                           "across_limit": 20, "ou_ids": [val],
                           }
                LOG.info("payload {} ".format(json.dumps(payload)))
                es_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)
                db_base_url = create_generic_object.connection[
                                  "base_url"] + "jira_issues/resolution_time_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                flag, zero_flag = create_customer_object.comparing_es_vs_db(es_response, db_response,
                                                                            columns=['key', 'additional_key',
                                                                                     'median', "min", "max", "mean",
                                                                                     "p90", 'total_tickets'], unique_id="dashboard_partial with ou id: {}".format(str(dashboard_partial)+" "+ str(val)))
                if not flag:
                    list_not_match.append(val)
                if not zero_flag:
                    zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)
        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))