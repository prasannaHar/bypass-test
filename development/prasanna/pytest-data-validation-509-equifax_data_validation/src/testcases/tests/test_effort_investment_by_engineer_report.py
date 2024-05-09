import logging
import random
import pytest
import datetime
import time
import calendar

from dateutil.relativedelta import relativedelta
from src.utils.generate_effort_investment_widget_drilldown_payload import *

temp_project_id = "10182"

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestEffortEngineer:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_create_effort_investment_by_engineer_widget(self, create_generic_object, widgetreusable_object,
                                                         get_integration_obj):
        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]
        months = 1
        categories = []
        products_id = temp_project_id
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT
        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT
        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            arg_req_payload={},
            request_type="get"
        )
        if ticket_categorization_scheme_response:
            ct = ticket_categorization_scheme_response["config"]["categories"]
            category_allocations = list(ct.keys())  # different index numbers
            for eachcategory in category_allocations:
                categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])
            for each_category in categories:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_ticket_categories=each_category

                )

                LOG.info("=== retrieving the  effort_investment_category_response ===")
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                data_check_flag = True
                try:
                    api_records = (effort_investment_category_response['records'])
                    assert len(api_records) >= 0, "unable to create the report , No Data Available"
                except:
                    data_check_flag = False
                assert data_check_flag is True, "unable to create the report , No Data Available"
        else:
            pytest.skip("ticket categorization scheme response is not Available ")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown(self, create_generic_object,
                                                                                widgetreusable_object,
                                                                                get_integration_obj):
        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]
        months = 1
        categories = []
        products_id = temp_project_id
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT
        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        if ticket_categorization_scheme_response:
            ct = ticket_categorization_scheme_response["config"]["categories"]
            category_allocations = list(ct.keys())  # different index numbers

            for eachcategory in category_allocations:
                categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])

            for each_category in categories:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_ticket_categories=each_category

                )
                LOG.info("=== retrieving the  effort_investment_category_response ===")
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                data_check_flag = True
                try:
                    api_records = (effort_investment_category_response['records'])
                    assert len(api_records) >= 0, "unable to create the report , No Data Available"
                except:
                    data_check_flag = False
                assert data_check_flag is True, "unable to create the report , No Data Available"
                requried_random_records = widgetreusable_object.retrieve_three_random_records(
                    effort_investment_category_response)
                for each_records in requried_random_records:

                    key = each_records['key']
                    effort = each_records['effort']

                    effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                        arg_req_integration_ids=get_integration_obj,
                        arg_gt=str(gt_epoc),
                        arg_lt=str(lt_epoc),
                        arg_project_id=products_id,
                        arg_across="assignee",
                        arg_status_categories="Done",
                        arg_assignee_display_names=key
                    )
                    effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=effort_investment_drilldown_api_url,
                        arg_req_payload=effort_investment_drilldown_payload
                    )

                    ticket_category_drilldown_count = 0
                    total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                    for category_count in range(total_ticket_category_drilldown_count):
                        try:
                            if effort_investment_drilldown_response['records'][category_count][
                                'ticket_category'] == each_category:
                                ticket_category_drilldown_count += 1
                        except:
                            continue

                    assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        else:
            pytest.skip("ticket categorization scheme response is not Available ")

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown_story_points(self,
                                                                                             create_generic_object,
                                                                                             widgetreusable_object,
                                                                                             get_integration_obj):
        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]
        months = 1
        categories = []
        products_id = temp_project_id
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT
        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT
        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        if ticket_categorization_scheme_response:
            ct = ticket_categorization_scheme_response["config"]["categories"]
            category_allocations = list(ct.keys())  # different index numbers
            for eachcategory in category_allocations:
                categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])
            required_filters_needs_tobe_applied = ["story_points"]
            filter_value = {
                "$gt": "0",
                "$lt": "10"
            }
            req_filter_names_and_value_pair = []
            for eachfilter in required_filters_needs_tobe_applied:
                req_filter_names_and_value_pair.append([eachfilter, filter_value])
            for each_category in categories:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_ticket_categories=each_category,
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,

                )
                LOG.info("=== retrieving the  effort_investment_category_response ===")
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )

                data_check_flag = True
                try:
                    api_records = (effort_investment_category_response['records'])
                    assert len(api_records) >= 0, "unable to create the report , No Data Available"
                except:
                    data_check_flag = False
                assert data_check_flag == True, "unable to create the report , No Data Available"
                requried_random_records = widgetreusable_object.retrieve_three_random_records(
                    effort_investment_category_response)
                for each_records in requried_random_records:
                    key = each_records['key']
                    effort = each_records['effort']
                    # drillworn api
                    effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                        arg_req_integration_ids=get_integration_obj,
                        arg_gt=str(gt_epoc),
                        arg_lt=str(lt_epoc),
                        arg_project_id=products_id,
                        arg_across="assignee",
                        arg_status_categories="Done",
                        arg_assignee_display_names=key,
                        arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    )
                    effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=effort_investment_drilldown_api_url,
                        arg_req_payload=effort_investment_drilldown_payload
                    )

                    ticket_category_drilldown_count = 0
                    total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                    for category_count in range(total_ticket_category_drilldown_count):
                        try:
                            if effort_investment_drilldown_response['records'][category_count][
                                'ticket_category'] == each_category:
                                ticket_category_drilldown_count += 1
                        except:
                            continue
                    assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        else:
            pytest.skip("ticket categorization scheme response is not Available ")

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown_projects(self, create_generic_object,
                                                                                         widgetreusable_object,
                                                                                         get_integration_obj):

        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]

        months = 1
        categories = []
        products_id = temp_project_id
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT
        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT
        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        if ticket_categorization_scheme_response:
            ct = ticket_categorization_scheme_response["config"]["categories"]
            category_allocations = list(ct.keys())  # different index numbers

            for eachcategory in category_allocations:
                categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])
            required_filters_needs_tobe_applied = ["jira_projects"]
            req_filter_names_and_value_pair = []

            get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                           arg_integration_ids=
                                                                               get_integration_obj)
            temp_records = [get_filter_response['records'][0]['project_name']]


            rdm = random.randint(0, len(temp_records[0]) - 1)
            filter_value = [temp_records[0][rdm]['key']]

            for eachfilter in required_filters_needs_tobe_applied:
                req_filter_names_and_value_pair.append([eachfilter, filter_value])

            for each_category in categories:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_ticket_categories=each_category,
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                )

                LOG.info("=== retrieving the  effort_investment_category_response ===")
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                data_check_flag = True
                try:
                    api_records = (effort_investment_category_response['records'])
                    assert len(api_records) >= 0, "unable to create the report , No Data Available"
                except:
                    data_check_flag = False

                assert data_check_flag == True, "unable to create the report , No Data Available"
                requried_random_records = widgetreusable_object.retrieve_three_random_records(
                    effort_investment_category_response)
                for each_records in requried_random_records:
                    key = each_records['key']
                    effort = each_records['effort']
                    # drillworn api
                    effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                        arg_req_integration_ids=get_integration_obj,
                        arg_gt=str(gt_epoc),
                        arg_lt=str(lt_epoc),
                        arg_project_id=products_id,
                        arg_across="assignee",
                        arg_status_categories="Done",
                        arg_assignee_display_names=key,
                        arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    )
                    effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(

                        arg_req_api=effort_investment_drilldown_api_url,
                        arg_req_payload=effort_investment_drilldown_payload
                    )
                    ticket_category_drilldown_count = 0
                    total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                    for category_count in range(total_ticket_category_drilldown_count):
                        try:
                            if effort_investment_drilldown_response['records'][category_count][
                                'ticket_category'] == each_category:
                                ticket_category_drilldown_count += 1
                        except:
                            continue
                    assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        else:
            pytest.skip("ticket categorization scheme response is not Available ")

    @pytest.mark.run(order=5)
    @pytest.mark.regression
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown_update_issue_resolved_in(self,
                                                                                                         create_generic_object,
                                                                                                         widgetreusable_object,
                                                                                                         get_integration_obj):

        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]

        months = 1
        categories = []
        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        if ticket_categorization_scheme_response:
            ct = ticket_categorization_scheme_response["config"]["categories"]
            category_allocations = list(ct.keys())  # different index numbers
            for eachcategory in category_allocations:
                categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])

            for each_category in categories:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_ticket_categories=each_category

                )
                LOG.info("=== retrieving the  effort_investment_category_response ===")
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                data_check_flag = True
                try:
                    api_records = (effort_investment_category_response['records'])
                    assert len(api_records) >= 0, "unable to create the report , No Data Available"
                except:
                    data_check_flag = False
                assert data_check_flag == True, "unable to create the report , No Data Available"
                requried_random_records = widgetreusable_object.retrieve_three_random_records(
                    effort_investment_category_response)
                for each_records in requried_random_records:
                    key = each_records['key']
                    effort = each_records['effort']
                    # drillworn api
                    effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                        arg_req_integration_ids=get_integration_obj,
                        arg_gt=str(gt_epoc),
                        arg_lt=str(lt_epoc),
                        arg_project_id=products_id,
                        arg_across="assignee",
                        arg_status_categories="Done",
                        arg_assignee_display_names=key
                    )
                    effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(

                        arg_req_api=effort_investment_drilldown_api_url,
                        arg_req_payload=effort_investment_drilldown_payload
                    )

                    ticket_category_drilldown_count = 0
                    total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                    for category_count in range(total_ticket_category_drilldown_count):
                        try:
                            if effort_investment_drilldown_response['records'][category_count][
                                'ticket_category'] == each_category:
                                ticket_category_drilldown_count += 1
                        except:
                            continue
                    assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        else:
            pytest.skip("ticket categorization scheme response is not Available ")

    @pytest.mark.run(order=5)
    @pytest.mark.regression
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown_absolute_time_range(self,
                                                                                                    create_generic_object,
                                                                                                    widgetreusable_object,
                                                                                                    get_integration_obj):

        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]
        # global effort
        months = 1
        categories = []
        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id

        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        if ticket_categorization_scheme_response:
            ct = ticket_categorization_scheme_response["config"]["categories"]
            category_allocations = list(ct.keys())  # different index numbers

            for eachcategory in category_allocations:
                categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])

            for each_category in categories:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_ticket_categories=each_category
                )
                LOG.info("=== retrieving the  effort_investment_category_response===")
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                data_check_flag = True
                try:
                    api_records = (effort_investment_category_response['records'])
                    assert len(api_records) >= 0, "unable to create the report , No Data Available"
                except:
                    data_check_flag = False
                assert data_check_flag == True, "unable to create the report , No Data Available"
                requried_random_records = widgetreusable_object.retrieve_three_random_records(
                    effort_investment_category_response)
                for each_records in requried_random_records:
                    key = each_records['key']
                    effort = each_records['effort']
                    # drillworn api
                    effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                        arg_req_integration_ids=get_integration_obj,
                        arg_gt=str(gt_epoc),
                        arg_lt=str(lt_epoc),
                        arg_project_id=products_id,
                        arg_across="assignee",
                        arg_status_categories="Done",
                        arg_assignee_display_names=key
                    )

                    effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=effort_investment_drilldown_api_url,
                        arg_req_payload=effort_investment_drilldown_payload
                    )

                    ticket_category_drilldown_count = 0
                    total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                    for category_count in range(total_ticket_category_drilldown_count):
                        try:
                            if effort_investment_drilldown_response['records'][category_count][
                                'ticket_category'] == each_category:
                                ticket_category_drilldown_count += 1
                        except:
                            continue
                    assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        else:
            pytest.skip("ticket categorization scheme response is not Available ")

    @pytest.mark.run(order=6)
    @pytest.mark.regression_need_db
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown_OU_People_based(self,
                                                                                                create_generic_object,
                                                                                                create_ou_object,
                                                                                                create_postgres_object,
                                                                                                widgetreusable_object,
                                                                                                get_integration_obj):
        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]
        # global effort
        months = 1
        manager_email = []
        manager_id = []
        categories = []
        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "people_OU_" + str(ts)
        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM " + create_generic_object.connection[
                "tenant_name"] + ".org_users WHERE email=" + "'" + \
                                             manager_email[
                                                 eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(
            arg_req_integration_ids=get_integration_obj,
            arg_req_ou_name=new_ou_name,
            arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        ct = ticket_categorization_scheme_response["config"]["categories"]
        category_allocations = list(ct.keys())  # different index numbers

        for eachcategory in category_allocations:
            categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])

        for each_category in categories:
            effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                arg_req_integration_ids=get_integration_obj,
                arg_gt=str(gt_epoc),
                arg_lt=str(lt_epoc),
                arg_project_id=products_id,
                arg_across="assignee",
                arg_ticket_categories=each_category,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]

            )
            LOG.info("=== retrieving the  effort_investment_category_response ===")
            effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(

                arg_req_api=effort_investment_category_api_url,
                arg_req_payload=effort_investment_category_payload,
            )
            data_check_flag = True
            try:
                api_records = (effort_investment_category_response['records'])
                assert len(api_records) >= 0, "unable to create the report , No Data Available"
            except:
                data_check_flag = False
            assert data_check_flag == True, "unable to create the report , No Data Available"
            requried_random_records = widgetreusable_object.retrieve_three_random_records(
                effort_investment_category_response)
            for each_records in requried_random_records:
                key = each_records['key']
                effort = each_records['effort']
                # drillworn api
                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_status_categories="Done",
                    arg_assignee_display_names=key,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])]

                )
                effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(

                    arg_req_api=effort_investment_drilldown_api_url,
                    arg_req_payload=effort_investment_drilldown_payload
                )
                ticket_category_drilldown_count = 0
                total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                for category_count in range(total_ticket_category_drilldown_count):
                    try:
                        if effort_investment_drilldown_response['records'][category_count][
                            'ticket_category'] == each_category:
                            ticket_category_drilldown_count += 1
                    except:
                        continue

                assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=7)
    @pytest.mark.regression
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown_OU_filter_based(self,
                                                                                                create_generic_object,
                                                                                                create_filterresuable_object,
                                                                                                create_ou_object,
                                                                                                widgetreusable_object,
                                                                                                get_integration_obj):
        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]

        months = 1
        manager_email = []
        manager_id = []
        categories = []
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "filter-ou" + str(ts)
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                       arg_integration_ids=
                                                                           get_integration_obj)
        required_project_filter_values = [get_filter_response['records'][0]['project_name']]
        LOG.info("required_project_filter_values : {}".format(required_project_filter_values))
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs,
            arg_required_integration_type="jira"
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        ct = ticket_categorization_scheme_response["config"]["categories"]
        category_allocations = list(ct.keys())  # different index numbers
        for eachcategory in category_allocations:
            categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])
        for each_category in categories:
            effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                arg_req_integration_ids=get_integration_obj,
                arg_gt=str(gt_epoc),
                arg_lt=str(lt_epoc),
                arg_project_id=products_id,
                arg_across="assignee",
                arg_ticket_categories=each_category,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]

            )
            LOG.info("=== retrieving the  effort_investment_category_response ===")
            effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=effort_investment_category_api_url,
                arg_req_payload=effort_investment_category_payload,
            )
            data_check_flag = True
            try:
                api_records = (effort_investment_category_response['records'])
                assert len(api_records) >= 0, "unable to create the report , No Data Available"
            except:
                data_check_flag = False
            assert data_check_flag == True, "unable to create the report , No Data Available"
            requried_random_records = widgetreusable_object.retrieve_three_random_records(
                effort_investment_category_response)
            for each_records in requried_random_records:
                key = each_records['key']
                effort = each_records['effort']
                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_status_categories="Done",
                    arg_assignee_display_names=key,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])]

                )

                effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_drilldown_api_url,
                    arg_req_payload=effort_investment_drilldown_payload

                )

                ticket_category_drilldown_count = 0
                total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                for category_count in range(total_ticket_category_drilldown_count):
                    try:
                        if effort_investment_drilldown_response['records'][category_count][
                            'ticket_category'] == each_category:
                            ticket_category_drilldown_count += 1

                    except:
                        continue

                assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=8)
    @pytest.mark.regression
    def test_create_effort_investment_by_engineer_compare_widget_with_drilldown_OU_user_based(self,
                                                                                              create_generic_object,
                                                                                              create_filterresuable_object,
                                                                                              create_ou_object,
                                                                                              widgetreusable_object,
                                                                                              get_integration_obj):
        effort_investment_category_api_url = create_generic_object.connection["base_url"] + \
                                             create_generic_object.api_data[
                                                 "effort_investment_category_api_url"]
        effort_investment_drilldown_api_url = create_generic_object.connection["base_url"] + \
                                              create_generic_object.api_data[
                                                  "drill_down_api_url"]
        org_prefered_user_based_attribute = create_generic_object.env["env_org_users_preferred_attribute"]
        ticket_categorization_scheme_url = create_generic_object.connection[
                                               "base_url"] + "ticket_categorization_schemes/" + \
                                           create_generic_object.env["effort_investment_profile_id"]
        months = 1
        manager_email = []
        manager_id = []
        categories = []

        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "user-ou" + str(ts)
        required_attrib_filter_values = create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_required_user_attrib=org_prefered_user_based_attribute,
            arg_retrieve_only_values=True
        )
        ou_creation_required_user_attrib_filter_key_value_pairs = {
            org_prefered_user_based_attribute: required_attrib_filter_values}
        get_generate_OU_response = create_ou_object.create_user_attribute_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=get_integration_obj,
            arg_required_integration_type="jira",
            arg_required_user_atrribute_filters_key_value_pair=ou_creation_required_user_attrib_filter_key_value_pairs
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT
        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT
        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        ct = ticket_categorization_scheme_response["config"]["categories"]
        category_allocations = list(ct.keys())  # different index numbers
        for eachcategory in category_allocations:
            categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])
        for each_category in categories:
            effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                arg_req_integration_ids=get_integration_obj,
                arg_gt=str(gt_epoc),
                arg_lt=str(lt_epoc),
                arg_project_id=products_id,
                arg_across="assignee",
                arg_ticket_categories=each_category,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            LOG.info("=== retrieving the  effort_investment_category_response ===")
            effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=effort_investment_category_api_url,
                arg_req_payload=effort_investment_category_payload,
            )
            data_check_flag = True
            try:
                api_records = (effort_investment_category_response['records'])
                assert len(api_records) >= 0, "unable to create the report , No Data Available"
            except:
                data_check_flag = False
            assert data_check_flag == True, "unable to create the report , No Data Available"
            requried_random_records = widgetreusable_object.retrieve_three_random_records(
                effort_investment_category_response)
            for each_records in requried_random_records:
                key = each_records['key']
                effort = each_records['effort']

                # drillworn api
                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_project_id=products_id,
                    arg_across="assignee",
                    arg_status_categories="Done",
                    arg_assignee_display_names=key,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])]
                )
                effort_investment_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_drilldown_api_url,
                    arg_req_payload=effort_investment_drilldown_payload
                )
                ticket_category_drilldown_count = 0
                total_ticket_category_drilldown_count = int(effort_investment_drilldown_response['count'])
                for category_count in range(total_ticket_category_drilldown_count):
                    try:
                        if effort_investment_drilldown_response['records'][category_count][
                            'ticket_category'] == each_category:
                            ticket_category_drilldown_count += 1
                    except:
                        continue

                assert effort == ticket_category_drilldown_count, "Effort value donot matchwith drilldown"
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])
