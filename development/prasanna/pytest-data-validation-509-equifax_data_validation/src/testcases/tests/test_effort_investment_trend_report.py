import calendar
import logging
import random
import time
import pytest
import datetime

from dateutil.relativedelta import relativedelta
from src.utils.generate_Api_payload import GenericPayload
from src.lib.generic_helper.generic_helper import TestGenericHelper as tghelper
from src.utils.generate_effort_investment_widget_drilldown_payload import *

api_payload = GenericPayload()
generic_object = tghelper()
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
application_url = generic_object.connection['base_url']

effort_investment_widget_api_url = application_url + "ba/jira/active_work/ticket_count"
effort_investment_drilldown_api_url = application_url + "jira_issues/list"
effort_investment_category_api_url = application_url + "ba/jira/ticket_count_fte"
org_prefered_user_based_attribute = os.getenv('org_prefered_user_based_attribute')
temp_project_id = "10182"


class TestEffortInvestmentTrend:
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_create_effort_investment_trend_report_widget(self, create_generic_object, widgetreusable_object,
                                                          get_integration_obj):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT
        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT
        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id
        )
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.regression
    @pytest.mark.sanity
    def test_effort_investment_trend_report_widget_with_drilldown1(self, create_generic_object, widgetreusable_object,
                                                                   get_integration_obj):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id

        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id,
        )
        ## retrieving the widget response
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"
        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        total_category = len(category_allocations)
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)
        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)

            # each category api
            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_ticket_categories=each_category,
                )
                LOG.info(
                    "effort_investment_category_payload {} ".format(json.dumps(effort_investment_category_payload)))

                ## retrieving the  effort_investment_category_response
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                data_check_flag = True
                try:
                    api_records = (effort_investment_category_response['records'])
                    assert len(api_records) != 0, "unable to create the report , No Data Available"
                except:
                    data_check_flag = False
                    if len((effort_investment_category_response['records'])) == 0:
                        pytest.skip("unable to create the report , No Data Available")

                assert data_check_flag is True, "unable to create the report , No Data Available"
                LOG.info(
                    "effort_investment_category_response {} ".format(json.dumps(effort_investment_category_response)))

                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break
                # drilldown api

                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id,
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

    @pytest.mark.regression
    def test_effort_investment_trend_report_widget_with_drilldown_story_points(self, create_generic_object,
                                                                               widgetreusable_object,
                                                                               get_integration_obj):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id

        required_filters_needs_tobe_applied = ["story_points"]
        filter_value = {
            "$gt": "-1",
            "$lt": "500"
        }
        req_filter_names_and_value_pair = []
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
        )

        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"

        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        total_category = len(category_allocations)
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)

        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)

            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_ticket_categories=each_category
                )
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break
                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id,
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

    @pytest.mark.regression
    def test_effort_investment_trend_report_widget_with_drilldown_projects(self, create_generic_object,
                                                                           widgetreusable_object, get_integration_obj):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id

        required_filters_needs_tobe_applied = ["jira_projects"]
        req_filter_names_and_value_pair = []
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                       arg_integration_ids=get_integration_obj)
        temp_records = [get_filter_response['records'][0]['project_name']]

        rdm = random.randint(0, len(temp_records[0]) - 1)
        filter_value = [temp_records[0][rdm]['key']]

        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
        )
        ## retrieving the widget response
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"
        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        total_category = len(category_allocations)
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)
        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)

            # each category api
            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    arg_ticket_categories=each_category
                )
                ## retrieving the  effort_investment_category_response
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break
                # drilldown api

                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id,
                    arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                    # arg_req_dynamic_fiters=req_filter_names_and_value_pair
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

    @pytest.mark.regression
    def test_effort_investment_trend_report_widget_with_drilldown_update_issues_resolved_in(self, create_generic_object,
                                                                                            widgetreusable_object,
                                                                                            get_integration_obj):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT
        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id
        )
        ## retrieving the widget response
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"

        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        total_category = len(category_allocations)
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)

        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)
            # each category api
            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_ticket_categories=each_category
                )

                ## retrieving the  effort_investment_category_response
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )

                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break

                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id
                    # arg_req_dynamic_fiters=req_filter_names_and_value_pair
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

    @pytest.mark.regression
    def test_effort_investment_trend_report_widget_with_drilldown_absolute_time_range(self, create_generic_object,
                                                                                      widgetreusable_object,
                                                                                      get_integration_obj):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id

        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt = lt - datetime.timedelta(days=1)
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id
        )

        ## retrieving the widget response
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True

        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"
        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        total_category = len(category_allocations)
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)

        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)
            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_ticket_categories=each_category
                )

                ## retrieving the  effort_investment_category_response
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )

                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break

                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id
                    # arg_req_dynamic_fiters=req_filter_names_and_value_pair
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

    @pytest.mark.regression_need_db
    def test_effort_investment_trend_report_widget_with_drilldown_OU_People_based(self, create_generic_object,
                                                                                  create_postgres_object,
                                                                                  widgetreusable_object,
                                                                                  create_ou_object,
                                                                                  get_integration_obj):
        global effort, gt_date, lt_month_end
        manager_email = []
        manager_id = []
        months = 1
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "people_OU_" + str(ts)
        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM " + create_generic_object.connection[
                'tenant_name'] + ".org_users WHERE email=" + "'" + \
                                             manager_email[
                                                 eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.postgres_database_execute_select_query(
                    sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=get_integration_obj,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)

        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )

        ## retrieving the widget response
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"

        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)

        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)

            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_ticket_categories=each_category,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])],
                    arg_req_integration_ids=get_integration_obj
                )

                ## retrieving the  effort_investment_category_response
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break

                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])]
                )
                LOG.info(effort_investment_drilldown_payload)

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

    @pytest.mark.regression
    def ptest_effort_investment_trend_report_widget_with_drilldown_OU_filter_based(self, create_generic_object,
                                                                                   widgetreusable_object,
                                                                                   create_ou_object,
                                                                                   get_integration_obj,
                                                                                   create_filterresuable_object):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter-ou" + str(ts)
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                       arg_integration_ids=
                                                                       get_integration_obj)
        required_project_filter_values = [get_filter_response['records'][0]['project_name']]
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
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )

        ## retrieving the widget response
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"
        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)

        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)

            # each category api
            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_ticket_categories=each_category,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])]
                )

                ## retrieving the  effort_investment_category_response
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break
                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id,
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

    @pytest.mark.regression
    def test_effort_investment_trend_report_widget_with_drilldown_OU_user_based(self, create_generic_object,
                                                                                widgetreusable_object,
                                                                                create_filterresuable_object,
                                                                                get_integration_obj, create_ou_object):
        global effort, gt_date, lt_month_end
        months = 1
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "user-ou" + str(ts)
        org_prefered_user_based_attribute = create_generic_object.env["env_org_users_preferred_attribute"]
        required_attrib_filter_values = create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
            arg_app_url=create_generic_object.connection['base_url'],
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
        lt_epoc = int(lt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.replace(tzinfo=datetime.timezone.utc).timestamp())  # converting to GMT

        effort_investment_widget_payload = generate_create_effort_investment_trend_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_interval="month",
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )

        ## retrieving the widget response
        effort_investment_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_widget_api_url,
            arg_req_payload=effort_investment_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False
            if len((effort_investment_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"

        ct = effort_investment_widget_response['records'][0]['category_allocations']
        category_allocations = list(ct.keys())
        random_month_start = widgetreusable_object.get_three_random_months(arg_gt=gt, arg_lt=lt, arg_months=months)

        for each_records in random_month_start:
            month_end = each_records.replace(day=calendar.monthrange(each_records.year, each_records.month)[1])
            month_end = month_end.combine(month_end, datetime.time.max)
            # each category api
            for each_category in category_allocations:
                effort_investment_category_payload = generate_create_effort_investment_trend_payload(
                    arg_gt=str(gt_epoc),
                    arg_lt=str(lt_epoc),
                    arg_interval="month",
                    arg_project_id=products_id,
                    arg_across="issue_resolved_at",
                    arg_ticket_categories=each_category,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])],
                    arg_req_integration_ids=get_integration_obj
                )

                ## retrieving the  effort_investment_category_response
                effort_investment_category_response = widgetreusable_object.retrieve_required_api_response(
                    arg_req_api=effort_investment_category_api_url,
                    arg_req_payload=effort_investment_category_payload,
                )
                LOG.info("effort_investment_category_response")
                LOG.info(effort_investment_category_response)

                category_month_count = int(effort_investment_category_response['count'])
                effort = 0
                for month_count in range(category_month_count):
                    gt_date = str(int(each_records.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    lt_month_end = str(int(month_end.replace(tzinfo=datetime.timezone.utc).timestamp()))
                    if gt_date == str(
                            effort_investment_category_response['records'][month_count]['key']):
                        effort = effort_investment_category_response['records'][month_count]['effort']
                        break

                effort_investment_drilldown_payload = generate_effort_investment_drilldown_payload(
                    arg_req_integration_ids=get_integration_obj,
                    arg_gt=gt_date,
                    arg_lt=lt_month_end,
                    arg_project_id=products_id,
                    arg_ou_id=[str(get_generate_OU_response['success'][0])]
                )
                LOG.info(effort_investment_drilldown_payload)

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
