import calendar
import datetime
import logging
import random
import pytest
import time
from dateutil.relativedelta import relativedelta
from src.utils.generate_effort_investment_widget_drilldown_payload import *
from src.lib.generic_helper.generic_helper import TestGenericHelper as tghelper

generic_object = tghelper()
effort_investment_single_stat_api_url = generic_object.connection['base_url'] + "ba/jira/ticket_count_fte"
ticket_categorization_scheme_url = generic_object.connection[
                                       'base_url'] + "ticket_categorization_schemes/" + generic_object.env["effort_investment_profile_id"]
org_prefered_user_based_attribute = generic_object.env['org_unit_name']

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestEffortInvestment:
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_create_effort_investment_single_stat_report_widget(self, create_generic_object, widgetreusable_object, get_integration_obj):
        global effort
        months = 1
        products_id = create_generic_object.env["project_id"]
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id
        )
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )

        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.regression
    @pytest.mark.sanity
    def test_create_effort_investment_single_stat_report_widget_check_category(self, create_generic_object, widgetreusable_object, get_integration_obj):
        global effort
        months = 1
        categories = ["Other"]
        widget_ct_allocation = []
        products_id = create_generic_object.env["project_id"]
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

        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id

        )
        ## retrieving the widget response
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"
        for eachcategory in range(int(effort_investment_trend_widget_response["count"])):
            widget_ct_allocation.append(effort_investment_trend_widget_response["records"][eachcategory]["key"])
        assert set(widget_ct_allocation).issubset(set(categories)), "Categories don't match in Widget and Category API"

    @pytest.mark.regression
    def test_create_effort_investment_single_stat_report_widget_story_points(self, create_generic_object, widgetreusable_object, get_integration_obj):
        global effort
        months = 1
        products_id = create_generic_object.env["project_id"]
        required_filters_needs_tobe_applied = ["story_points"]
        filter_value = {
            "$gt": "-1",
            "$lt": "500"
        }
        req_filter_names_and_value_pair = []
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
        )
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.regression
    def test_create_effort_investment_single_stat_report_widget_projects(self, create_generic_object, widgetreusable_object, get_integration_obj):
        months = 1
        products_id = create_generic_object.env["project_id"]
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
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
        )
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )
        # ;
        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.regression
    def test_create_effort_investment_single_stat_report_widget_update_issue_resolve(self, create_generic_object, widgetreusable_object, get_integration_obj):
        months = 1
        products_id = create_generic_object.env["project_id"]
        lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)  # getting 1st day of month
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT
        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id
        )
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.regression_need_db
    def test_create_effort_investment_single_stat_report_widget_check_category_OU_People_based(self, create_generic_object,
                                                            widgetreusable_object,create_ou_object, get_integration_obj, create_postgres_object):
        global effort
        months = 1
        categories = []
        manager_email = []
        manager_id = []
        widget_ct_allocation = []
        products_id = create_generic_object.env["project_id"]
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "people_ou_" + str(ts)
        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM " + create_generic_object.connection['tenant_name'] + ".org_users WHERE email=" + "'" + \
                                             manager_email[
                                                 eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.postgres_database_execute_select_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=get_integration_obj,
                                                    arg_req_ou_name=new_ou_name,
                                                    arg_users=manager_id)
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
        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])
        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"

        for eachcategory in range(int(effort_investment_trend_widget_response["count"])):
            widget_ct_allocation.append(effort_investment_trend_widget_response["records"][eachcategory]["key"])
        assert set(categories).issubset(set(widget_ct_allocation)), "Categories don't match in Widget and Category API"

    def test_create_effort_investment_single_stat_report_widget_check_category_OU_filter_based(self, create_generic_object,
                                                                                widgetreusable_object, create_filterresuable_object,
                                                                                               create_ou_object, get_integration_obj):
        global effort
        months = 1
        categories = []
        widget_ct_allocation = []
        products_id = create_generic_object.env["project_id"]
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

        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])

        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"
        for eachcategory in range(int(effort_investment_trend_widget_response["count"])):
            widget_ct_allocation.append(effort_investment_trend_widget_response["records"][eachcategory]["key"])
        assert set(categories).issubset(set(widget_ct_allocation)), "Categories don't match in Widget and Category API"

    def test_create_effort_investment_single_stat_report_widget_check_category_OU_user_based(self, create_generic_object,
                                                                        widgetreusable_object, create_filterresuable_object,
                                                                                             create_ou_object, get_integration_obj):
        months = 1
        categories = []
        widget_ct_allocation = []
        products_id = create_generic_object.env["project_id"]
        ts = calendar.timegm(time.gmtime())
        org_prefered_user_based_attribute = create_generic_object.env["env_org_users_preferred_attribute"]
        new_ou_name = "user-ou" + str(ts)
        required_attrib_filter_values =create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
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
        lt_epoc = int(lt.timestamp() + 19800)

        gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=months - 1), datetime.time.min)
        gt = gt.replace(day=1)
        gt_epoc = int(gt.timestamp() + 19800)
        ticket_categorization_scheme_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=ticket_categorization_scheme_url,
            request_type="get",
            arg_req_payload={},
        )
        ct = ticket_categorization_scheme_response["config"]["categories"]
        category_allocations = list(ct.keys())

        for eachcategory in category_allocations:
            categories.append(ticket_categorization_scheme_response['config']["categories"][eachcategory]['name'])

        effort_investment_trend_widget_payload = generate_create_effort_investment_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        effort_investment_trend_widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=effort_investment_single_stat_api_url,
            arg_req_payload=effort_investment_trend_widget_payload,
        )
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])
        data_check_flag = True
        try:
            api_records = (effort_investment_trend_widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len((effort_investment_trend_widget_response['records'])) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"
        for eachcategory in range(int(effort_investment_trend_widget_response["count"])):
            widget_ct_allocation.append(effort_investment_trend_widget_response["records"][eachcategory]["key"])
        assert set(categories).issubset(set(widget_ct_allocation)), "Categories don't match in Widget and Category API"
