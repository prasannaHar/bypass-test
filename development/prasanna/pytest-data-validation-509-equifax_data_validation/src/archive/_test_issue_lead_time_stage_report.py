import datetime
import logging
import random
from copy import deepcopy
import calendar
import time
import pytest
from src.utils.generate_Api_payload import GenericPayload

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

rating_types = [("good"), ("slow"), ("needs_attention"), ("missing")]
temp_project_id = "10182"


class TestIssueLeadtime:
    def test_issue_lead_time_stage_report(self, create_generic_object, widgetreusable_object):

        pytest.application_url = create_generic_object.connection["base_url"]
        pytest.drilldown_api_url = pytest.application_url + create_generic_object.api_data["drill_down_api_url"]
        pytest.jira_integration_ids = create_generic_object.get_integration_id()
        pytest.lead_time_widget_api_url = pytest.application_url + create_generic_object.api_data["velocity"]
        pytest.lead_time_api_url = pytest.application_url + create_generic_object.api_data["velocity_values"]
        pytest.api_payload = GenericPayload()
        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id

        widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id

        )
        LOG.info("=== retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=widget_payload
        )
        # ;
        data_check_flag = True

        try:
            api_records = (widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=2)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown(self, widgetreusable_object):

        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id

        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        # ;
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"

        except:

            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,

            )
            LOG.info("lead_time_drilldown_payload")
            LOG.info(lead_time_drilldown_payload)

            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                median = median / total_record_count

                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=3)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_story_points(self, widgetreusable_object):

        required_filters_needs_tobe_applied = ["story_points"]
        filter_value = {
            "$gt": "-1",
            "$lt": "100"
        }
        req_filter_names_and_value_pair = []

        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )

        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"

        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair

            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count

                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=4)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_issue_resolved_at(self, widgetreusable_object):

        ts = calendar.timegm(time.gmtime())
        gt_ts = ts - 86400 * 20
        required_filters_needs_tobe_applied = ["jira_issue_resolved_at"]
        filter_value = {
            "$gt": str(gt_ts),
            "$lt": str(ts)
        }
        req_filter_names_and_value_pair = []

        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )
            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count

                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=5)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_filter_Issue_Type(self, create_generic_object,
                                                                                          widgetreusable_object):

        required_filters_needs_tobe_applied = ["jira_issue_types"]
        req_filter_names_and_value_pair = []

        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id
        # for eachfilter in required_filters_needs_tobe_applied:
        #     req_filter_names_and_value_pair.append([eachfilter, filter_value])
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                    arg_integration_ids=pytest.jira_integration_ids)
        # count=
        temp_records = [get_filter_response['records'][0]['issue_type']]
        # for eachrec

        rdm = random.randint(0, len(temp_records[0]) - 1)
        filter_value = [temp_records[0][rdm]['key']]

        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair

            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )
            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=6)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_filter_project_name(self, create_generic_object,
                                                                                            widgetreusable_object):

        required_filters_needs_tobe_applied = ["jira_projects"]
        req_filter_names_and_value_pair = []

        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id
        # for eachfilter in required_filters_needs_tobe_applied:
        #     req_filter_names_and_value_pair.append([eachfilter, filter_value])
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                    arg_integration_ids=pytest.jira_integration_ids)
        # count=
        temp_records = [get_filter_response['records'][0]['project_name']]
        # for eachrec

        rdm = random.randint(0, len(temp_records[0]) - 1)
        # LOG.info(temp_records[0][rdm]['key'])
        filter_value = [temp_records[0][rdm]['key']]

        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            # LOG.info("lead_time_drilldown_payload")
            # LOG.info(lead_time_drilldown_payload)
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            try:
                median = median / total_record_count
                assert widget_count == median
            except:
                continue  # no records for project

    @pytest.mark.run(order=7)
    @pytest.mark.parametrize("rating_type", rating_types)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_ratings(self, rating_type,
                                                                                widgetreusable_object):

        req_filter_values_copy = deepcopy(rating_type)
        req_filter_names_and_value_pair = []
        LOG.info("=== generating dynamic filter value pair ===")
        LOG.info("=== generating the filter values ===")
        req_filter_values = rating_type.split(",")
        reviewer_filter_value = ["ratings", req_filter_values]
        req_filter_names_and_value_pair = [reviewer_filter_value]

        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_rating=[rating_type]

        )
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_rating=[rating_type],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair

            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            fetch_record_rating = \
                                lead_time_drilldown_response['records'][record]['data'][data]['velocity_stage_result'][
                                    'rating']
                            assert fetch_record_rating in req_filter_values_copy, "records are not matching as per the selected rating"
                        except:
                            continue

    @pytest.mark.run(order=8)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_Lead_time_Profile(self, widgetreusable_object):
        products_id = temp_project_id

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id
        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id
            )
            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=9)
    def test_issue_lead_time_stage_report_compare_widget(self, widgetreusable_object):

        products_id = temp_project_id

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id
        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        # ;
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,

            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count

                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=10)
    def test_issue_lead_time_stage_report_compare_widget_change_lead_time_profile(self, widgetreusable_object):
        constants = text_file_retrieve_file_content_fun(arg_req_file="../test-temp-files/lead_time_constants.txt")
        lead_time_profile = constants.split(",")[0]
        products_id = temp_project_id
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_velocity_config_id=lead_time_profile
        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )

        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_velocity_config_id=lead_time_profile
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=11)
    def test_issue_lead_time_stage_report_compare_widget_OU_People_based(self, create_postgres_object, create_ou_object,
                                                                         widgetreusable_object):
        manager_email = []
        manager_id = []
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("lead time payload")
        LOG.info(lead_time_widget_payload)
        # ;
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count

                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=12)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_People_based_story_points(self,
                                                                                                     create_ou_object,
                                                                                                     create_postgres_object,
                                                                                                     widgetreusable_object):
        manager_email = []
        manager_id = []
        required_filters_needs_tobe_applied = ["story_points"]
        filter_value = {
            "$gt": "20",
            "$lt": "100"
        }
        req_filter_names_and_value_pair = []

        products_id = temp_project_id
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )

        # ;
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=13)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_People_based_filter_Issue_Type(self,
                                                                                                          create_generic_object,
                                                                                                          create_ou_object,
                                                                                                          create_postgres_object,
                                                                                                          widgetreusable_object):

        manager_email = []
        manager_id = []
        required_filters_needs_tobe_applied = ["jira_issue_types"]
        req_filter_names_and_value_pair = []

        products_id = temp_project_id
        # for eachfilter in required_filters_needs_tobe_applied:
        #     req_filter_names_and_value_pair.append([eachfilter, filter_value])
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                       arg_integration_ids=pytest.jira_integration_ids)
        # count=
        temp_records = [get_filter_response['records'][0]['issue_type']]
        # for eachrec

        rdm = random.randint(0, len(temp_records[0]) - 1)
        # LOG.info(temp_records[0][rdm]['key'])
        filter_value = [temp_records[0][rdm]['key']]

        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]

            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=13)
    def test_issue_lead_time_stage_with_drilldown_OU_People_based_filter_project_name(self, create_generic_object,
                                                                                      create_ou_object,
                                                                                      create_postgres_object,
                                                                                      widgetreusable_object):

        manager_email = []
        manager_id = []
        required_filters_needs_tobe_applied = ["jira_projects"]
        req_filter_names_and_value_pair = []
        products_id = temp_project_id
        # for eachfilter in required_filters_needs_tobe_applied:
        #     req_filter_names_and_value_pair.append([eachfilter, filter_value])
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                       arg_integration_ids=pytest.jira_integration_ids)

        temp_records = [get_filter_response['records'][0]['project_name']]
        rdm = random.randint(0, len(temp_records[0]) - 1)
        filter_value = [temp_records[0][rdm]['key']]
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)
        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]

            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            try:
                median = median / total_record_count
                assert widget_count == median
            except:
                continue  # no records for project
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=14)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_People_based_issue_resolved_at(self,
                                                                                                          create_ou_object,
                                                                                                          create_postgres_object,
                                                                                                          widgetreusable_object):

        manager_email = []
        manager_id = []

        ts = calendar.timegm(time.gmtime())
        gt_ts = ts - 86400 * 20
        required_filters_needs_tobe_applied = ["jira_issue_resolved_at"]
        filter_value = {
            "$gt": str(gt_ts),
            "$lt": str(ts)
        }
        req_filter_names_and_value_pair = []

        products_id = temp_project_id
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=15)
    @pytest.mark.parametrize("rating_type", rating_types)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_People_based_ratings(self, rating_type,
                                                                                                create_ou_object,
                                                                                                create_postgres_object,
                                                                                                widgetreusable_object):
        req_filter_values_copy = deepcopy(rating_type)
        req_filter_names_and_value_pair = []
        LOG.info("=== generating dynamic filter value pair ===")

        LOG.info("=== generating the filter values ===")
        req_filter_values = rating_type.split(",")
        reviewer_filter_value = ["ratings", req_filter_values]
        req_filter_names_and_value_pair = [reviewer_filter_value]

        manager_email = []
        manager_id = []
        products_id = temp_project_id

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_rating=[rating_type],
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("lead_time_widget_payload")
        LOG.info(lead_time_widget_payload)
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:

            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_rating=[rating_type],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            LOG.info("lead_time_drilldown_payload")
            LOG.info(lead_time_drilldown_payload)
            key = each_records["key"]

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )
            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            fetch_record_rating = \
                                lead_time_drilldown_response['records'][record]['data'][data]['velocity_stage_result'][
                                    'rating']
                            assert fetch_record_rating in req_filter_values_copy, "records are not matching as per the selected rating"
                        except:
                            continue
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=16)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_People_based_Lead_time_Profile(self,
                                                                                                          create_ou_object,
                                                                                                          create_postgres_object,
                                                                                                          widgetreusable_object):

        manager_email = []
        manager_id = []
        products_id = temp_project_id

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        # ;
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )
            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=17)
    def test_issue_lead_time_stage_report_compare_widget_drilldown_OU_People_based(self, create_ou_object,
                                                                                   create_postgres_object,
                                                                                   widgetreusable_object):
        manager_email = []
        manager_id = []
        products_id = temp_project_id

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        LOG.info("lead time payload")
        LOG.info(lead_time_widget_payload)
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        # ;
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=18)
    def test_issue_lead_time_stage_report_compare_widget_change_lead_time_profile_OU_People_based(self,
                                                                                                  create_ou_object,
                                                                                                  create_postgres_object,
                                                                                                  widgetreusable_object):

        constants = text_file_retrieve_file_content_fun(arg_req_file="../test-temp-files/lead_time_constants.txt")
        lead_time_profile = constants.split(",")[0]
        manager_email = []
        manager_id = []

        products_id = temp_project_id

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "people_OU_" + str(ts)

        get_OU_manager_from_email_response = create_ou_object.get_OU_mangers()
        total_mangers = int(get_OU_manager_from_email_response['records'][0]['email']['total_count'])
        for eachmanager in range(total_mangers):
            manager_email.append(
                get_OU_manager_from_email_response['records'][0]['email']['records'][eachmanager]['key'])
            sql_query_needs_to_be_executed = "SELECT ref_id FROM foo.org_users WHERE email=" + "'" + manager_email[
                eachmanager] + "'"
            LOG.info(sql_query_needs_to_be_executed)
            try:
                required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            except:
                assert False, "unable to get data from table"
            LOG.info(required_records_from_database[0][0])
            manager_id.append(str(required_records_from_database[0][0]))

        get_generate_OU_response = create_ou_object.create_people_OU(arg_req_integration_ids=pytest.jira_integration_ids,
                                                                     arg_req_ou_name=new_ou_name,
                                                                     arg_users=manager_id)
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])],
            arg_velocity_config_id=lead_time_profile
        )
        LOG.info("lead time payload")
        LOG.info(lead_time_widget_payload)
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload
        )
        # ;
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])],
                arg_velocity_config_id=lead_time_profile
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=19)
    def test_issue_lead_time_stage_report_compare_widget_OU_filter_based(self, create_filterresuable_object,
                                                                         create_ou_object, widgetreusable_object):

        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)
        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(
            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}

        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )

        LOG.info("new ou name")
        LOG.info(new_ou_name)

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        LOG.info("lead time payload")
        LOG.info(lead_time_widget_payload)
        # ;
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=20)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_filter_based_story_points(self,
                                                                                                     create_filterresuable_object,
                                                                                                     create_ou_object,
                                                                                                     widgetreusable_object):
        required_filters_needs_tobe_applied = ["story_points"]
        filter_value = {
            "$gt": "20",
            "$lt": "100"
        }
        req_filter_names_and_value_pair = []

        # commenting as workaround as  Delete Project Api Not Working
        # get_create_project = create_project_and_dashboard_for_issue_time()
        # products_id = get_create_project["id"]
        products_id = temp_project_id
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)

        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(
            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}

        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )

        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                median = median / total_record_count

                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=21)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_filter_based_filter_Issue_Type(self,
                                                                                                          create_generic_object,
                                                                                                          create_filterresuable_object,
                                                                                                          create_ou_object,
                                                                                                          widgetreusable_object):
        required_filters_needs_tobe_applied = ["jira_issue_types"]
        req_filter_names_and_value_pair = []

        products_id = temp_project_id

        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                       arg_integration_ids=pytest.jira_integration_ids)
        temp_records = [get_filter_response['records'][0]['issue_type']]

        rdm = random.randint(0, len(temp_records[0]) - 1)
        # LOG.info(temp_records[0][rdm]['key'])
        filter_value = [temp_records[0][rdm]['key']]

        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)

        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(

            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )

        # LOG.info("required_project_filter_values", required_project_filter_values)

        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}

        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )

        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:

            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            # LOG.info("lead_time_drilldown_payload")
            # LOG.info(lead_time_drilldown_payload)
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=22)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_filter_based_filter_project_name(self,
                                                                                                            create_generic_object,
                                                                                                            create_filterresuable_object,
                                                                                                            create_ou_object,
                                                                                                            widgetreusable_object):
        required_filters_needs_tobe_applied = ["jira_projects"]
        req_filter_names_and_value_pair = []
        products_id = temp_project_id
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                       arg_integration_ids=pytest.jira_integration_ids)
        temp_records = [get_filter_response['records'][0]['project_name']]
        rdm = random.randint(0, len(temp_records[0]) - 1)
        filter_value = [temp_records[0][rdm]['key']]
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)
        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(
            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )

        # LOG.info("required_project_filter_values", required_project_filter_values)
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )

        # ;
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        # ;
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            try:
                median = median / total_record_count
                assert widget_count == median
            except:
                continue  # no records for project
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=23)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_filter_based_issue_resolved_at(self,
                                                                                                          create_filterresuable_object,
                                                                                                          create_ou_object,
                                                                                                          widgetreusable_object):

        ts = calendar.timegm(time.gmtime())
        gt_ts = ts - 86400 * 20
        required_filters_needs_tobe_applied = ["jira_issue_resolved_at"]
        filter_value = {
            "$gt": str(gt_ts),
            "$lt": str(ts)
        }
        req_filter_names_and_value_pair = []

        products_id = temp_project_id
        for eachfilter in required_filters_needs_tobe_applied:
            req_filter_names_and_value_pair.append([eachfilter, filter_value])

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)
        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(

            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )

        # LOG.info("required_project_filter_values", required_project_filter_values)

        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}

        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        # ;
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=24)
    @pytest.mark.parametrize("rating_type", rating_types)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_filter_based_ratings(self, rating_type,
                                                                                                create_filterresuable_object,
                                                                                                create_ou_object,
                                                                                                widgetreusable_object):
        req_filter_values_copy = deepcopy(rating_type)
        req_filter_values = rating_type.split(",")
        reviewer_filter_value = ["ratings", req_filter_values]
        req_filter_names_and_value_pair = [reviewer_filter_value]
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)

        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(
            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )

        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )

        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_rating=[rating_type],
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("lead_time_widget_payload")
        LOG.info(lead_time_widget_payload)
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_rating=[rating_type],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            LOG.info("lead_time_drilldown_payload")
            LOG.info(lead_time_drilldown_payload)
            key = each_records["key"]
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            fetch_record_rating = \
                                lead_time_drilldown_response['records'][record]['data'][data]['velocity_stage_result'][
                                    'rating']
                            assert fetch_record_rating in req_filter_values_copy, "records are not matching as per the selected rating"
                        except:
                            continue
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=25)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_OU_filter_based_Lead_time_Profile(self,
                                                                                                          create_filterresuable_object,
                                                                                                          create_ou_object,
                                                                                                          widgetreusable_object):

        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)
        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(
            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )

        # LOG.info("required_project_filter_values", required_project_filter_values)
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )

        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=26)
    def test_issue_lead_time_stage_report_compare_widget_drilldown_OU_filter_based(self, create_filterresuable_object,
                                                                                   create_ou_object,
                                                                                   widgetreusable_object):

        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)
        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(
            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )

        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("lead time payload")
        LOG.info(lead_time_widget_payload)

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        # ;
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count

                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=27)
    def test_issue_lead_time_stage_report_compare_widget_change_lead_time_profile_OU_filter_based(self,
                                                                                                  create_filterresuable_object,
                                                                                                  create_ou_object,
                                                                                                  widgetreusable_object):
        constants = text_file_retrieve_file_content_fun(arg_req_file="../test-temp-files/lead_time_constants.txt")
        lead_time_profile = constants.split(",")[0]

        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "filter_OU_" + str(ts)
        required_project_filter_values = create_filterresuable_object.jira_lead_time_generate_project_filter_values(
            arg_app_url=pytest.application_url,
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_required_filter="project_name",
            arg_retrieve_only_values=True
        )
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        get_generate_OU_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name=new_ou_name,
            arg_required_integration_id=pytest.jira_integration_ids,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )

        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])],
            arg_velocity_config_id=lead_time_profile
        )
        LOG.info("lead time payload")
        LOG.info(lead_time_widget_payload)
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload
        )
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])],
                arg_velocity_config_id=lead_time_profile

            )
            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=28)
    def test_issue_lead_time_stage_report_compare_widget_OU_basic_integration(self, create_ou_object,
                                                                              widgetreusable_object):
        products_id = temp_project_id

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "basic_OU_" + str(ts)

        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name,
            arg_req_integration_ids=pytest.jira_integration_ids,
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            # arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        LOG.info("lead time payload")
        LOG.info(lead_time_widget_payload)
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                # arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])]
            )
            key = each_records["key"]
            widget_count = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(

                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload

            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
        create_ou_object.delete_OU(arg_OU_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=29)
    def test_issue_lead_time_stage_report_filter_issuetype_project_resolvedin(self, create_generic_object,
                                                                              widgetreusable_object):

        products_id = temp_project_id
        get_filter_response_issueType = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                                 arg_integration_ids=pytest.jira_integration_ids)

        temp_records_issueType = [get_filter_response_issueType['records'][0]['issue_type']]
        issueType_value = []
        for eachissueType in temp_records_issueType[0]:
            issueType_value.append(eachissueType['key'])
        if "STORY" in issueType_value:
            issueType = ["STORY"]
        else:
            issueType = [issueType_value[0]]
        get_filter_response_project = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                               arg_integration_ids=pytest.jira_integration_ids)
        temp_records_project = [get_filter_response_project['records'][0]['project_name']]
        project_value = []
        for eachproject in temp_records_project[0]:
            project_value.append(eachproject['key'])
        today = datetime.date.today()
        first = today.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        lt = datetime.datetime.combine(lastMonth, datetime.time.min)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        gt = datetime.datetime.combine(lastMonth.replace(day=1), datetime.time.max)
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        required_filters_needs_tobe_applied = ["jira_issue_types", "jira_projects", "jira_issue_resolved_at"]
        filter_value = [issueType, project_value, {"$gt": str(gt_epoc), "$lt": str(lt_epoc)}]
        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])
        widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )
        LOG.info("=== retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=widget_payload
        )
        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag == True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=30)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_filter_issuetype_project_resolvedin(self,
                                                                                                            create_generic_object,
                                                                                                            widgetreusable_object):

        products_id = temp_project_id
        get_filter_response_issueType = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                                 arg_integration_ids=pytest.jira_integration_ids)
        temp_records_issueType = [get_filter_response_issueType['records'][0]['issue_type']]
        issueType_value = []
        for eachissueType in temp_records_issueType[0]:
            issueType_value.append(eachissueType['key'])
        if "STORY" in issueType_value:
            issueType = ["STORY"]
        else:
            issueType = [issueType_value[0]]
        get_filter_response_project = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                               arg_integration_ids=pytest.jira_integration_ids)
        temp_records_project = [get_filter_response_project['records'][0]['project_name']]
        project_value = []
        for eachproject in temp_records_project[0]:
            project_value.append(eachproject['key'])

        today = datetime.date.today()
        first = today.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        lt = datetime.datetime.combine(lastMonth, datetime.time.min)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT
        gt = datetime.datetime.combine(lastMonth.replace(day=1), datetime.time.max)
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT
        required_filters_needs_tobe_applied = ["jira_issue_types", "jira_projects", "jira_issue_resolved_at"]
        filter_value = [issueType, project_value, {"$gt": str(gt_epoc), "$lt": str(lt_epoc)}]

        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )

        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        # ;
        data_check_flag = True
        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)

        for each_records in requried_random_records:
            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )

            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")

    @pytest.mark.run(order=31)
    def test_issue_lead_time_stage_report_compare_widget_with_drilldown_filter_issuetype_project_resolvedin_basicOU(
            self, create_ou_object, create_generic_object,
            widgetreusable_object):

        products_id = temp_project_id
        import calendar
        import time
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "basic_OU_" + str(ts)

        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name,
            arg_req_integration_ids=pytest.jira_integration_ids,
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        get_filter_response_issueType = create_generic_object.get_filter_options(arg_filter_type=["issue_type"],
                                                                                 arg_integration_ids=pytest.jira_integration_ids)

        temp_records_issueType = [get_filter_response_issueType['records'][0]['issue_type']]
        issueType_value = []
        for eachissueType in temp_records_issueType[0]:
            issueType_value.append(eachissueType['key'])
        if "STORY" in issueType_value:
            issueType = ["STORY"]
        else:
            issueType = [issueType_value[0]]

        get_filter_response_project = create_generic_object.get_filter_options(arg_filter_type=["project_name"],
                                                                               arg_integration_ids=pytest.jira_integration_ids)
        temp_records_project = [get_filter_response_project['records'][0]['project_name']]
        project_value = []
        for eachproject in temp_records_project[0]:
            project_value.append(eachproject['key'])

        today = datetime.date.today()
        first = today.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)

        lt = datetime.datetime.combine(lastMonth, datetime.time.min)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT
        #
        gt = datetime.datetime.combine(lastMonth.replace(day=1), datetime.time.max)
        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT

        required_filters_needs_tobe_applied = ["jira_issue_types", "jira_projects", "jira_issue_resolved_at"]
        filter_value = [issueType, project_value, {"$gt": str(gt_epoc), "$lt": str(lt_epoc)}]

        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])

        lead_time_widget_payload = pytest.api_payload.generate_create_leadtime_stagereport_payload(
            arg_req_integration_ids=pytest.jira_integration_ids,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )
        LOG.info("=== retrieving the widget response ===")
        lead_time_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=pytest.lead_time_widget_api_url,
            arg_req_payload=lead_time_widget_payload,
        )
        data_check_flag = True

        try:
            api_records = (lead_time_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False

        assert data_check_flag == True, "unable to create the report , No Data Available"
        requried_random_records = widgetreusable_object.retrieve_three_random_records(lead_time_widget_response)
        for each_records in requried_random_records:

            median = 0
            lead_time_drilldown_payload = pytest.api_payload.generate_leadtime_stagereport_drilldown_payload(
                arg_req_integration_ids=pytest.jira_integration_ids,
                arg_key=each_records["key"],
                arg_product_id=products_id,
                arg_ou_id=[str(get_generate_OU_response['success'][0])],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )

            key = each_records["key"]
            widget_count = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=pytest.lead_time_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            median = median + int(lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                median = median / total_record_count
                assert widget_count == median
            else:
                LOG.warning("No Records Found in API Response")
