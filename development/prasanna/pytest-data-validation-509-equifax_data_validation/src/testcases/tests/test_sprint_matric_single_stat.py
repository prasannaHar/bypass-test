import logging
import pytest
import datetime
import calendar
import time
from src.utils.generate_Api_payload import GenericPayload
from dateutil.relativedelta import relativedelta

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

temp_project_id = "10182"
api_payload = GenericPayload()


class TestSprintSingleStat:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_sprint_metric_single_stat_report_widget(self, create_generic_object, widgetreusable_object,
                                                     get_integration_obj):
        gt_epoc, lt_epoc = create_generic_object.get_epoc_time(value=1)
        sprint_metric_widget_api_url = create_generic_object.connection[
                                           "base_url"] + "jira_issues/sprint_metrics_report"

        widget_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_req_dynamic_fiters=[["projects", create_generic_object.env["project_names"]]],
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc)
        )
        LOG.info("====  retrieving the widget response ====")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=widget_payload
        )
        data_check_flag = True
        try:
            assert len(widget_response['records']) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0: pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_sprint_metric_single_stat_report_compare_widget_drilldown(self, create_generic_object,
                                                                       widgetreusable_object, create_ou_object,
                                                                       get_integration_obj):
        gt_epoc, lt_epoc = create_generic_object.get_epoc_time(value=1)
        sprint_metric_widget_api_url = create_generic_object.connection[
                                           "base_url"] + "jira_issues/sprint_metrics_report"
        ou_ids = create_ou_object.get_ou_id()
        widget_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_req_dynamic_fiters=[["projects", create_generic_object.env["project_names"]]],
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc), arg_ou_id=ou_ids
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=widget_payload
        )
        data_check_flag = True
        try:
            assert len(widget_response['records']) > 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0: pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"
        widget_total = widget_response['count']
        drilldown_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_req_dynamic_fiters=[["projects", create_generic_object.env["project_names"]]],
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_include_total_count=True,
            arg_metric="velocity_points", arg_ou_id=ou_ids
        )

        LOG.info("====  retrieving the widget response ===")
        drilldown_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=drilldown_payload
        )
        drilldown_count = drilldown_response['_metadata']['total_count']
        assert widget_total == drilldown_count, "ticket count is not matching for Widget and Drill down"

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_sprint_metric_single_stat_report_compare_widget_drilldown_basicOU(self, create_generic_object,
                                                                               create_ou_object,
                                                                               widgetreusable_object,
                                                                               get_integration_obj):
        gt_epoc, lt_epoc = create_generic_object.get_epoc_time(value=1)
        sprint_metric_widget_api_url = create_generic_object.connection[
                                           "base_url"] + "jira_issues/sprint_metrics_report"

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "basic_OU_" + str(ts)
        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name,
            arg_req_integration_ids=get_integration_obj,
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        widget_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_req_dynamic_fiters = [["projects",create_generic_object.env["project_names"]]],
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_ou_id=[str(get_generate_OU_response['success'][0])]
        )
        LOG.info("====  retrieving the widget response ===")

        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=widget_payload
        )

        data_check_flag = True
        try:
            assert len(widget_response['records']) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0: pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"
        widget_total = widget_response['count']
        drilldown_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_req_dynamic_fiters=[["projects", create_generic_object.env["project_names"]]],
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_include_total_count=True,
            arg_metric="velocity_points",
            arg_ou_id=[str(get_generate_OU_response['success'][0])],
        )

        LOG.info("====  retrieving the widget response ===")
        drilldown_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=drilldown_payload
        )
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])
        drilldown_count = drilldown_response['_metadata']['total_count']
        assert widget_total == drilldown_count, "ticket count is not matching for Widget and Drill down"

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_sprint_metric_single_stat_report_widget_filters_buffer_idealRange_sprintCount(self, create_generic_object,
                                                                                           widgetreusable_object,
                                                                                           get_integration_obj):
        gt_epoc, lt_epoc = create_generic_object.get_epoc_time(value=1)
        sprint_metric_widget_api_url = create_generic_object.connection[
                                           "base_url"] + "jira_issues/sprint_metrics_report"

        buffer_days = 1
        required_filters_needs_tobe_applied = ["creep_buffer", "ideal_range", "sprint_count"]
        filter_value = [buffer_days * 86400, {"max": "90", "min": "70"}, 3]

        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])
        req_filter_names_and_value_pair.append(["projects", create_generic_object.env["project_names"]])

        widget_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=widget_payload
        )
        data_check_flag = True
        try:
            assert len(widget_response['records']) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0: pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=5)
    @pytest.mark.regression
    def test_sprint_metric_single_stat_report_compare_widget_drilldown_filters_buffer_idealRange_sprintCount(self,
                                                                                                             create_generic_object,
                                                                                                             widgetreusable_object,
                                                                                                             get_integration_obj):
        gt_epoc, lt_epoc = create_generic_object.get_epoc_time(value=1)
        sprint_metric_widget_api_url = create_generic_object.connection[
                                           "base_url"] + "jira_issues/sprint_metrics_report"

        buffer_days = 1
        required_filters_needs_tobe_applied = ["creep_buffer", "ideal_range", "sprint_count"]
        filter_value = [buffer_days * 86400, {"max": "90", "min": "70"}, 3]
        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])
        req_filter_names_and_value_pair.append(["projects", create_generic_object.env["project_names"]])

        widget_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )
        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=widget_payload
        )

        data_check_flag = True
        try:
            assert len(widget_response['records']) > 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0: pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"
        widget_total = widget_response['count']
        drilldown_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_include_total_count=True,
            arg_metric="avg_commit_to_done",
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )

        LOG.info("====  retrieving the widget response ===")
        drilldown_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=drilldown_payload
        )
        drilldown_count = drilldown_response['_metadata']['total_count']
        assert widget_total == drilldown_count, "ticket count is not matching for Widget and Drill down"

    @pytest.mark.run(order=6)
    @pytest.mark.regression
    def test_sprint_metric_single_stat_report_compare_widget_drilldown_filters_buffer_idealRange_sprintCount_basicOU(
            self, create_ou_object, widgetreusable_object, get_integration_obj, create_generic_object):
        gt_epoc, lt_epoc = create_generic_object.get_epoc_time(value=1)
        sprint_metric_widget_api_url = create_generic_object.connection[
                                           "base_url"] + "jira_issues/sprint_metrics_report"

        ts = calendar.timegm(time.gmtime())
        new_ou_name = "basic_OU_" + str(ts)
        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name,
            arg_req_integration_ids=get_integration_obj,
        )
        buffer_days = 1
        required_filters_needs_tobe_applied = ["creep_buffer", "ideal_range", "sprint_count"]
        filter_value = [buffer_days * 86400, {"max": "90", "min": "70"}, 3]

        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])
        req_filter_names_and_value_pair.append(["projects", create_generic_object.env["project_names"]])

        widget_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])],
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=widget_payload
        )

        data_check_flag = True
        try:
            assert len(widget_response['records']) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0: pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "unable to create the report , No Data Available"

        widget_total = widget_response['count']

        drilldown_payload = api_payload.generate_create_sprint_matric_single_stat_report_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_gt=str(gt_epoc),
            arg_lt=str(lt_epoc),
            arg_include_total_count=True,
            arg_metric="avg_commit_to_done",
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_id=[str(get_generate_OU_response['success'][0])],
        )

        LOG.info("====  retrieving the widget response ===")
        drilldown_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=sprint_metric_widget_api_url,
            arg_req_payload=drilldown_payload
        )
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])
        drilldown_count = drilldown_response['_metadata']['total_count']
        assert widget_total == drilldown_count, "ticket count is not matching for Widget and Drill down"
