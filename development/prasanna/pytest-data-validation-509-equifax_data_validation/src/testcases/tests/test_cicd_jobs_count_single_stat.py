import logging
import pytest
import calendar
import time

from src.utils.generate_test_cicd_payload import *

temp_project_id = "10182"

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestcicdSingleState:
    @pytest.mark.run(order=1)
    @pytest.mark.need_maintenance
    def test_create_cicd_job_count_single_stat_report_widget(self, create_generic_object, widgetreusable_object, get_integration_obj):
        cicd_job_count_single_stat_api_url = create_generic_object.connection["base_url"] + "cicd_scm/job_counts"

        products_id = temp_project_id

        cicd_job_count_single_stat_widget_payload = generate_create_cicd_job_count_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_project_id=products_id
        )
        LOG.info("==== retrieving the widget response ====")
        cicd_job_count_single_stat_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=cicd_job_count_single_stat_api_url,
            arg_req_payload=cicd_job_count_single_stat_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (cicd_job_count_single_stat_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=2)
    @pytest.mark.need_maintenance
    def test_create_cicd_job_count_single_stat_report_widget_update_duration(self, create_generic_object, widgetreusable_object, get_integration_obj):
        cicd_job_count_single_stat_api_url = create_generic_object.connection["base_url"] + "cicd_scm/job_counts"

        products_id = temp_project_id
        cicd_job_count_single_stat_widget_payload = generate_create_cicd_job_count_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_project_id=products_id,
            arg_job_start_date=30
        )
        LOG.info("==== retrieving the widget response ====")
        cicd_job_count_single_stat_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=cicd_job_count_single_stat_api_url,
            arg_req_payload=cicd_job_count_single_stat_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (cicd_job_count_single_stat_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=3)
    @pytest.mark.need_maintenance
    def test_create_cicd_job_count_single_stat_report_widget_ou_basic(self, create_generic_object, widgetreusable_object, create_ou_object, get_integration_obj):
        cicd_job_count_single_stat_api_url = create_generic_object.connection["base_url"] + "cicd_scm/job_counts"

        products_id = temp_project_id
        
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "basic_OU_" + str(ts)

        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name,
            arg_required_integration_type="jenkins",
            arg_req_integration_ids=get_integration_obj,
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        cicd_job_count_single_stat_widget_payload = generate_create_cicd_job_count_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_project_id=products_id,
            arg_ou_id=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("==== retrieving the widget response ====")
        cicd_job_count_single_stat_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=cicd_job_count_single_stat_api_url,
            arg_req_payload=cicd_job_count_single_stat_widget_payload,
        )
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])
        data_check_flag = True
        try:
            api_records = (cicd_job_count_single_stat_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=4)
    @pytest.mark.need_maintenance
    def test_create_cicd_job_count_single_stat_report_widget_filter_jobName(self, create_generic_object, widgetreusable_object, get_integration_obj):
        cicd_job_count_single_stat_api_url = create_generic_object.connection["base_url"] + "cicd_scm/job_counts"

        products_id = temp_project_id
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["job_name"],
                                                 arg_integration_ids=get_integration_obj)
        temp_records = [get_filter_response['records'][0]['job_name']]
        cicd_jobs_value = []
        for eachjob in temp_records[0]:
            cicd_jobs_value.append(eachjob['key'])

        required_filters_needs_tobe_applied = ["job_names"]
        filter_value = [cicd_jobs_value]
        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])

        cicd_job_count_single_stat_widget_payload = generate_create_cicd_job_count_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_project_id=products_id,
            arg_job_start_date=30,
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )
        LOG.info("==== retrieving the widget response ====")
        cicd_job_count_single_stat_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=cicd_job_count_single_stat_api_url,
            arg_req_payload=cicd_job_count_single_stat_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (cicd_job_count_single_stat_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag is True, "unable to create the report , No Data Available"

    @pytest.mark.run(order=5)
    @pytest.mark.need_maintenance
    def test_create_cicd_job_count_single_stat_report_widget_filter_jobName_basicOU(self, create_generic_object, widgetreusable_object, create_ou_object, get_integration_obj):
        cicd_job_count_single_stat_api_url = create_generic_object.connection["base_url"] + "cicd_scm/job_counts"
        products_id = temp_project_id
        ts = calendar.timegm(time.gmtime())
        new_ou_name = "basic_OU_" + str(ts)
        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name,
            arg_required_integration_type="jenkins",
            arg_req_integration_ids=get_integration_obj,
        )
        LOG.info("new ou name")
        LOG.info(new_ou_name)
        get_filter_response = create_generic_object.get_filter_options(arg_filter_type=["job_name"],
                                                 arg_integration_ids=get_integration_obj)

        temp_records = [get_filter_response['records'][0]['job_name']]
        cicd_jobs_value = []
        for eachjob in temp_records[0]:
            cicd_jobs_value.append(eachjob['key'])

        required_filters_needs_tobe_applied = ["job_names"]
        filter_value = [cicd_jobs_value]
        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])

        cicd_job_count_single_stat_widget_payload = generate_create_cicd_job_count_single_stat_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_project_id=products_id,
            arg_job_start_date=30,
            arg_ou_id=[str(get_generate_OU_response['success'][0])],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )
        LOG.info("==== retrieving the widget response ====")
        cicd_job_count_single_stat_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=cicd_job_count_single_stat_api_url,
            arg_req_payload=cicd_job_count_single_stat_widget_payload,
        )
        data_check_flag = True
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])
        try:
            api_records = (cicd_job_count_single_stat_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag is True, "unable to create the report , No Data Available"
