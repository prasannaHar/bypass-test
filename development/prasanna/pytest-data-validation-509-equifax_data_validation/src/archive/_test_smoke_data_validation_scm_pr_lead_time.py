import logging
import pandas as pd
import pytest
from copy import deepcopy

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

req_filter = [ ("creator"), ("label"), ("pr_created_at"), ("velocity_config_id")]
req_percentile_filter = [("p90"), ("p95")]
req_filter_values1 = [("slow"), ("slow,needs_attention"), ("missing"), ("good,missing")]


class TestSCMPRLeadTime:
    @pytest.mark.run(order=1)
    def test_scm_pr_lead_time_by_stage_report_creation(self, create_generic_object, widgetreusable_object,
                                                       drilldown_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_ou_ids=True
        )
        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

    @pytest.mark.run(order=2)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown(self, create_generic_object, widgetreusable_object,
                                                                  drilldown_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_ou_ids=True
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:

            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"], arg_ou_ids=True
            )

            key = each_records["key"]
            widget_mean = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload)
            total_record_count = int(lead_time_drilldown_response['count'])
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            drilldown_mean = drilldown_mean + int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            if total_record_count != 0:
                drilldown_mean = drilldown_mean / total_record_count
            assert widget_mean == drilldown_mean

    @pytest.mark.run(order=3)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_median(self, create_generic_object, drilldown_object,
                                                                         widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_ou_ids=True
        )
        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"], arg_ou_ids=True
            )
            key = each_records["key"]
            widget_mean = each_records['median']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )
            total_record_count = int(lead_time_drilldown_response['count'])
            # print("total_record_count", total_record_count)
            required_timeduration_values = []
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            temp_drilldown_median = int(
                                lead_time_drilldown_response['records'][record]['data'][data]['median'])
                            required_timeduration_values.append(temp_drilldown_median)
                        except:
                            required_timeduration_values.append(0)
                            continue

            required_timeduration_values.sort()
            median_index = int(len(required_timeduration_values) / 2)
            drill_down_median = required_timeduration_values[median_index]
            assert -2 <= (widget_mean - drill_down_median) <= 2, "widget versus drilldown medians are not matching"

    @pytest.mark.run(order=4)
    @pytest.mark.parametrize("req_filter", req_filter)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drill_down_with_filters(self, req_filter, create_generic_object,
                                                                                widgetreusable_object,
                                                                                create_filterresuable_object,
                                                                                drilldown_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'
        req_filter_names_and_value_pair = []
        if req_filter == "pr_created_at":
            LOG.info("====  generating dynamic filter value pair ===")
            number_of_months, last_month_start_date, last_month_end_date = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods(
                "LAST_TWO_QUARTERS")
            req_filter_names_and_value_pair = [
                [req_filter, {"$gt": str(last_month_start_date), "$lt": str(last_month_end_date)}],
            ]
        elif req_filter == "velocity_config_id":
            LOG.info("====  generating dynamic filter value pair ===")
            req_filter_names_and_value_pair = [
                [req_filter, create_generic_object.env["env_scm_velocity_config_id"]]
            ]
        else:
            LOG.info("====  generating dynamic filter value pair ===")
            LOG.info("====  generating the filter values ===")
            required_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_req_integration_ids=get_integration_obj,
                arg_required_filter=req_filter,
            )

            reviewer_filter_value = [req_filter, required_filter_values]
            req_filter_names_and_value_pair = [reviewer_filter_value]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            arg_ou_ids=True

        )
        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        # print("requried_random_records", requried_random_records)

        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair, arg_ou_ids=True
            )
            key = each_records["key"]
            widget_mean = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            assert total_record_count != 0, "no data present in the drill-down"
            # print("total_record_count", total_record_count)

            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            drilldown_mean = drilldown_mean + int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            drilldown_mean = drilldown_mean / total_record_count
            assert widget_mean == drilldown_mean

    @pytest.mark.run(order=5)
    @pytest.mark.parametrize("req_filter_values1", req_filter_values1)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_with_rating_filters(self, req_filter_values1,create_generic_object,
                                                                                      drilldown_object,
                                                                                      widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'
        req_filter_values_copy = deepcopy(req_filter_values1)
        req_filter_names_and_value_pair = []

        LOG.info("====  generating dynamic filter value pair ===")
        LOG.info("====  generating the filter values ===")
        if "," in req_filter_values1:
            req_filter_values1 = req_filter_values1.split(",")
        else:
            req_filter_values1 = [req_filter_values1]
        reviewer_filter_value = ["ratings", req_filter_values1]
        req_filter_names_and_value_pair = [reviewer_filter_value]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair, arg_ou_ids=True
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "Unable to create the report"
        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair, arg_ou_ids=True
            )

            key = each_records["key"]
            widget_mean = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
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
                            assert fetch_record_rating.lower() in req_filter_values_copy, "records are not matching as per the selected rating"
                        except:
                            continue

    @pytest.mark.run(order=6)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_ou_filter_based(self, create_generic_object,
                                                                                  create_filterresuable_object,
                                                                                  create_ou_object, drilldown_object,
                                                                                  widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'
        required_project_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_req_integration_ids=get_integration_obj,
            arg_required_filter="project",
            arg_retrieve_only_values=True
        )

        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-filter-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            # arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_ou_ids=[new_ou_id]
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )
        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                # arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_ou_ids=[new_ou_id]
            )
            key = each_records["key"]
            widget_mean = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            assert total_record_count != 0, "no data present in the drill-down"
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            drilldown_mean = drilldown_mean + int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            drilldown_mean = drilldown_mean / total_record_count
            assert widget_mean == drilldown_mean
        LOG.info("====  cleanup - delete required OU ===")
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=7)
    @pytest.mark.parametrize("req_filter_values1", req_filter_values1,)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_with_rating_filters_ou_filter_based(self,req_filter_values1,
                                                                                                      create_generic_object,
                                                                                                      widgetreusable_object,
                                                                                                      create_filterresuable_object,
                                                                                                      create_ou_object,
                                                                                                      drilldown_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_project_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_req_integration_ids=get_integration_obj,
            arg_required_filter="project",
            arg_retrieve_only_values=True
        )
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}

        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-filter-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]
        req_filter_values_copy = deepcopy(req_filter_values1)
        req_filter_names_and_value_pair = []

        LOG.info("====  generating dynamic filter value pair ===")
        LOG.info("====  generating the filter values ===")
        if "," in req_filter_values1:
            req_filter_values1 = req_filter_values1.split(",")
        else:
            req_filter_values1 = [req_filter_values1]
        reviewer_filter_value = ["ratings", req_filter_values1]
        req_filter_names_and_value_pair = [reviewer_filter_value]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            # arg_required_integration_ids=get_integration_obj,
            arg_ou_ids=[int(new_ou_id)],
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_ou_ids=[new_ou_id],
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )
            key = each_records["key"]
            widget_mean = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )
            LOG.info(lead_time_drilldown_payload)
            if lead_time_drilldown_response['records']:
                total_record_count = int(lead_time_drilldown_response['count'])
                for record in range(total_record_count):
                    for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                        if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                            try:
                                fetch_record_rating = \
                                    lead_time_drilldown_response['records'][record]['data'][data]['velocity_stage_result'][
                                        'rating']
                                # print(fetch_record_rating)
                                assert fetch_record_rating.lower() in req_filter_values_copy, "records are not matching as per the selected rating"
                            except:
                                continue
            else:
                LOG.info("No data in lead time drilldown widget")
            create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=8)
    @pytest.mark.parametrize("req_filter", req_filter)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_with_filters_ou_fiter_based(self,req_filter,
                                                                                              create_generic_object,
                                                                                              create_filterresuable_object,
                                                                                              create_ou_object,
                                                                                              widgetreusable_object,
                                                                                              drilldown_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_project_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_req_integration_ids=get_integration_obj,
            arg_required_filter="project",
            arg_retrieve_only_values=True
        )

        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}
        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-filter-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]
        req_filter_names_and_value_pair = []
        if req_filter == "pr_created_at":
            LOG.info("====  generating dynamic filter value pair ===")
            number_of_months, last_month_start_date, last_month_end_date = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods(
                "LAST_TWO_QUARTERS")
            req_filter_names_and_value_pair = [
                [req_filter, {"$gt": str(last_month_start_date), "$lt": str(last_month_end_date)}],
            ]

        elif req_filter == "velocity_config_id":
            LOG.info("====  generating dynamic filter value pair ===")
            req_filter_names_and_value_pair = [
                [req_filter, create_generic_object.env["env_scm_velocity_config_id"]]]

        else:
            LOG.info("====  generating dynamic filter value pair ===")
            LOG.info("====  generating the filter values ===")
            required_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_req_integration_ids=get_integration_obj,
                arg_required_filter=req_filter
            )

            reviewer_filter_value = [req_filter, required_filter_values]
            req_filter_names_and_value_pair = [reviewer_filter_value]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_ou_ids=[new_ou_id],
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )
        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_ou_ids=[new_ou_id],
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )
            key = each_records["key"]
            widget_mean = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )
            total_record_count = int(lead_time_drilldown_response['count'])
            assert total_record_count != 0, "no data present in the drill-down"
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            drilldown_mean = drilldown_mean + int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue

            drilldown_mean = drilldown_mean / total_record_count
            assert widget_mean == drilldown_mean
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=9)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_median_ou_filter_based(self, create_generic_object,
                                                                                         create_filterresuable_object,
                                                                                         create_ou_object,
                                                                                         drilldown_object,
                                                                                         widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_project_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_req_integration_ids=get_integration_obj,
            arg_required_filter="project",
            arg_retrieve_only_values=True
        )

        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}

        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-filter-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_ou_ids=[str(new_ou_id)]

        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:

            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_ou_ids=[str(new_ou_id)]
            )

            key = each_records["key"]
            widget_mean = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload)
            total_record_count = int(lead_time_drilldown_response['count'])
            assert total_record_count != 0, "no data present in the drill-down"
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            drilldown_mean = drilldown_mean + int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            drilldown_mean = drilldown_mean / total_record_count
            assert widget_mean == drilldown_mean
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=10)
    @pytest.mark.parametrize("req_percentile_filter", req_percentile_filter)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_percentile_based_ou_filter_based(self,req_percentile_filter,
                                                                                                   create_generic_object,
                                                                                                   widgetreusable_object,
                                                                                                   create_filterresuable_object,
                                                                                                   create_ou_object,
                                                                                                   drilldown_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_project_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_req_integration_ids=get_integration_obj,
            arg_required_filter="project",
            arg_retrieve_only_values=True
        )
        ou_creation_required_filter_key_value_pairs = {"projects": required_project_filter_values}

        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-filter-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_ou_ids=[new_ou_id],
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )
        data_check_flag = True

        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")

        assert data_check_flag is True, "Unable to create the report"
        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        for each_records in requried_random_records:
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_ou_ids=[new_ou_id],
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"]
            )

            key = each_records["key"]
            widget_mean = each_records['mean']
            widget_p95 = each_records[req_percentile_filter]

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            # print("total_record_count", total_record_count)
            required_timeduration_values = []
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            temp_drilldown_median = int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                            required_timeduration_values.append(temp_drilldown_median)
                        except:
                            required_timeduration_values.append(0)
                            continue

            required_timeduration_values.sort()
            df = pd.DataFrame(required_timeduration_values, columns=["values_period"])
            drill_down_p95 = df.values_period.quantile(0.95)
            if req_percentile_filter == "p90":
                drill_down_p95 = df.values_period.quantile(0.90)
            assert int(widget_p95) == int(drill_down_p95) or (
                    -1 <= (widget_p95 - drill_down_p95) <= 1), "P95 - widget and drill-down data is not consistent"

        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=11)
    @pytest.mark.parametrize("req_percentile_filter", req_percentile_filter, )
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_percentile(self, req_percentile_filter, drilldown_object,
                                                                             create_generic_object,
                                                                             widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_ou_ids=True
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        for each_records in requried_random_records:
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"], arg_ou_ids=True
            )
            key = each_records["key"]
            widget_mean = each_records['mean']
            widget_p95 = each_records[req_percentile_filter]
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            required_timeduration_values = []

            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            temp_drilldown_median = int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                            required_timeduration_values.append(temp_drilldown_median)

                        except:
                            required_timeduration_values.append(0)
                            continue

            required_timeduration_values.sort()
            df = pd.DataFrame(required_timeduration_values, columns=["values_period"])
            drill_down_p95 = df.values_period.quantile(0.95)
            if req_percentile_filter == "p90":
                drill_down_p95 = df.values_period.quantile(0.90)
            assert int(widget_p95) == int(drill_down_p95) or (
                    -1 <= (widget_p95 - drill_down_p95) <= 1), "P95 - widget and drill-down data is not consistent"

    @pytest.mark.run(order=12)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_ou_user_attribute_based(self, create_generic_object,
                                                                                          create_filterresuable_object,
                                                                                          create_ou_object,
                                                                                          drilldown_object,
                                                                                          widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_attrib_filter_values = create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_required_user_attrib=create_generic_object.api_data["env_org_users_preferred_attribute"],
            arg_retrieve_only_values=True
        )
        ou_creation_required_user_attrib_filter_key_value_pairs = {
            create_generic_object.api_data["env_org_users_preferred_attribute"]: required_attrib_filter_values}
        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-user-attribute-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_user_attrib_filter_key_value_pairs

        )
        new_ou_id = (ou_creation_response['success'])[0]
        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            # arg_required_integration_ids=get_integration_obj,
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_ou_ids=[new_ou_id]
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )
        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"
        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                # arg_required_integration_ids=get_integration_obj,
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_ou_ids=[new_ou_id]
            )
            key = each_records["key"]
            widget_mean = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            assert total_record_count != 0, "no data present in the drill-down"
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            drilldown_mean = drilldown_mean + int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            drilldown_mean = drilldown_mean / total_record_count
            assert widget_mean == drilldown_mean

        LOG.info("====  cleanup - delete required OU ===")
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=13)
    @pytest.mark.parametrize("req_percentile_filter", req_percentile_filter, )
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_percentile_based_ou_user_attribute_based(self,req_percentile_filter,
                                                                                                           create_generic_object,
                                                                                                           create_ou_object,
                                                                                                           drilldown_object,
                                                                                                           create_filterresuable_object,
                                                                                                           widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_attrib_filter_values = create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_required_user_attrib=create_generic_object.api_data["env_org_users_preferred_attribute"],
            arg_retrieve_only_values=True
        )

        ou_creation_required_user_attrib_filter_key_value_pairs = {
            create_generic_object.api_data["env_org_users_preferred_attribute"]: required_attrib_filter_values}
        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-user-attribute-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_user_attrib_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]
        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            arg_ou_ids=[new_ou_id],
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(

            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)
        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_ou_ids=[new_ou_id],
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
            )

            key = each_records["key"]
            widget_mean = each_records['mean']
            widget_p95 = each_records[req_percentile_filter]

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            required_timeduration_values = []
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            temp_drilldown_median = int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                            required_timeduration_values.append(temp_drilldown_median)

                        except:
                            required_timeduration_values.append(0)
                            continue

            required_timeduration_values.sort()
            df = pd.DataFrame(required_timeduration_values, columns=["values_period"])
            drill_down_p95 = df.values_period.quantile(0.95)

            if req_percentile_filter == "p90":
                drill_down_p95 = df.values_period.quantile(0.90)
            assert int(widget_p95) == int(drill_down_p95) or (
                    -1 <= (widget_p95 - drill_down_p95) <= 1), "P95 - widget and drill-down data is not consistent"
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=14)
    @pytest.mark.skip(reason="this test case need maintenance")
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_median_ou_user_attribute_based(self,
                                                                                                 create_generic_object,
                                                                                                 create_filterresuable_object,
                                                                                                 create_ou_object,
                                                                                                 drilldown_object,
                                                                                                 widgetreusable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_attrib_filter_values = create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_required_user_attrib=create_generic_object.api_data["env_org_users_preferred_attribute"],
            arg_retrieve_only_values=True
        )

        ou_creation_required_user_attrib_filter_key_value_pairs = {
            create_generic_object.api_data["env_org_users_preferred_attribute"]: required_attrib_filter_values}
        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-user-attribute-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_user_attrib_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]
        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            # arg_required_integration_ids=get_integration_obj,
            arg_ou_ids=[new_ou_id],
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"]
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )
        data_check_flag = True

        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                # arg_required_integration_ids=get_integration_obj,
                arg_ou_ids=[new_ou_id],
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"]
            )

            key = each_records["key"]
            widget_mean = each_records['median']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            # print("total_record_count", total_record_count)
            required_timeduration_values = []
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            temp_drilldown_median = int(
                                lead_time_drilldown_response['records'][record]['data'][data]['median'])
                            required_timeduration_values.append(temp_drilldown_median)

                        except:
                            required_timeduration_values.append(0)
                            continue

            df = pd.DataFrame(required_timeduration_values, columns=["values_period"])
            drill_down_median = df['values_period'].median()

            assert int(widget_mean) == int(drill_down_median)
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=15)
    @pytest.mark.parametrize("req_filter", req_filter)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_with_filters_ou_user_attribute_based(self,req_filter,
                                                                                                       create_generic_object,
                                                                                                       create_filterresuable_object,
                                                                                                       create_ou_object,
                                                                                                       widgetreusable_object,
                                                                                                       drilldown_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'
        required_attrib_filter_values = create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_required_user_attrib=create_generic_object.api_data["env_org_users_preferred_attribute"],
            arg_retrieve_only_values=True
        )

        ou_creation_required_user_attrib_filter_key_value_pairs = {
            create_generic_object.api_data["env_org_users_preferred_attribute"]: required_attrib_filter_values}
        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-user-attribute-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_user_attrib_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]
        req_filter_names_and_value_pair = []
        if req_filter == "pr_created_at":
            LOG.info("====  generating dynamic filter value pair ===")
            number_of_months, last_month_start_date, last_month_end_date = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods(
                "LAST_TWO_QUARTERS")
            req_filter_names_and_value_pair = [
                [req_filter, {"$gt": str(last_month_start_date), "$lt": str(last_month_end_date)}],
            ]

        elif req_filter == "velocity_config_id":

            LOG.info("====  generating dynamic filter value pair ===")
            req_filter_names_and_value_pair = [
                [req_filter, create_generic_object.env["env_scm_velocity_config_id"]]
            ]
        else:
            LOG.info("====  generating dynamic filter value pair ===")
            LOG.info("====  generating the filter values ===")
            required_filter_values = create_filterresuable_object.scm_pr_lead_time_generate_req_filter_values(
                arg_app_url=create_generic_object.connection["base_url"],
                arg_req_integration_ids=get_integration_obj,
                arg_required_filter=req_filter
            )

            reviewer_filter_value = [req_filter, required_filter_values]
            req_filter_names_and_value_pair = [reviewer_filter_value]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            # arg_required_integration_ids=get_integration_obj,
            arg_ou_ids=[new_ou_id],
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair

        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )
        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                # arg_required_integration_ids=get_integration_obj,
                arg_ou_ids=[new_ou_id],
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )
            key = each_records["key"]
            widget_mean = each_records['mean']
            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
                arg_req_payload=lead_time_drilldown_payload
            )

            total_record_count = int(lead_time_drilldown_response['count'])
            assert total_record_count != 0, "no data present in the drill-down"
            # print("total_record_count", total_record_count)
            for record in range(total_record_count):
                for data in range(len(lead_time_drilldown_response['records'][record]['data'])):
                    if lead_time_drilldown_response['records'][record]['data'][data]['key'] == key:
                        try:
                            drilldown_mean = drilldown_mean + int(
                                lead_time_drilldown_response['records'][record]['data'][data]['mean'])
                        except:
                            continue
            drilldown_mean = drilldown_mean / total_record_count
            assert widget_mean == drilldown_mean
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])

    @pytest.mark.run(order=16)
    @pytest.mark.need_maintenance
    @pytest.mark.parametrize("req_filter_values1", req_filter_values1)
    def test_scm_pr_lead_time_by_stage_report_widget_vs_drilldown_with_rating_filters_ou_user_attribute_based(self,req_filter_values1,
                                                                                                              create_generic_object,
                                                                                                              widgetreusable_object,
                                                                                                              drilldown_object,
                                                                                                              create_ou_object,
                                                                                                              create_filterresuable_object, get_integration_obj):
        widget_api_url = create_generic_object.connection["base_url"] + "velocity"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'velocity/values'

        required_attrib_filter_values = create_filterresuable_object.ou_retrieve_req_org_user_attrib_filter_values(
            arg_app_url=create_generic_object.connection["base_url"],
            arg_required_user_attrib=create_generic_object.api_data["env_org_users_preferred_attribute"],
            arg_retrieve_only_values=True
        )
        ou_creation_required_user_attrib_filter_key_value_pairs = {
            create_generic_object.api_data["env_org_users_preferred_attribute"]: required_attrib_filter_values}
        ou_creation_response = create_ou_object.create_filter_based_ou(
            arg_required_ou_name="py-scm-user-attribute-based-ou",
            arg_required_integration_id=get_integration_obj,
            arg_required_filters_key_value_pair=ou_creation_required_user_attrib_filter_key_value_pairs
        )
        new_ou_id = (ou_creation_response['success'])[0]
        req_filter_values_copy = deepcopy(req_filter_values1)
        req_filter_names_and_value_pair = []

        LOG.info("====  generating dynamic filter value pair ===")
        LOG.info("====  generating the filter values ===")

        if "," in req_filter_values1:
            req_filter_values1 = req_filter_values1.split(",")
        else:
            req_filter_values1 = [req_filter_values1]
        reviewer_filter_value = ["ratings", req_filter_values1]
        req_filter_names_and_value_pair = [reviewer_filter_value]

        LOG.info("====  generating widget payload ===")
        widget_payload_generation = drilldown_object.generate_scm_pr_lead_time_by_stage_widget_payload(
            # arg_required_integration_ids=get_integration_obj,
            arg_ou_ids=[new_ou_id],
            arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
            arg_req_dynamic_fiters=req_filter_names_and_value_pair
        )

        LOG.info("====  retrieving the widget response ===")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=widget_api_url,
            arg_req_payload=widget_payload_generation
        )

        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"

        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"

        LOG.info("====  retrieving three random records ===")
        requried_random_records = widgetreusable_object.retrieve_three_random_records(widget_response)

        for each_records in requried_random_records:
            drilldown_mean = 0
            lead_time_drilldown_payload = drilldown_object.generate_scm_pr_lead_time_by_stage_drilldown_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_ou_ids=[new_ou_id],
                arg_velocity_config_id_to_be_used=create_generic_object.env["env_scm_velocity_config_id"],
                arg_required_stage=each_records["key"],
                arg_req_dynamic_fiters=req_filter_names_and_value_pair
            )

            key = each_records["key"]
            widget_mean = each_records['mean']

            lead_time_drilldown_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=drilldown_api_url,
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

            create_ou_object.delete_required_ou(arg_requried_ou_id=[str(new_ou_id)])
