import json
import logging
import pytest
import calendar
import time

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueHygiene:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_hygiene_report_creation(self, create_generic_object, drilldown_object, widgetreusable_object,
                                           get_integration_obj):
        """Test issue Hygiene report creation"""
        widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        LOG.info("jira integration id : {}".format(get_integration_obj))
        LOG.info("==== generating widget payload ====")
        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
            arg_required_integration_ids=get_integration_obj,
        )
        LOG.info("=====retrieving the widget response=====")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=drilldown_api_url,
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

        hygiene_types = create_generic_object.api_data["hygiene_types"]
        for eachHygieneType in hygiene_types:
            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_required_hygiene_types=[eachHygieneType],
                arg_across="project"
            )
            """retrieving the widget response"""
            widget_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=widget_api_url,
                arg_req_payload=widget_payload_generation
            )
            try:
                api_records = (widget_response['records'])
                if len(api_records) > 0:
                    pass
            except:
                pass

    @pytest.mark.run(order=2)
    def test_issue_hygiene_report_data_consistency_widget_vs_drill_down(self, create_generic_object, drilldown_object,
                                                                        widgetreusable_object, get_integration_obj):
        """Test for the issue in hygiene report data of consistency widget vs drill down"""
        widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        LOG.info("=====generating widget payload====")
        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
            arg_required_integration_ids=get_integration_obj,
        )

        LOG.info("=======retrieving the widget response======")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=drilldown_api_url,
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

        assert data_check_flag == True, "Unable to create the report"
        hygiene_types = create_generic_object.api_data["hygiene_types"]
        for eachHygieneType in hygiene_types:
            widget_records_count = 0
            drill_down_records_count = 0
            LOG.info("===== generating widget payload ====")
            payload_generated = drilldown_object.generate_hygiene_report_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_required_hygiene_types=[eachHygieneType],
                arg_across="project"
            )
            LOG.info("===== retrieving the widget response =====")
            widget_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=widget_api_url,
                arg_req_payload=payload_generated
            )
            try:
                if len(widget_response['records']) > 0:
                    widget_records_count = 0
                    for each_record in widget_response['records']:
                        widget_records_count = widget_records_count + each_record["total_tickets"]
                    LOG.info("====== retrieving the widget response =====")
                    drill_down_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=drilldown_api_url,
                        arg_req_payload=payload_generated
                    )
                    drill_down_records_count = (drill_down_response["_metadata"])["total_count"]
                else:
                    drill_down_records_count=0
            except:
                pass
            assert widget_records_count == drill_down_records_count, "widget and drill-down count is not matching..."

    @pytest.mark.run(order=3)
    def test_issue_hygiene_report_data_consistency_widget_vs_drill_down_custom_hygiene(self, create_generic_object,
                                                                                       drilldown_object,
                                                                                       widgetreusable_object):
        """test issue hygiene report data consistency widget vs drill down custom hygiene"""
        widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        LOG.info("===== generating widget payload =====")
        jira_integration_ids = create_generic_object.get_integration_id()
        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
            arg_required_integration_ids=jira_integration_ids,
        )

        LOG.info("======= retrieving the widget response =====")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=drilldown_api_url,
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
        custom_hygiene_details = create_generic_object.env["env_custom_hygiene_details"]
        custom_hygiene_details = custom_hygiene_details.split("_")
        for eachHygieneType in custom_hygiene_details:
            drill_down_records_count = 0
            LOG.info("====== generating widget payload ======")
            hygiene_type = drilldown_object.payload_custom_vs_filters()
            payload_generated = drilldown_object.generate_hygiene_report_custom_hygiene_payload(
                arg_required_integration_ids=jira_integration_ids,
                arg_required_hygiene_types=[],
                arg_req_dynamic_fiters=hygiene_type[eachHygieneType],
                arg_exclude_custom_fields=True
            )

            LOG.info("==== retrieving the widget response  ====")
            widget_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=widget_api_url,
                arg_req_payload=payload_generated
            )
            try:
                if len(widget_response['records']) > 0:
                    widget_records_count = 0
                    for each_record in widget_response['records']:
                        widget_records_count = widget_records_count + each_record["total_tickets"]
                    LOG.info("==== retrieving the widget response ====")
                    drill_down_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=drilldown_api_url,
                        arg_req_payload=payload_generated
                    )
                    drill_down_records_count = int((drill_down_response["_metadata"])["total_count"])

                    LOG.info("widget records count:  {}".format(widget_records_count))
                    LOG.info("drilldown records count {}".format(drill_down_records_count))
                else:
                    drill_down_records_count = 0
            except:
                pass
            assert widget_records_count == drill_down_records_count, "widget and drill-down count is not matching..."

    @pytest.mark.run(order=4)
    def test_issue_hygiene_report_data_consistency_widget_vs_drill_down_baic_ou(self, create_generic_object,
                                                                                create_ou_object, widgetreusable_object,
                                                                                drilldown_object, get_integration_obj):
        """test issue hygiene report data consistency widget vs drill down baic ou"""
        widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        ts = calendar.timegm(time.gmtime())

        new_ou_name = "basic_OU_" + str(ts)
        LOG.info("new ou name : {}".format(new_ou_name))
        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name,
            arg_req_integration_ids=get_integration_obj,
        )
        LOG.info("===== generating widget payload =====")
        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_ou_ids=[str(get_generate_OU_response['success'][0])]

        )
        LOG.info("==== retrieving the widget response ====")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=drilldown_api_url,
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
        hygiene_types = create_generic_object.api_data["hygiene_types"]
        for eachHygieneType in hygiene_types:
            drill_down_records_count = 1

            LOG.info("==== generating widget payload ====")
            payload_generated = drilldown_object.generate_hygiene_report_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_required_hygiene_types=[eachHygieneType],
                arg_across="project",
                arg_ou_ids=[str(get_generate_OU_response['success'][0])]

            )
            LOG.info("===== retrieving the widget response =====")
            widget_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=widget_api_url,
                arg_req_payload=payload_generated
            )
            try:
                # api_records = (widget_response['records'])
                widget_records_count = 0
                if len(widget_response['records']) > 0:

                    for each_record in widget_response['records']:
                        widget_records_count = widget_records_count + each_record["total_tickets"]

                    LOG.info("==== retrieving the widget response ====")
                    drill_down_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=drilldown_api_url,
                        arg_req_payload=payload_generated
                    )
                    drill_down_records_count = (drill_down_response["_metadata"])["total_count"]
                else:
                    drill_down_records_count = 0


            except Exception as ex:
                pass
            assert widget_records_count == drill_down_records_count, "widget and drill-down count is not matching..."
        create_ou_object.delete_required_ou(arg_requried_ou_id=[str(get_generate_OU_response['success'][0])])

    @pytest.mark.run(order=5)
    def test_issue_hygiene_report_report_creation_filter_statuses(self, create_generic_object, drilldown_object,
                                                                  widgetreusable_object):
        """test issue hygiene report report creation filter statuses"""
        widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        jira_integration_ids = create_generic_object.get_integration_id()
        LOG.info("jira integration ID : {}".format(jira_integration_ids))
        required_filters_needs_tobe_applied = ["or"]
        filter_value = create_generic_object.api_data["filter_value"]

        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])

        LOG.info("==== generating widget payload ====")
        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
            arg_required_integration_ids=jira_integration_ids,

        )

        LOG.info("==== retrieving the widget response ====")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=drilldown_api_url,
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
        hygiene_types = create_generic_object.api_data["hygiene_types"]
        for eachHygieneType in hygiene_types:

            LOG.info("==== generating widget payload ====")
            widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
                arg_required_integration_ids=jira_integration_ids,
                arg_required_hygiene_types=[eachHygieneType],
                arg_across="project",
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,

            )
            LOG.info("===== retrieving the widget response =====")
            widget_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=widget_api_url,
                arg_req_payload=widget_payload_generation
            )
            try:
                api_records = (widget_response['records'])
                if len(api_records) > 0:
                    pass
            except:
                pass

    @pytest.mark.run(order=6)
    # @pytest.mark.regression
    def test_issue_hygiene_report_data_consistency_widget_vs_drill_down_filter_statuses(self, create_generic_object,
                                                                                        drilldown_object,
                                                                                        widgetreusable_object,
                                                                                        get_integration_obj):
        """test issue hygiene report data consistency widget vs drill down filter statuses"""
        widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        required_filters_needs_tobe_applied = ["or"]
        filter_value = [{"statuses": ["BACKLOG", "BLOCKED", "DONE"]}]

        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])
        LOG.info("==== generating widget payload ====")
        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
            arg_required_integration_ids=get_integration_obj,
        )

        LOG.info("===== retrieving the widget response =====")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=drilldown_api_url,
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

        hygiene_types = create_generic_object.api_data["hygiene_types"]
        for eachHygieneType in hygiene_types:
            drilldown_records_count = 0
            LOG.info("===== generating widget payload =====")
            payload_generated = drilldown_object.generate_hygiene_report_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_required_hygiene_types=[eachHygieneType],
                arg_across="project",
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
            )

            LOG.info("==== retrieving the widget response ====")
            widget_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=widget_api_url,
                arg_req_payload=payload_generated,
            )

            try:
                if (len(widget_response['records']) > 0):
                    widget_records_count = 0
                    for each_record in widget_response['records']:
                        widget_records_count = widget_records_count + each_record["total_tickets"]
                    ## retrieving the widget response
                    drill_down_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=drilldown_api_url,
                        arg_req_payload=payload_generated
                    )
                    drilldown_records_count = (drill_down_response["_metadata"])["total_count"]
                else:
                    drilldown_records_count = 0
            except:
                pass
            assert widget_records_count == drilldown_records_count, "widget and drill-down count is not matching..."

    @pytest.mark.run(order=7)
    def test_issue_hygiene_report_data_consistency_widget_vs_drill_down_filter_statuses_ouexclusion_basicOU(self,
                                                                                                            create_generic_object,
                                                                                                            create_ou_object,
                                                                                                            drilldown_object,
                                                                                                            widgetreusable_object,
                                                                                                            get_integration_obj):
        """test issue hygiene report data consistency widget vs drill down filter statuses ouexclusion basicOU"""
        widget_api_url = create_generic_object.connection["base_url"] + "jira_issues/tickets_report"
        drilldown_api_url = create_generic_object.connection["base_url"] + 'jira_issues/list'
        ts = calendar.timegm(time.gmtime())
        jira_integration_ids = create_generic_object.get_integration_id()
        new_ou_name = "basic_OU_" + str(ts)
        LOG.info("new ou name : {}".format(new_ou_name))

        get_generate_OU_response = create_ou_object.create_basic_integration_OU(
            arg_req_ou_name=new_ou_name)
        required_filters_needs_tobe_applied = ["or"]
        filter_value = [{"statuses":["BLOCKED","BACKLOG","BLOCKED IN DEV","IN PROGRESS"]}]

        req_filter_names_and_value_pair = []
        for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
            req_filter_names_and_value_pair.append([eachfilter, eachvalue])
        LOG.info("===== generating widget payload =====")
        widget_payload_generation = drilldown_object.generate_hygiene_report_payload(
            arg_required_integration_ids=get_integration_obj,
            arg_ou_ids=[str(get_generate_OU_response['success'][0])],
        )

        LOG.info(" ===== retrieving the widget response =====")
        widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=drilldown_api_url,
            arg_req_payload=widget_payload_generation
        )

        LOG.info("widget respose :{}".format(widget_response))
        data_check_flag = True
        try:
            api_records = (widget_response['records'])
            assert len(api_records) != 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
            if len(widget_response['records']) == 0:
                pytest.skip("unable to create the report , No Data Available")
        assert data_check_flag is True, "Unable to create the report"
        hygiene_types = create_generic_object.api_data["hygiene_types"]
        for eachHygieneType in hygiene_types:
            widget_records_count = 0
            drill_down_records_count = 1
            LOG.info("=== generating widget payload===")
            payload_generated = drilldown_object.generate_hygiene_report_payload(
                arg_required_integration_ids=get_integration_obj,
                arg_required_hygiene_types=[eachHygieneType],
                arg_across="project",
                arg_req_dynamic_fiters=req_filter_names_and_value_pair,
                arg_ou_ids=[str(get_generate_OU_response['success'][0])],
                arg_ou_exclusion="hygiene_types"
            )

            LOG.info("===== retrieving the widget response =====")
            widget_response = widgetreusable_object.retrieve_required_api_response(
                arg_req_api=widget_api_url,
                arg_req_payload=payload_generated,
            )
            try:
                if len(widget_response['records']) > 0:
                    widget_records_count = 0
                    for each_record in widget_response['records']:
                        widget_records_count = widget_records_count + each_record["total_tickets"]

                    drill_down_response = widgetreusable_object.retrieve_required_api_response(
                        arg_req_api=drilldown_api_url,
                        arg_req_payload=payload_generated
                    )

                    drill_down_records_count = (drill_down_response["_metadata"])["total_count"]
                else:
                    drill_down_records_count = 0
            except:
                pass
            assert widget_records_count == drill_down_records_count, "widget and drill-down count is not matching..."
