import logging
import random

import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestScmreworkReport:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_rework_001(self, create_scm_rework_object, create_generic_object,
                            create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm rework report"""

        LOG.info("==== create widget with available filter ====")
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        create_scm_rework_report = create_scm_rework_object.scm_rework_report(integration_id=get_integration_obj,
                                                                              interval_config=gt)

        assert create_scm_rework_report, "widget is not created"

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_rework_002(self, create_scm_rework_object, create_generic_object,
                            create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm rework report"""

        LOG.info("==== create widget with available filter ====")
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        create_scm_rework_report = create_scm_rework_object.scm_rework_report(integration_id=get_integration_obj,
                                                                              interval_config=gt)

        assert create_scm_rework_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of SCM files report with drilldown ====")
        key_value = create_scm_rework_object.scm_rework_report(integration_id=get_integration_obj, interval_config=gt,
                                                               keys=True)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_rework_object.scm_rework_report_drilldown(integration_id=get_integration_obj,
                                                                             interval_config=gt,
                                                                             key=eachRecord['key'])
            assert eachRecord['count'] == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + eachRecord['key']

    @pytest.mark.run(order=3)
    @pytest.mark.regression
    def test_scm_rework_003(self, create_scm_rework_object, create_generic_object,
                            create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm rework report"""

        LOG.info("==== create widget with available filter ====")
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        gt_committed_at, lt_committed_at = create_generic_object.get_epoc_time(value=2, type="days")

        authors = create_generic_object.get_filter_options_scm(arg_filter_type="author",report_type="commits")

        filters = {"committed_at": {"$gt": gt_committed_at, "$lt": lt_committed_at},
                   "exclude": {"authors": authors}}

        create_scm_rework_report = create_scm_rework_object.scm_rework_report(integration_id=get_integration_obj,
                                                                              interval_config=gt, var_filters=filters)

        assert create_scm_rework_report, "widget is not created"

    @pytest.mark.run(order=4)
    @pytest.mark.regression
    def test_scm_rework_004(self, create_scm_rework_object, create_generic_object,
                            create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm rework report"""

        LOG.info("==== create widget with available filter ====")
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        gt_committed_at, lt_committed_at = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])

        authors = create_generic_object.get_filter_options_scm(arg_filter_type="author",report_type="commits")

        filters = {"committed_at": {"$gt": gt_committed_at, "$lt": lt_committed_at},
                   "exclude": {"authors": authors}}

        create_scm_rework_report = create_scm_rework_object.scm_rework_report(integration_id=get_integration_obj,
                                                                              interval_config=gt, var_filters=filters)

        assert create_scm_rework_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of SCM files report with drilldown ====")
        key_value = create_scm_rework_object.scm_rework_report(integration_id=get_integration_obj, interval_config=gt,
                                                               keys=True, var_filters=filters)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_rework_object.scm_rework_report_drilldown(integration_id=get_integration_obj,
                                                                             interval_config=gt,
                                                                             key=eachRecord['key'], var_filters=filters)
            assert eachRecord['count'] == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + eachRecord['key']
