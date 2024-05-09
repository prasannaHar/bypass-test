import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14

class TestScmFilesReport:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    def test_scm_files_001(self, create_scm_files_object,
                                     create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm files report"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        create_scm_files_report = create_scm_files_object.scm_files_report(integration_id=get_integration_obj)

        assert create_scm_files_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_scm_files_report(),
                                                       create_scm_files_report,
                                                       "Schema validation failed for scm files report  widget endpoint")

    @pytest.mark.run(order=2)
    def test_scm_files_002(self, create_scm_files_object,
                                        create_widget_helper_object, widget_schema_validation, get_integration_obj):

        """Validate alignment of scm files report and compare the same with Drilldown"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        create_scm_files_report = create_scm_files_object.scm_files_report(integration_id=get_integration_obj)

        assert create_scm_files_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_scm_files_report(),
                                                       create_scm_files_report,
                                                       "Schema validation failed for scm files report  widget endpoint")

        LOG.info("==== Validate the data in the widget of SCM files report with drilldown ====")
        key_value = create_scm_files_object.scm_files_report(keys=True, integration_id=get_integration_obj)
        for eachRecord in key_value:
            drilldown = create_scm_files_object.scm_files_report_drilldown(integration_id=get_integration_obj,
                                                                           module=eachRecord['key'],
                                                                           repo_id=eachRecord['repo_id'])
            assert eachRecord['count'] == drilldown, "Mismatch data in the drill down: " + eachRecord['key']

    @pytest.mark.run(order=3)
    def test_scm_files_003(self, create_scm_files_object, create_generic_object,
                                             create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm files report"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        filters = {"committed_at": {"$gt": gt, "$lt": lt}, }
        create_scm_files_report = create_scm_files_object.scm_files_report(integration_id=get_integration_obj,
                                                                           var_filters=filters)

        assert create_scm_files_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_scm_files_report(),
                                                       create_scm_files_report,
                                                       "Schema validation failed for scm files report  widget endpoint")

    @pytest.mark.run(order=4)
    def test_scm_files_004(self, create_scm_files_object, create_generic_object,
                                                create_widget_helper_object, widget_schema_validation, get_integration_obj):

        """Validate alignment of scm files report and compare the same with Drilldown"""

        LOG.info("==== create widget with available filter ====")
        """sending snapshot_range"""
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        filters = {"committed_at": {"$gt": gt, "$lt": lt}, }
        create_scm_files_report = create_scm_files_object.scm_files_report(integration_id=get_integration_obj,
                                                                           var_filters=filters)

        assert create_scm_files_report, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_scm_files_report(),
                                                       create_scm_files_report,
                                                       "Schema validation failed for scm files report  widget endpoint")

        LOG.info("==== Validate the data in the widget of SCM files report with drilldown ====")
        key_value = create_scm_files_object.scm_files_report(keys=True, integration_id=get_integration_obj,
                                                             var_filters=filters)
        for eachRecord in key_value:
            drilldown = create_scm_files_object.scm_files_report_drilldown(integration_id=get_integration_obj,
                                                                           module=eachRecord['key'],
                                                                           repo_id=eachRecord['repo_id'],
                                                                           var_filters=filters)
            assert eachRecord['count'] == drilldown, "Mismatch data in the drill down: " + eachRecord['key']
