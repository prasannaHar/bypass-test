import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestScmFilesReport:
    @pytest.mark.run(order=1)
    def test_scm_coding_days_report_001(self, create_scm_coding_days_object,
                                        create_generic_object, get_integration_obj):
        """Validate alignment of scm coding days report"""

        LOG.info("==== create widget with available filter ====")
        """sending committed_at"""
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        committed_at = {"$gt": gt, "$lt": lt}
        create_scm_files_report = create_scm_coding_days_object.scm_coding_days_report(
            integration_id=get_integration_obj, committed_at=committed_at)

        assert create_scm_files_report, "widget is not created"

    @pytest.mark.run(order=2)
    def test_scm_coding_days_report_002(self, create_scm_coding_days_object,
                                        create_generic_object, get_integration_obj):
        """Validate alignment of scm coding days report"""

        LOG.info("==== create widget with available filter ====")
        """sending committed_at"""
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        committed_at = {"$gt": gt, "$lt": lt}
        create_scm_files_report = create_scm_coding_days_object.scm_coding_days_report(
            integration_id=get_integration_obj, committed_at=committed_at)

        assert create_scm_files_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of SCM files report with drilldown ====")
        key_value = create_scm_coding_days_object.scm_coding_days_report(integration_id=get_integration_obj,
                                                                         committed_at=committed_at, keys=True)
        for eachRecord in key_value:
            drilldown = create_scm_coding_days_object.scm_coding_days_report_drilldown(
                integration_id=get_integration_obj, committed_at=committed_at,
                key=eachRecord['key'])
            assert drilldown, "Drilldown not loaded"
