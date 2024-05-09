import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestScmCommitSingleStat:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_commit_single_stat_001(self, create_scm_commit_singleStat_object,
                                           create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm commits single stat """

        LOG.info("==== create widget with available filter ====")
        create_scm_commit_single_stat = create_scm_commit_singleStat_object.scm_commit_single_stat(
            integration_id=get_integration_obj)

        assert create_scm_commit_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_scm_commit_single_stat(),
                                                       create_scm_commit_single_stat,
                                                       "Schema validation failed for scm commits single stat  widget endpoint")

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_scm_commit_single_stat_002(self, create_scm_commit_singleStat_object,
                                                   create_widget_helper_object, widget_schema_validation, get_integration_obj):
        """Validate alignment of scm commits single stat """

        LOG.info(
            "==== create widget with available filter agg_type,code_change_size_config,code_change_size_unit,time_period====")
        create_scm_commit_single_stat = create_scm_commit_singleStat_object.scm_commit_single_stat(
            integration_id=get_integration_obj, agg_type="total", code_change_size_config={"small": "50", "medium": "150"},
            code_change_size_unit="files", time_period=7)

        assert create_scm_commit_single_stat, "widget is not created"
        LOG.info("==== Validate Widget Schema ====")
        create_widget_helper_object.schema_validations(widget_schema_validation.create_widget_scm_commit_single_stat(),
                                                       create_scm_commit_single_stat,
                                                       "Schema validation failed for scm commits single stat  widget endpoint")
