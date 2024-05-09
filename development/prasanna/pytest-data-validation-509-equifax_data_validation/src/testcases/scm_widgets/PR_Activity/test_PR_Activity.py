import logging
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestPrActivity:
    @pytest.mark.run(order=1)
    @pytest.mark.need_maintenance
    def test_pr_activity_001(self, create_pr_activity_object, create_generic_object):
        """Validate alignment of scm commits single stat """

        LOG.info("==== create widget with available filter ====")
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        time_range = {"$gt": gt, "$lt": lt}
        pr_activity = create_pr_activity_object.pr_activity(time_range=time_range)

        assert pr_activity, "widget is not created"
