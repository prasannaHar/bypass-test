import logging
import urllib3

from src.lib.widget_details.widget_helper import TestWidgetHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisHelper:
    def __init__(self, generic_helper):
        pass

    def filter_dev_prod_user(self, force_source, gt, lt, userid, profile_id):
        filters = {"user_id_type": "ou_user_ids", "user_id_list": userid,
                   "time_range": {"$gt": str(gt), "$lt": str(lt)},
                   "dev_productivity_profile_id": profile_id, "force_source": force_source}

        return filters


