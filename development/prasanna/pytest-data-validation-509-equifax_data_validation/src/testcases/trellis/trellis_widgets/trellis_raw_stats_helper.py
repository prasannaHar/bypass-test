import logging

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class TrellisRawStatsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()

    def retreive_metric_raw_stats(self, raw_stats, req_metric):
        result = [d for d in raw_stats if d.get('name') == req_metric]
        if len(result) > 0:
            metric_stats = result[0]
            if "count" in metric_stats.keys():
                return metric_stats["count"]
        return 0
