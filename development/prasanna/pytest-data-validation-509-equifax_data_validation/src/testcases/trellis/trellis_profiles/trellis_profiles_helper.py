import logging
from src.lib.widget_details.widget_helper import TestWidgetHelper


LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestTrellisProfileHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.widget = TestWidgetHelper(generic_helper)

    def retrieve_trellis_profiles(self, profile_name=None):
        profiles_response = self.widget.trellis_profiles_retriever()
        profiles = {eachprofile["name"]:eachprofile["id"] for eachprofile in profiles_response["records"]}
        if profile_name:
            return profiles[profile_name]
        return profiles
