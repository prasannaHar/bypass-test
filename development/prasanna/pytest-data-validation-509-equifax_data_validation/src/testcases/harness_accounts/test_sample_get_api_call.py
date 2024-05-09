import logging
import pytest
import json
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSampleTestCase:
    rbacusertype = ["ADMIN"]

    @pytest.mark.parametrize("rbacusertype", rbacusertype)
    def test_sample_user_groups_api(self, rbacusertype, create_generic_object):
        """Validate trellis score report functionality"""
        url = create_generic_object.connection["base_url"] + "ng/api/aggregate/acl/usergroups"
        payload={}
        resp = create_generic_object.execute_api_call(url, "get", data=payload,
                                         status_code_info=True, 
                                         params={"filterType":"INCLUDE_INHERITED_GROUPS"})
        assert resp.status_code == 200, "api call is failing"

