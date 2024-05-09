import pytest
import logging

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestApiValidation:
    def test_login(self, create_generic_object):
        login_response = create_generic_object.get_auth_token()
        assert len(login_response) > 0, "Failed to Login : " + login_response
