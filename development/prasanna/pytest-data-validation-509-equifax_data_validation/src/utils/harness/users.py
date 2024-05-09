import logging
import json
import pandas as pd
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class UserAccountsHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.connection = self.generic.get_connect_info()
        self.config_info = self.generic.get_api_info()
        self.role_mapping = self.config_info["rbac_role_identifier"]
        
    def reset_account_password(self, email, token, default="Test@1111"):
        try:
            payload = { "accountId": pytest.tenant_name,"token": token,
                "email": email, "password": default, "name": "pytest rbac user"}
            url = self.connection["base_url"] + self.config_info["ngsignin"]
            resp = self.generic.execute_api_call(url, "put", data=payload,
                            status_code_info=True, params={"generation":"NG", 
                                "accountId":pytest.tenant_name},
                            harness_default_params = False)
            assert resp.status_code == 200, "password reset failed--" + email
        except Exception as e:
            LOG.error(f" ===== reset_account_password: Error occurred. Error: {e}")
            return False
        return True
    
    def create_new_user_account(self, email, cleanup=True):
        try:
            if cleanup:
                invite_id = self.invited_users_list(email=email)
                if invite_id:
                    deletion_status = self.delete_invited_user(invite_id=invite_id)
            payload = {"emails": [email], "roleBindings": []}
            url = self.connection["base_url"] + self.config_info["users"]
            resp = self.generic.execute_api_call(url, "post", data=payload, status_code_info=True)
            assert resp.status_code == 200, "new account creation failed--" + email
        except Exception as e:
            LOG.error(f" ===== create_new_user_account: Error occurred. Error: {e}")
            return False
        return True

    def delete_invited_user(self, invite_id):
        try:
            url = self.connection["base_url"] + self.config_info["user_invites"] + f"/{invite_id}"
            resp = self.generic.execute_api_call(url, "delete", data={}, 
                            status_code_info=True, accountIdentifier=False)
            assert resp.status_code == 200, f"invite deletion failed-- {invite_id}"
        except Exception as e:
            LOG.error(f" ===== delete_invited_user: Error occurred. Error: {e}")
            return False
        return True

    def invited_users_list(self, email=None):
        try:
            url = self.connection["base_url"] + self.config_info["user_invites_list"]
            resp = self.generic.execute_api_call(url, "post", data={}, status_code_info=True)
            assert resp.status_code == 200, "unable to fetch invite users list"
            resp_data = json.loads(resp.text)
            resp_df = pd.DataFrame(resp_data["data"]["content"])
            emails = resp_df["email"].values.tolist()
            if email:
                if email in emails: 
                    return resp_df[resp_df["email"]==email]['id'][0]
                else: return []
            return emails
        except Exception as e:
            LOG.error(f" ===== invited_users_list: Error occurred. Error: {e}")
            return []

