import logging
from src.utils.harness.resource_groups import ResourceGroupHelper
from src.utils.harness.user_groups import UserGroupHelper
from src.utils.harness.users import UserAccountsHelper
from lib.mailinator_generic_helper.mailinator_helper import MailinatorHelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

class HarnessRBACPrerequisiteSetup:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.config_info = self.generic.get_api_info()
        self.rbac_config = self.config_info["harness_rbac_setup_info"]
        self.rg_helper = ResourceGroupHelper(generic_helper)
        self.ug_helper = UserGroupHelper(generic_helper)
        self.u_helper = UserAccountsHelper(generic_helper)
        self.mailinator_domain = self.generic.get_env_var_info("MAILINATOR_DOMAIN")
        self.mailinator_helper = MailinatorHelper(generic_helper)

    def resource_groups_creation(self):
        resource_groups = self.rbac_config["rg"]
        req_pattern = self.config_info["resource_group_name_pattern"]
        try:
            for resource_group in resource_groups:
                ## resource groups creation
                rg_status = self.rg_helper.create_resource_group(
                                    resource_group_name=req_pattern+resource_group)
                if not rg_status:
                    LOG.error("resource group creation failed {}".format(req_pattern+resource_group))
                else:
                    ## update the resource group with required permissions
                    rg_update_status = self.rg_helper.update_resource_group(
                                        resource_group_name=req_pattern+resource_group,
                                        insights=resource_groups[resource_group][0],
                                        collections=resource_groups[resource_group][1])
                    if not rg_update_status:
                        LOG.error("resource group updation failed {}".format(req_pattern+resource_group))
                    
        except Exception as e:
            LOG.error(f" ===== resource_groups_creation: Error occurred. Error: {e}")
            return False
        return True

    def user_groups_creation(self):
        user_groups = self.rbac_config["ug"]
        req_pattern = self.config_info["user_group_name_pattern"]
        try:
            for user_group in user_groups:
                ## resource groups creation
                ug_status = self.ug_helper.create_user_group(
                                    user_group_name=req_pattern+user_group)
                if not ug_status:
                    LOG.error("user_groups_creation: user group creation failed {}".format(req_pattern+user_group))
                else:
                    ## update the resource group with required permissions
                    ug_update_status = self.ug_helper.update_user_group(
                                        user_group_name=req_pattern+user_group,
                                        role_and_rg_mapping=user_groups[user_group])
                    if not ug_update_status:
                        LOG.error("user_groups_creation: user group updation failed {}".format(req_pattern+user_group))
                    
        except Exception as e:
            LOG.error(f" ===== user_groups_creation: Error occurred. Error: {e}")
            return False
        return True


    def user_creation(self):
        users = self.rbac_config["u"]
        req_pattern = self.config_info["user_email_name_pattern"]
        try:
            for user in users:
                email = f"{req_pattern}{user}@{self.mailinator_domain}"
                u_status = self.u_helper.create_new_user_account(email=email)
                if not u_status:
                    LOG.error("user_creation: user creation failed {}".format(req_pattern+user))
                else:
                    ## retrieve reset password token
                    reset_password_token = self.mailinator_helper.retrieve_mailinator_password_reset_token(
                                                        email=f"{req_pattern}{user}")
                    ## set the password
                    reset_status = self.u_helper.reset_account_password(email=email, token=reset_password_token)
                    breakpoint()
                    if not reset_status:
                        LOG.error("user_creation: password reset failed {}".format(email))                    
        except Exception as e:
            LOG.error(f" ===== user_groups_creation: Error occurred. Error: {e}")
            return False
        return True

