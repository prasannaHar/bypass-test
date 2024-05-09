import random

import pytest
import requests
import logging
import os
import urllib3
import json
import datetime
import pandas as pd
import time
import base64

from dateutil.relativedelta import relativedelta

from src.utils.generate_Api_payload import GenericPayload

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestGenericHelper:
    def __init__(self):
        pytest.standalone_app_flag = True
        self.headers = {"Content-Type": "application/json"}
        self.temp_project_id = "10182"
        self.connect_timeout = 30
        self.connection = self.get_connect_info()
        self.api_data = self.get_api_info()
        self.env = self.get_env_based_info()
        if self.get_env_based_info(key="harness_app"):
            pytest.standalone_app_flag = not self.get_env_based_info(key="harness_app")
        self.products_url = self.connection["base_url"] + "products/"
        self.user_url = self.connection["base_url"] + "users"
        self.genericpayload = GenericPayload()
        self.auth = self.get_auth_token()
        pytest.tenant_name = self.connection["tenant_name"]
        pytest.user_auth_info = {}

    def get_connect_info(self):
        """This function reads the env variable and returns the connection info"""
        configuration = dict()
        configuration["tenant_name"] = os.getenv('TENANT')
        configuration["env_name"] = os.getenv('ENV')
        configuration["username"] = os.getenv("USER")
        configuration["password"] = os.getenv("PASSWORD")
        configuration["base_url"] = os.getenv("BASE_URL")
        configuration["public_dashboard_username"] = os.getenv("PUBLIC_DASHBOARD_USER")
        configuration['public_dashboard_password'] = os.getenv("PUBLIC_DASHBOARD_PASSWORD")
        configuration["public_user_dps"] = os.getenv("PUBLIC_USER_DPS")
        configuration['public_pass_dps'] = os.getenv("PUBLIC_PASS_DPS")
        configuration["public_user_ou_manager"] = os.getenv("PUBLIC_USER_OU_MANAGER")
        configuration['public_pass_ou_manager'] = os.getenv("PUBLIC_PASS_OU_MANAGER")
        configuration["admin_user_dps"] = os.getenv("ADMIN_USER_DPS")
        configuration['admin_pass_dps'] = os.getenv("ADMIN_PASS_DPS")
        configuration["admin_user_ou_manager"] = os.getenv("ADMIN_USER_OU_MANAGER")
        configuration['admin_pass_ou_manager'] = os.getenv("ADMIN_PASS_OU_MANAGER")
        configuration["org_admin_user_trellis"] = os.getenv("ORG_ADMIN_TRELLIS")
        configuration['org_admin_pass_trellis'] = os.getenv("ORG_ADMIN_PASS_TRELLIS")
        configuration["org_admin_user"] = os.getenv("ORG_ADMIN")
        configuration['org_admin_pass'] = os.getenv("ORG_ADMIN_PASS")
        configuration["admin_user_trellis_ous"] = os.getenv("ADMIN_TRELLIS_RES")
        configuration['admin_pass_trellis_ous'] = os.getenv("ADMIN_PASS_TRELLIS_RES")
        return configuration

    def get_api_info(self, apis_file=None):
        """This function returns the payload info for selected functional area"""
        apis_file = "config_apis.json"
        if not apis_file:
            apis_file = os.getenv("APIS_FILE")
        with open(os.path.join("config", apis_file)) as fp:
            return json.load(fp)

    def get_env_based_info(self, env_file=None, key=None):
        """This function returns the payload info for selected functional area"""
        if not env_file:
            env_file = os.getenv("ENV_FILE")
        config_data = {}
        with open(os.path.join("config", env_file)) as fp:
            config_data = json.load(fp)
        if key:
            if key in config_data.keys(): return config_data[key]
            return False
        return config_data

    def get_env_var_info(self, env_var):
        """"This function returns the value of environment variable."""
        env_var_value = os.getenv(env_var)
        return env_var_value

    def retrieve_user_email(self, user_type="ADMIN"):
        "this function returns the user email id"
        username_field, password_field = self.api_data["propelo_users_mapping"][user_type]
        username, password = self.connection[username_field], self.connection[password_field]
        return username

    def get_auth_token(self, user_type="ADMIN"):
        """This function logs into the DNAC API interface and returns the auth token"""
        global username, password
        base_url = self.connection["base_url"] + "authenticate"
        username_field, password_field = self.api_data["propelo_users_mapping"][user_type]
        username, password = self.connection[username_field], self.connection[password_field]
        if (not username) or (not password):
            username_field, password_field = self.api_data["propelo_users_mapping"]["ADMIN"]
            username, password = self.connection[username_field], self.connection[password_field]
        data = json.dumps({"username": username, "password": password,
                           "company": self.connection["tenant_name"]})
        if not pytest.standalone_app_flag:
            ## authentication api
            base_url = self.connection["base_url"] + self.api_data["harness_users_login"]
            ## encoding the credentials
            creds = username + ":" + password
            encoded_bytes = base64.b64encode(creds.encode("utf-8"))
            creds_encoded = encoded_bytes.decode("utf-8")
            ## payload generation
            data = json.dumps({"authorization": "Basic "+creds_encoded})
        response_data = requests.post(base_url, data=data, headers=self.headers)
        if response_data.status_code == 403:
            LOG.error(
                "Failed to with API call with error message: {0}, Error: {1}".format(
                    response_data.status_code, response_data.text
                )
            )
            return response_data

        if response_data.status_code != 200:
            LOG.error(
                "Failed to get authentication token. Code: {0}, Error: {1}".format(
                    response_data.status_code, response_data.text
                )
            )
            return None

        resp_data = json.loads(response_data.text)

        if "token" in resp_data:
            LOG.info(
                "Successfully connected to {0} and authentication token is returned".format(
                    base_url
                )
            )
            return "Bearer" + " " + resp_data["token"]
        
        elif "resource" in resp_data:
            LOG.info(
                "Successfully connected to {0} and authentication token is returned".format(
                    base_url
                )
            )
            pytest.user_auth_info = resp_data
            return "Bearer" + " " + resp_data["resource"]["token"]

        else:
            LOG.error(
                "Token not found in response data. Response:\n{0}".format(
                    json.dumps(resp_data, indent=4)
                )
            )
            return None

    def execute_api_call(
            self,
            url,
            request_type,
            create_auth_token=None,
            params=None,
            data=None,
            use_cookie=False,
            timeout=120,
            status_code_info=None,
            harness_default_params = True,
            accountIdentifier = True
    ):
        """This is a generic function to execute any api given the url, type and the params"""

        if not create_auth_token:
            create_auth_token = self.auth

        if use_cookie:
            self.headers["Cookie"] = "Authorization= Bearer {0}".format(create_auth_token)
            if "Authorization" in self.headers.keys():
                del self.headers["Authorization"]
        else:
            self.headers["Authorization"] = create_auth_token
            if "Cookie" in self.headers.keys():
                del self.headers["Cookie"]

        if create_auth_token:
            self.headers["Authorization"] = create_auth_token

        if not pytest.standalone_app_flag:
            sei_api_identififers = self.api_data["sei_api_identififers"]
            sei_api_version = self.api_data["sei_api_version"]
            if any(item in url for item in sei_api_identififers):
                url_split = url.split("gateway/")
                url = f"{url_split[0]}gateway/{sei_api_version}{url_split[1]}" 
            if params == None: params = {}
            if harness_default_params:
                params["routingId"] = pytest.tenant_name
                if accountIdentifier:
                    params["accountIdentifier"] = pytest.tenant_name

        if request_type == "get":
            LOG.info("Executing GET operation on {0}".format(url))
            if params:
                response_data = requests.get(
                    url, headers=self.headers, params=params, timeout=None
                )
            else:
                response_data = requests.get(
                    url, headers=self.headers, timeout=timeout
                )

        elif request_type == "put":
            LOG.info("Executing PUT operation on {0}".format(url))
            response_data = requests.put(
                url,
                headers=self.headers,
                params=params,
                data=json.dumps(data),
                timeout=timeout,
            )
        elif request_type == "patch":
            LOG.info("Executing Patch operation on {0}".format(url))
            response_data = requests.patch(
                url,
                headers=self.headers,
                params=params,
                data=json.dumps(data),
                timeout=timeout,
            )

        elif request_type == "delete":
            LOG.info("Executing DELETE operation on {0}".format(url))
            response_data = requests.delete(
                url, headers=self.headers, params=params, data=json.dumps(data), timeout=timeout
            )

        else:
            LOG.info("Executing POST operation on {0}".format(url))
            response_data = requests.post(
                url,
                headers=self.headers,
                params=params,
                data=json.dumps(data)
            )

        if response_data.status_code == 401 and "token expired" in response_data.text:
            LOG.warning(
                "Error code : {0}, Error message : {1}, Trying to refresh the token".format(
                    response_data.status_code, response_data.text
                )
            )
            self.create_auth_token = self.get_auth_token()
            return self.execute_api_call(
                url,
                request_type,
                params=params,
                data=data,
                use_cookie=use_cookie,
                timeout=timeout,
            )
        elif response_data.status_code in [400, 403, 404]:
            LOG.info(
                "Permission issue: {} has response code and message: {} ".format(
                    response_data.status_code, response_data.text
                )
            )
            return response_data

        elif response_data.status_code == 201:
            LOG.error(
                "Success to execute api.. returns Error code : {0}, Error message : {1}".format(
                    response_data.status_code, response_data.text
                )
            )
            return json.loads(response_data.text)
        elif response_data.status_code in [500, 501, 502]:
            LOG.warning(
                "warning to execute api.. returns Error code : {0}, Error message : {1}".format(
                    response_data.status_code, response_data.text
                )
            )
            return response_data
        else:
            LOG.info(
                "Success code: {0}, API Call executed successfully".format(
                    response_data.status_code
                )
            )
        if (request_type == "delete") and (status_code_info):
            return response_data
        elif request_type == "delete":
            return response_data.text
        elif request_type == "put":
            return response_data
        elif status_code_info:
            return response_data
        elif response_data.text:
            return json.loads(response_data.text)
        else:
            return response_data.status_code

    def get_integration_id(self, intergration_name="jira"):
        """get the integration id from the intergration name"""
        base_id = []
        baseurl = self.connection["base_url"] + "integrations/list"
        payload = {"page": 0, "page_size": 10, "filter": {"partial": {"name": intergration_name, "status": "ACTIVE"}}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        if len(response["records"]):
            for name_id in response["records"]:
                if name_id["status"] == "ACTIVE":
                    status_url = self.connection["base_url"] + "ingestion/" + name_id["id"] + "/status"
                    status_resp = self.execute_api_call(status_url, "post", data={})
                    if status_resp["status"] == "healthy":
                        base_id.append(name_id["id"])
        else:
            LOG.info("intergration name is not present")
            return None
        return base_id

    def get_integration_list(self, application_name):
        base_id = []
        baseurl = self.connection["base_url"] + "integrations/list"
        payload = {"filter": {"applications": [application_name]}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        if len(response["records"]):
            for i in range(0, len(response["records"])):
                if response['records'][i]['status'] == "ACTIVE":
                    base_id.append(response['records'][i]["id"])

        else:
            raise Exception("No integration id found or no active integrations ")

        return base_id

    def get_org_unit_id(self, org_unit_name="Initech"):
        org_id = []
        baseurl = self.connection["base_url"] + "org/units/list"
        payload = {"page": 0, "page_size": 50, "filter": {"partial": {"name": org_unit_name}}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        if len(response["records"]):
            for org_ids in response["records"]:
                if org_ids["name"] == org_unit_name:
                    org_id.append(org_ids["id"])
        else:
            LOG.info("org units name is not present")
            return None
        return org_id

    def get_user_detail(self):
        baseurl = self.connection["base_url"] + "users/me/details"
        get_user_detail_response = self.execute_api_call(baseurl, "get")

        return get_user_detail_response

    def create_user(self, arg_user_type, password_enabled=True, sso_enabled=False, mfa_enabled=False,
                    login_user_type="ADMIN"):
        baseurl = self.connection["base_url"] + "users"
        create_user_payload = self.genericpayload.generate_create_user_payload(arg_user_type, password_enabled,
                                                                               sso_enabled,
                                                                               mfa_enabled, )
        create_new_user_response = self.rbac_user(
            baseurl, "post",
            data=create_user_payload,
            user_type=login_user_type
        )
        return create_new_user_response

    def delete_user(self, arg_user_id):
        self.execute_api_call(self.user_url + "/" + arg_user_id, "delete")
        LOG.info("delete the user : {} ".format(arg_user_id))

    def update_user(self, arg_user_id, arg_user_type, arg_first_name, arg_last_name, arg_email):
        create_user_payload = self.genericpayload.generate_update_user_payload(arg_user_type=arg_user_type,
                                                                               arg_email=arg_email,
                                                                               arg_first_name=arg_first_name,
                                                                               arg_last_name=arg_last_name)

        create_update_user_response = self.execute_api_call(self.user_url + "/" + str(arg_user_id), "put",
                                                            data=create_user_payload,
                                                            )
        return json.loads(create_update_user_response.text)

    def get_user_detail_from_id(self, arg_user_id):
        get_user_detail_response = self.execute_api_call(self.user_url + "/" + str(arg_user_id),
                                                         "get")
        return get_user_detail_response

    def get_filter_options(self, arg_filter_type, arg_integration_ids, req_additional_keys=None):
        get_filters_url = ""
        jira_issues = ["issue_type", "label", "priority", "assignee", "status", "status_category", "fix_version",
                       "component",
                       "reporter", "resolution", "version"]

        # if arg_filter_type[0] == "project_name":
        if arg_filter_type[0] == "project_name":
            get_filters_url = self.connection["base_url"] + "jiraprojects/values"
        elif arg_filter_type[0] in jira_issues:
            get_filters_url = self.connection["base_url"] + "jira_issues/values"
        elif arg_filter_type[0] == "job_name":
            get_filters_url = self.connection["base_url"] + "cicd/job_runs/values"
        elif "customfield" in arg_filter_type[0]:
            get_filters_url = self.connection["base_url"] + "jira_issues/custom_field/values"

        get_filter_options_payload = self.genericpayload.generate_filter_options_payload(
            arg_filter_type=arg_filter_type,
            arg_integration_ids=arg_integration_ids)
        LOG.info(json.dumps(get_filter_options_payload))
        get_filter_options_response = self.execute_api_call(get_filters_url, "post",
                                                            data=get_filter_options_payload)
        if req_additional_keys:
            filter_values_dict = get_filter_options_response['records'][0][arg_filter_type[0]]
            response_df = pd.json_normalize(filter_values_dict)
            response_df = response_df[response_df["additional_key"].isin(req_additional_keys)]
            return response_df['key'].tolist()

        return get_filter_options_response

    def get_filter_options_ado(self, arg_filter_type, arg_integration_ids):
        get_filters_url = ""
        # breakpoint()
        workitem = self.api_data["constant_filters_ado"]
        attribute = ["teams", "code_area"]
        set2 = set(attribute)
        workitem = [elem for elem in workitem if elem not in set2]
        if arg_filter_type in attribute:
            get_filters_url = self.connection["base_url"] + "issue_mgmt/workitems/attribute/values"
        elif arg_filter_type not in attribute and arg_filter_type in workitem:
            get_filters_url = self.connection["base_url"] + "issue_mgmt/workitems/values"
        # elif arg_filter_type == "job_name":
        #     get_filters_url = self.connection["base_url"] + "cicd/job_runs/values"
        if "Microsoft.VSTS" in arg_filter_type:
            get_filters_url = self.connection["base_url"] + "issue_mgmt/workitems/custom_field/values"
            arg_filter_type = [arg_filter_type.split("-")[0]]
        LOG.info("filter_type im get filter options ado 2------{}".format(arg_filter_type))
        get_filter_options_payload = self.genericpayload.generate_filter_options_payload(
            arg_filter_type=[arg_filter_type],
            arg_integration_ids=arg_integration_ids)
        get_filter_options_response = self.execute_api_call(get_filters_url, "post",
                                                            data=get_filter_options_payload)
        return get_filter_options_response

    def get_scm_filter_options(self, arg_filter_type, arg_integration_ids):
        get_filters_url = ""
        scm_filter = ["assignee", "repo_id", "state", "project"]
        if arg_filter_type[0] in scm_filter:
            get_filters_url = self.connection["base_url"] + "scm/prs/values"

        get_filter_options_payload = self.genericpayload.generate_filter_options_payload(
            arg_filter_type=arg_filter_type,
            arg_integration_ids=arg_integration_ids)
        LOG.info("payload-----{}".format(json.dumps(get_filter_options_payload)))
        get_filter_options_response = self.execute_api_call(get_filters_url, "post",
                                                            data=get_filter_options_payload
                                                            )
        return get_filter_options_response

    def get_epoc_time(self, value, type="months", calc_curr_day=True, value_and_type=None):
        ## overriding the value and type values in cases of value_and_type not none case
        if value_and_type:
            value = value_and_type[0]
            type = value_and_type[1]
        if calc_curr_day:
            lt = datetime.datetime.combine(datetime.datetime.today(), datetime.time.max)
        else:
            lt = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(days=1), datetime.time.max)
        lt_epoc = int(lt.timestamp() + 19800)  # converting to GMT

        if type == "months":
            gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=value), datetime.time.min)
        elif type == "days":
            gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(days=value), datetime.time.min)

        gt_epoc = int(gt.timestamp() + 19800)  # converting to GMT
        return str(gt_epoc), str(lt_epoc)

    def get_epoc_utc(self, value=None, type="months", lt_time_delta=0, value_and_type=None, timerange_factor=1):
        ## overriding the value and type values in cases of value_and_type not none case
        if value_and_type:
            value = value_and_type[0] * timerange_factor
            value = int(value)
            type = value_and_type[1]
        # Calculate end date as now().
        if lt_time_delta == 0:
            lt = datetime.datetime.now(datetime.timezone.utc)
        # Calculate end date as now() - 1d.
        else:
            lt = datetime.datetime.combine(
                datetime.date.today() - datetime.timedelta(days=lt_time_delta), datetime.datetime.max.time()
            )
        lt_epoc = int(lt.timestamp())

        if type == "months":
            gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(months=value), datetime.time.min)

        elif type == "days":
            gt = datetime.datetime.combine(datetime.datetime.today() - relativedelta(days=value), datetime.time.min)

        gt_epoc = int(gt.timestamp())
        return str(gt_epoc), str(lt_epoc)

    def integration_ids_basedon_workspace(self, application=None):
        integration_ids = []
        try:
            baseurl = self.connection["base_url"] + "org/workspaces/" + self.env["workspace_id"]
            response = self.execute_api_call(baseurl, "get", data={})
            if application:
                integrations = pd.json_normalize(response['integrations'], max_level=1)
                application_integrations = integrations[integrations['application'] == application]
                integration_ids = application_integrations["id"].values.tolist()
                return integration_ids
            integration_ids = list(map(str, response["integration_ids"]))

        except Exception as ex:
            LOG.info("failure due to {}".format(ex))
            # self.integration_ids_basedon_workspace()
        return integration_ids

    def get_category_ou_id(self, workspace_id):
        category = []
        baseurl = self.connection["base_url"] + "org/groups/list"
        payload = {"filter": {"workspace_id": [workspace_id]}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        category = [cat_id['id'] for cat_id in response["records"]]
        return category

    def get_filter_options_scm(self, arg_filter_type, report_type=None, integrationID=None, rev_filter_type=None,
                               req_additional_keys=None):
        if arg_filter_type in ["code_change_sizes", "comment_densities", "has_issue_keys"]:
            filter_vals = self.api_data["scm_prs_fixed_filer_vals"]
            return filter_vals[arg_filter_type]
        get_filters_url = ""
        if report_type:
            get_filters_url = self.connection["base_url"] + "scm/" + report_type + "/values"
        else:
            get_filters_url = self.connection["base_url"] + "scm/prs/values"
        if integrationID is None:
            integrationID = self.integration_ids_basedon_workspace()
        if rev_filter_type:
            filter_maping = self.api_data["scm_exclusions_vs_across_mapping"]
            temp_filter_maping = dict([(v, [k for k, v1 in filter_maping.items() if v1 == v])
                                       for v in set(filter_maping.values())])
            arg_filter_type = temp_filter_maping[arg_filter_type][0]

        get_filter_options_payload = self.genericpayload.generate_filter_options_payload(
            arg_filter_type=[arg_filter_type],
            arg_integration_ids=integrationID)
        get_filter_options_response = self.execute_api_call(get_filters_url, "post",
                                                            data=get_filter_options_payload)
        filter_values_dict = get_filter_options_response['records'][0][arg_filter_type]
        if req_additional_keys:
            response_df = pd.json_normalize(filter_values_dict)
            response_df = response_df[response_df["additional_key"].isin(req_additional_keys)]
            return response_df['key'].tolist()

        filter_values_dict_sorted = sorted(filter_values_dict, key=lambda i: i['count'])
        filter_values_dict_processed = list(filter(lambda person: "key" in person.keys(), filter_values_dict_sorted))
        filter_values = [x['key'] for x in filter_values_dict_processed]
        filter_values.reverse()
        return filter_values[:10]

    def get_ado_filter_options(self, arg_filter_type, req_additional_keys=None, ou_id=None,
                               rev_filter_type=None, integrationID=None, total_tickets=None):
        """
        Args:
            arg_filter_type : required filter name
            req_additional_keys : 
                accepts the list of additional_key values & 
                returns the key values associated to accepted the additional_keys
            ou_id (str, optional): _description_. Defaults to None.
            rev_filter_type (_type_, optional): _description_. Defaults to None.
        """
        ado_im_filter_max_values = self.api_data["ado_im_filter_max_values"]
        tenant_specific_keys = {"project":1,"other":15}
        if pytest.tenant_name in ado_im_filter_max_values.keys():
            tenant_specific_keys = ado_im_filter_max_values[pytest.tenant_name]
        if arg_filter_type in ["workitem_story_points", "workitem_sprint_states", "workitem_hygiene_types"]:
            filter_vals = self.api_data["ado_im_fixed_filter_vals"]
            return filter_vals[arg_filter_type]
        endpoint = self.api_data["ado_im_values"]
        if arg_filter_type in ["teams", "code_area"]:
            endpoint = self.api_data["ado_im_workitem_attribute_values"]
        if (arg_filter_type.startswith("Custom")) or (arg_filter_type.startswith("Microsoft")):
            endpoint = self.api_data["ado_im_custom_field_values"]
        get_filters_url = self.connection["base_url"] + endpoint
        if ou_id is None:
            integration_id = self.integration_ids_basedon_workspace()
        else:
            integration_id = self.get_integrations_based_on_ou_id(ou_id, application_name="azure_devops")
        if integrationID:
            integration_id = integrationID
        if rev_filter_type:
            filter_maping = self.api_data["ado_im_filter_names_mapping"]
            temp_filter_maping = dict([(v, [k for k, v1 in filter_maping.items() if v1 == v])
                                       for v in set(filter_maping.values())])
            arg_filter_type = temp_filter_maping[arg_filter_type][0]
        get_filter_options_payload = self.genericpayload.generate_filter_options_payload(
            arg_filter_type=[arg_filter_type],
            arg_integration_ids=integration_id,
            integrations_inside_only=True)
        get_filter_options_response = self.execute_api_call(get_filters_url, "post",
                                                            data=get_filter_options_payload)
        filter_values_dict = get_filter_options_response['records'][0][arg_filter_type]
        filter_values_dict = filter_values_dict['records']
        response_df = pd.json_normalize(filter_values_dict)
        if req_additional_keys:
            response_df = response_df[response_df["additional_key"].isin(req_additional_keys)]
            return response_df['key'].tolist()
        if total_tickets:
            return response_df["total_tickets"].sum()

        filter_values_dict_sorted = sorted(filter_values_dict, key=lambda i: i['total_tickets'])
        filter_values_dict_processed = list(filter(lambda person: "key" in person.keys(), filter_values_dict_sorted))
        filter_values = [x['key'] for x in filter_values_dict_processed]
        filter_values.reverse()

        if arg_filter_type in tenant_specific_keys:
            return filter_values[:tenant_specific_keys[arg_filter_type]]
        else:
            return filter_values[:tenant_specific_keys["other"]]


    def get_aggregration_fields(self, only_custom=False, ou_id=None, application_name=None):
        """getting the Aggregations values of x axis"""
        arrg_total = self.api_data["constant_aggregations"]
        # breakpoint()
        if ou_id is None:
            integration_id = self.integration_ids_basedon_workspace()
        else:
            integration_id = self.get_integrations_based_on_ou_id(
                ou_id, application_name=application_name)
        baseurl = self.connection["base_url"] + self.api_data["integration_configs_list"]
        payload = {"filter": {"integration_ids": integration_id}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        for arrg_value in response["records"]:
            if 'agg_custom_fields' in (arrg_value['config']).keys():
                if arrg_value['config']['agg_custom_fields']:
                    for values_agg in arrg_value['config']['agg_custom_fields']:
                        if "Microsoft" in values_agg['key']:
                            continue
                        else:
                            arrg_total.append("custom_field" + '-' + values_agg['key'] + '-' + values_agg['name'])
        if only_custom:
            arrg_total_custom = list(filter(lambda x: x.startswith("custom_field"), arrg_total))
            return set(arrg_total_custom)
        return set(arrg_total)

    def get_integration_custom_details_ado(self, ou_id=None, req_details="fields"):
        if ou_id is None:
            integration_id = self.integration_ids_basedon_workspace(ou_id)
        else:
            integration_id = self.get_integrations_based_on_ou_id(
                ou_id=ou_id, application_name="azure_devops")
        baseurl = self.connection["base_url"] + self.api_data["integration_configs_list"]
        payload = {"filter": {"integration_ids": integration_id}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        response = response["records"][0]
        if req_details == "fields":
            custom_field_details_df = pd.json_normalize(response["config"]["agg_custom_fields"])
            return custom_field_details_df['key'].values.tolist()
        elif req_details == "hygienes":
            custom_hygiene_details_df = pd.json_normalize(response["custom_hygienes"])
            return custom_hygiene_details_df["name"].values.tolist()
        return []

    def get_category(self, workspaceID, return_type="id"):
        category = []
        baseurl = self.connection["base_url"] + self.api_data["ou_categories"]
        payload = {"filter": {"workspace_id": [workspaceID]}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        for cat_id in response["records"]:
            if return_type == "response":
                return response
            else:
                if return_type == "id":
                    category.append(cat_id['id'])
                elif return_type == "name":
                    category.append(cat_id['name'])
                elif return_type == "root_ou_id":
                    category.append(cat_id['root_ou_id'])

        return category

    def rbac_user(self, url, request_type, data=None,
                  user_type="PUBLIC_DASHBOARD", status_code_info=None):

        create_auth_token = self.get_auth_token(user_type=user_type)
        if request_type == "get":
            resp = self.execute_api_call(url, create_auth_token=create_auth_token,
                                         request_type=request_type, status_code_info=status_code_info)
        else:
            resp = self.execute_api_call(url, create_auth_token=create_auth_token,
                                         request_type=request_type, data=data,
                                         status_code_info=status_code_info)
        return resp

    def get_application_type_with_workspace_id(self, workspace_id):
        # with help of this we will return integrationids tied to the workspace with the integration_type
        application_list = [{"azure_boards": []}, {"jira": []},
                            {"scm": []}, {"pipelines": []}]
        try:
            baseurl = self.connection["base_url"] + "org/workspaces/" + str(workspace_id)
            resp = self.execute_api_call(baseurl, 'get', data={})
            for i in range(0, len(resp['integrations'])):
                if resp['integrations'][i]['application'] == "azure_devops":
                    if "Repos" in str(resp['integrations'][i]['name']):
                        application_list[2]['scm'].append(resp['integrations'][i]['id'])

                    if "Boards" in str(resp['integrations'][i]['name']):
                        application_list[0]['azure_boards'].append(resp['integrations'][i]['id'])

                    if "Pipelines" in str(['integrations'][i]['name']):
                        application_list[3]['pipelines'].append(resp['integrations'][i]['id'])

                if resp['integrations'][i]['application'] == "jira":
                    application_list[1]['jira'].append(resp['integrations'][i]['id'])

                if resp['integrations'][i]['application'] in ["github", "gitlab", "bitbucket"]:
                    application_list[2]['scm'].append(resp['integrations'][i]['id'])

        except Exception as ex:
            LOG.info("failure due to {}".format(ex))
        return application_list

    def get_ado_custom_field(self, integration_id, only_custom=False):
        arrg_total = self.api_data["constant_aggregations_ado"]
        baseurl = self.connection["base_url"] + self.api_data["integration_configs_list"]
        payload = {"filter": {"integration_ids": integration_id}}
        response = self.execute_api_call(baseurl, "post", data=payload)
        for arrg_value in response["records"]:
            if 'agg_custom_fields' in (arrg_value['config']).keys():
                if arrg_value['config']['agg_custom_fields']:
                    for values_agg in arrg_value['config']['agg_custom_fields']:
                        arrg_total.append(values_agg['key'])
        if only_custom:
            arrg_total_custom = list(filter(lambda x: x.startswith("Microsoft"), arrg_total))
            return set(arrg_total_custom)
        return set(arrg_total)

    def get_integrations_based_on_ou_id(self, ou_id, get_name=None, application_name=None):
        integration_ids = []
        try:
            org_url = self.connection["base_url"] + "org/units/" + ou_id
            resp = self.execute_api_call(org_url, 'get', data={})
            if get_name: return resp["name"]
            if len(resp["sections"]) == 0:
                baseurl = self.connection["base_url"] + "org/workspaces/" + str(resp["workspace_id"])
                response = self.execute_api_call(baseurl, "get", data={})
                integration_ids_1 = list(map(str, response["integration_ids"]))
                integration_ids = integration_ids_1
            if len(resp["sections"]) != 0:
                for i in range(0, len(resp["sections"])):
                    integration_id = resp["sections"][i]['integrations']
                    integration_id_key = [*integration_id]
                    if application_name != None:
                        if (integration_id[integration_id_key[0]]['type'] == application_name):
                            integration_ids.append(integration_id_key[0])
                    else:
                        integration_ids.append(integration_id_key[0])

        except Exception as ex:
            LOG.info("failure due to {}".format(ex))
        return integration_ids

    def get_jira_field_based_on_filter_type(self, filter_type):

        if "customfield" in filter_type:
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]

        else:
            filter_type_list = ["statuses", "priorities", "projects", "status_categories", "issue_types",
                                "fix_versions", "labels", "reporters", "assignees", "resolutions", "components"]
            if filter_type in filter_type_list:
                filter_option = filter_type
            else:
                filter_option = filter_type[:-1]

        return filter_option

    def max_3_values_filter_type(self, filter_type, int_ids, exclude=None):
        # df_sprint = ou_list
        req_filter_names_and_value_pair = ""
        if "customfield" in filter_type:
            # breakpoint()
            filter_type = filter_type.split("-")
            filter_option = filter_type[1]
        else:
            if filter_type == "statuses":
                filter_option = "status"
            elif filter_type == "priorities":
                filter_option = "priority"
            elif filter_type == "projects":
                filter_option = "project_name"
            elif filter_type == "status_categories":
                filter_option = "status_category"
            else:
                filter_option = filter_type[:-1]
        # breakpoint()
        get_filter_response = self.get_filter_options(arg_filter_type=[filter_option],
                                                      arg_integration_ids=int_ids)

        if len(get_filter_response['records'][0][filter_option]) == 0:
            if exclude is not None:
                LOG.info("no value present for the exclude field")
                return req_filter_names_and_value_pair
            else:
                pytest.skip("Filter Doesnot have Any values")

        if len(get_filter_response['records'][0][filter_option]) != 0:
            all_filter_records = [get_filter_response['records'][0][filter_option]]
            value = []

            ran_value = random.sample(all_filter_records[0], min(3, len(all_filter_records[0])))
            for eachissueType in ran_value:
                if filter_option == "assignee":
                    if eachissueType['additional_key'] != "_UNASSIGNED_":
                        value.append(eachissueType['key'])
                else:
                    value.append(eachissueType['key'])

            try:
                if "customfield" in filter_option:
                    required_filters_needs_tobe_applied = ["custom_fields"]
                    filter_value = [{filter_option: value}]
                else:
                    required_filters_needs_tobe_applied = [filter_type]
                    filter_value = [value]

                req_filter_names_and_value_pair = []
                for (eachfilter, eachvalue) in zip(required_filters_needs_tobe_applied, filter_value):
                    req_filter_names_and_value_pair.append([eachfilter, eachvalue])

            except Exception as ex:
                LOG.info("Exception max_3_values_filter_type function----{}".format(ex))

        return req_filter_names_and_value_pair

    def aggs_get_custom_value_with_value(self, int_ids):
        custom_fields = self.get_aggregration_fields(only_custom=True)
        list_custom_fields = []
        custom_fields_with_value = {}
        for i in custom_fields:
            string_list = i.split("-")
            list_custom_fields.append(string_list[1])

        url = self.connection['base_url'] + "jira_issues/custom_field/values"
        payload = {"fields": list_custom_fields, "integration_ids": int_ids, "filter": {"integration_ids": int_ids}}
        resp = self.execute_api_call(url, "post", data=payload)
        # breakpoint()
        for i in list_custom_fields:
            for j in range(0, len(resp['records'])):
                # breakpoint()
                if i in list(resp['records'][j].keys()):
                    custom_value = resp['records'][j][i]
                    if len(custom_value) != 0:
                        values = [d['key'] for d in custom_value]
                        # pick 2 random values
                        if len(values) == 1:
                            values_2 = random.sample(values, 1)
                        else:
                            values_2 = random.sample(values, 2)
                        custom_fields_with_value.update({i: values_2})

        return custom_fields_with_value
