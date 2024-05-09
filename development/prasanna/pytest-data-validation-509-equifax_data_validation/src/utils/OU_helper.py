import logging
import os
import pandas as pd
from pandas.io.json import json_normalize

from src.utils.generate_ou_payloads import *
# from src.utils.generate_Api_payload import GenericPayload
from src.utils.widget_reusable_functions import WidgetReusable

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class Ouhelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.baseurl = self.generic.connection["base_url"]
        self.jira_integration_ids = os.getenv('jira_integration_ids')
        self.jira_issues_table_name = os.getenv('jira_issues_table_name')
        self.login_url = self.baseurl + "authenticate"
        self.create_ou_url = self.baseurl + "org/units"
        self.get_OU_manager_url = self.baseurl + "org/users/values"
        self.delete_OU_url = self.baseurl + "org/units"
        self.reusable = WidgetReusable(generic_helper)
        self.ou_group_list = self.generic.connection["base_url"] + "org/groups/list"
        self.api_info = self.generic.get_api_info()

    def create_basic_integration_OU(self, arg_req_ou_name, arg_req_integration_ids=None,
                                    arg_required_integration_type=None):
        worksapec_id = self.generic.env["workspace_id"]
        payload = {"filter": {"workspace_id": [worksapec_id]}}
        response = self.generic.execute_api_call(self.ou_group_list, "post", data=payload)
        if response["records"]:
            create_OU_payload = generate_create_ou_payload_interation_basic(arg_req_ou_name=arg_req_ou_name,
                                                                            ou_category_id=response["records"][2]['id'],
                                                                            parent_ref_id=response["records"][2][
                                                                                'root_ou_ref_id'])

            create_OU_response = self.reusable.retrieve_required_api_response(
                arg_req_api=self.create_ou_url,
                arg_req_payload=create_OU_payload
            )
            # create_OU_response = json.loads(create_OU_response.text)
            return create_OU_response

    def create_people_OU(self, arg_req_ou_name,
                         arg_req_integration_ids,
                         arg_users,
                         arg_required_integration_type="jira",
                         ):
        create_OU_payload = generate_create_ou_payload_people_based(arg_users=arg_users,
                                                                    arg_req_ou_name=arg_req_ou_name,
                                                                    arg_req_integration_ids=arg_req_integration_ids,
                                                                    arg_required_integration_type=arg_required_integration_type)

        create_OU_response = self.reusable.retrieve_required_api_response(
            arg_req_api=self.create_ou_url,
            arg_req_payload=create_OU_payload
        )
        return create_OU_response

    def get_OU_mangers(self, fields=["email"]):
        create_get_OU_mangers_payload = generate_get_ou_manager_users(fields)
        create_OU_response = self.generic.execute_api_call(self.get_OU_manager_url, "post",
                                                           data=create_get_OU_mangers_payload)

        return create_OU_response

    def delete_OU(self, arg_OU_id):
        delete_OU_payload = generate_delete_OU_payload(arg_OU_id=arg_OU_id)
        delete_OU_response = self.reusable.retrieve_required_api_response(
            arg_req_api=self.delete_OU_url,
            arg_req_payload=delete_OU_payload,
            request_type="delete"
        )

        return delete_OU_response

    def create_filter_based_ou(self,
                               arg_required_ou_name,
                               arg_required_integration_id,
                               arg_required_integration_type="github",
                               arg_required_filters_key_value_pair={}
                               ):

        worksapec_id = self.generic.env["workspace_id"]
        payload = {"filter": {"workspace_id": [worksapec_id]}}
        response = self.generic.execute_api_call(self.ou_group_list, "post", data=payload)
        required_payload_generation = generate_dynamic_ou_payload_filter_based(
            arg_required_ou_name=arg_required_ou_name,
            arg_required_filters_key_value_pair=arg_required_filters_key_value_pair,
            arg_required_integration_type=arg_required_integration_type,
            category_id=response["records"][0]['id'], dashboardid=response["records"][0]['root_ou_ref_id']
        )

        # print("required_payload_generation", required_payload_generation)
        OU_creation_response = self.reusable.retrieve_required_api_response(
            arg_req_api=self.create_ou_url,
            arg_req_payload=required_payload_generation
        )
        return OU_creation_response

    def delete_required_ou(self, arg_requried_ou_id):
        delete_ou_response = self.generic.execute_api_call(
            url=self.create_ou_url,
            request_type="delete", data=arg_requried_ou_id,
        )

        assert delete_ou_response == "ok", "OU is not yet deleted"

    def create_user_attribute_based_ou(
            self, arg_required_ou_name,
            arg_required_integration_id,
            arg_required_user_atrribute_filters_key_value_pair={},
            arg_required_integration_type="github",
            arg_required_filters_key_value_pair={}
    ):
        worksapec_id = self.generic.env["workspace_id"]
        payload = {"filter": {"workspace_id": [worksapec_id]}}
        response = self.generic.execute_api_call(self.ou_group_list, "post", data=payload)
        required_payload_generation = generate_dynamic_ou_payload_filter_based(
            arg_required_ou_name=arg_required_ou_name,
            arg_required_filters_key_value_pair=arg_required_filters_key_value_pair,
            arg_required_integration_type=arg_required_integration_type,
            category_id=response["records"][0]['id'], dashboardid=response["records"][0]['root_ou_ref_id']
        )
        OU_creation_response = self.reusable.retrieve_required_api_response(
            arg_req_api=self.create_ou_url,
            arg_req_payload=required_payload_generation
        )
        return OU_creation_response

    def get_ou_id(self, filter=False):
        list_ou_id = []
        ou_baseurl = self.baseurl + "org/units/list"
        worksapec_id = self.generic.env["workspace_id"]
        payload = {"filter": {"workspace_id": [worksapec_id]}}
        if filter:
            payload["partial"] = {"name": filter}
        response = self.generic.execute_api_call(ou_baseurl, "post", data=payload)
        if response["records"]:
            for ou_id in response["records"]:
                list_ou_id.append(ou_id["id"])
            LOG.info("OU id  list : {}".format(list_ou_id))
            return list_ou_id
        else:
            LOG.info("OU ids are not present the workspace")
            return None

    def retrieve_ou_uuid(self, ou_id):
        url = self.create_ou_url + "/" + ou_id
        response = self.generic.execute_api_call(url, "get", data={})
        return response["ou_id"]

    def retrieve_child_ous(self, ou_id, include_parent_ou=False, trellis_mapping=True):
        url = self.baseurl + self.api_info["org_list"]
        payload = {"page_size": 20, "filter": {"parent_ref_id": ou_id}}
        response = self.generic.execute_api_call(url, "post", data=payload)
        childous = [eachitem["ou_id"] for eachitem in response["records"]]
        if trellis_mapping:
            childous = [eachitem["ou_id"] for eachitem in response["records"] if "trellis_profile_id" in eachitem.keys()]
        if include_parent_ou:
            ou_uuid = self.retrieve_ou_uuid(ou_id=ou_id)
            childous.append(ou_uuid)
        return childous

    def retrieve_active_users_version(self):
        url = self.baseurl + self.api_info["org_users_version"]
        response = self.generic.execute_api_call(url, "get", data={})
        ouversions = pd.json_normalize(response["records"])
        active_version = (((ouversions.loc[ouversions['active'] == True])["version"]).to_list())[0]
        return active_version

    def retrieve_valid_users(self, user_id=None, full_name=None, integration_ids=None, custom_field=None):
        active_ou_version = self.retrieve_active_users_version()
        url = self.baseurl + self.api_info["org_users_list"] + "?version=" + str(active_ou_version)
        response = self.generic.execute_api_call(url, "post", data={"page_size": 100000})
        ouuserdf = pd.json_normalize(response["records"])
        validouuserdf = ouuserdf.loc[ouuserdf['id'] != "0"]
        if user_id:
            validouuserdf = validouuserdf[validouuserdf["org_uuid"]==user_id]
            if custom_field:
                return validouuserdf[f"additional_fields.{custom_field}"].values.tolist()[0]
        if full_name:
            validouuserdf = validouuserdf[validouuserdf["full_name"]==full_name]
        if user_id or full_name:
            integration_user_ids = validouuserdf["integration_user_ids"].tolist()
            integration_user_ids_flat_list = [item for sublist in integration_user_ids for item in sublist]
            fetch_column = "user_id"
            if integration_ids:
                fetch_column = "integration_id"
            return (pd.DataFrame(integration_user_ids_flat_list))[fetch_column].tolist()
        return validouuserdf

    def retrieve_ou_assocaited_users(self, ou_id):
        url = self.create_ou_url + "/" + ou_id
        response = self.generic.execute_api_call(url, "get", data={})
        ouusersdf = self.retrieve_valid_users()
        if "default_section" in response:
            if "dynamic_user_definition" in response["default_section"] and "users" in response["default_section"]:
                tempdict = (response["default_section"])["dynamic_user_definition"]
                for eachkey in tempdict:
                    if eachkey != "partial_match":
                        if eachkey.startswith("custom_field"):
                            dfcolumn = "additional_fields." + (eachkey.rsplit("_", 1))[1]
                            ouusersdf = ouusersdf.loc[ouusersdf[dfcolumn].isin(tempdict[eachkey])]
                        else:
                            ouusersdf = ouusersdf.loc[ouusersdf[eachkey].isin(tempdict[eachkey])]
            elif "users" in response["default_section"]:
                user_ids = response["default_section"]["users"]
                ouusersdf = ouusersdf.loc[ouusersdf['id'].isin(user_ids)]
        return (ouusersdf['org_uuid']).to_list()

    def get_org_units(self, ou_categories, return_type="id"):
        org_id = []
        baseurl = self.baseurl + self.api_info["ou_list"]
        payload = {"page": 0, "page_size": 50000, "filter": {"ou_category_id": [ou_categories]}}
        response = self.generic.execute_api_call(baseurl, "post", data=payload)
        if len(response["records"]):
            if return_type == "response":
                return response
            else:
                for org_ids in response["records"]:
                    if return_type == "id":
                        org_id.append(org_ids["id"])
                    elif return_type == "name":
                        org_id.append(org_ids["name"])
                    elif return_type == "ou_id":
                        org_id.append(org_ids["ou_id"])

        else:
            LOG.info("org units name is not present")
            return None
        return org_id

    def create_update_ou(self, name, ou_category_id, ou_id=None, default_dashboard_id=0, sections=None,
                         dynamic_user_definition=None, parent_ref_id=None, req_type="post", user_type="ADMIN"):
        baseurl = self.generic.connection["base_url"] + self.api_info["create_update_ou"]
        payload = {
            "name": name,
            "ou_category_id": ou_category_id,
            "default_dashboard_id": default_dashboard_id,
        }
        if ou_id:
            payload["id"] = ou_id

        if parent_ref_id:
            payload["parent_ref_id"] = str(parent_ref_id)

        if sections:
            payload["sections"] = sections
        if dynamic_user_definition:
            payload["default_section"] = {"dynamic_user_definition": dynamic_user_definition}

        payload = [payload]
        response = self.generic.rbac_user(baseurl, req_type, data=payload, user_type=user_type)
        return response

    # --
    def ou_filters(self, id, integration, integration_type, var_filters=None):

        integrations_filters = {"type": integration_type}
        if var_filters:
            integrations_filters["filters"] = var_filters
        updated_integration = {"id": id, "integrations": {integration: integrations_filters}}
        return updated_integration

    def get_org_users_list(self):
        baseurl = self.generic.connection["base_url"] + self.api_info["org_users_list"]
        payload = {"page": 0, "page_size": 5000, "filter": {}}
        response = self.generic.execute_api_call(baseurl, "post", data=payload)
        return response

    def get_users_list(self):
        baseurl = self.generic.connection["base_url"] + self.api_info["propelo_users_list"]
        payload = {"page": 0, "page_size": 5000, "filter": {}}
        response = self.generic.execute_api_call(baseurl, "post", data=payload)
        return response

    def ou_id_and_filter_type(self):
        ou_ids=self.generic.env['dora_ou_ids']
        ou_filter = []
        # ou_ids = self.env["dora_ou_ids"]
        jira_filter_type = []
        ado_filter_type = []

        for i in range(0, len(ou_ids)):
            jira_int_ids = []
            ado_int_ids = []
            scm_int_ids = []
            # breakpoint()

            ado_filter_type_custom_field = []
            # breakpoint()
            jira, ado, scm = self.separation_of_int_ids_based_on_OU(ou_id=ou_ids[i])
            if len(jira) != 0:
                jira_int_ids.append(ou_ids[i])
                jira_filter_type = self.generic.api_data["constant_filters_jira"]
                jira_filter_type_custom_field = self.generic.get_aggregration_fields(only_custom=True,
                                                                             ou_id=ou_ids[i])
                jira_filter_type = jira_filter_type + list(jira_filter_type_custom_field)
                filter = jira_filter_type
                for j in filter:
                    ou_filter.append(
                        {"ou_ids": ou_ids[i], "filter": j, "jira_int_ids": jira, "ado_int_ids": ado,
                         "scm_int_ids": scm, "type": "jira"})
            elif len(ado) != 0:
                ado_int_ids.append(ou_ids[i])
            if len(scm) != 0:
                scm_int_ids.append(ou_ids[i])

            # ado_filter_type = []

            if len(ado) != 0:
                ado_filter_type = self.generic.api_data["constant_filters_ado"]
                ado_filter_type_custom_field_set = self.generic.get_ado_custom_field(only_custom=True,
                                                                             integration_id=ado)
                ado_filter_type_custom_field = list(ado_filter_type_custom_field_set)
                for k in ado_filter_type_custom_field:
                    if "Scheduling" in k:
                        ado_filter_type_custom_field.remove(k)

                ado_filter_type = ado_filter_type + list(ado_filter_type_custom_field)
                filter = ado_filter_type
                for k in filter:
                    ou_filter.append(
                        {"ou_ids": ou_ids[i], "filter": k, "jira_int_ids": jira, "ado_int_ids": ado,
                         "scm_int_ids": scm, "type": "azure_devops"})

        return ou_filter

    def separation_of_int_ids_based_on_OU(self, ou_id):
        #       get integration ids based on the ou_id
        # breakpoint()
        integration_id = self.generic.get_integrations_based_on_ou_id(ou_id)
        jira = []
        scm = []
        ado = []
        for i in integration_id:
            url = self.generic.connection["base_url"] + "integrations/" + str(i)
            resp = self.generic.execute_api_call(url, 'get', data={})
            if resp['application'] == "jira":
                jira.append(i)

            elif resp['application'] == "azure_devops":
                if resp["metadata"]["subtype"] == "wi":
                    ado.append(i)
                elif resp["metadata"]["subtype"] == "scm":
                    scm.append(i)

            elif resp['application'] in ["github", "gitlab", "bitbucket"]:
                scm.append(i)

        return jira, ado, scm

    def get_workspace_associated_to_ou(self, ou_id):
        workspace_id = []
        try:
            org_url = self.generic.connection["base_url"] + "org/units/" + ou_id
            resp = self.generic.execute_api_call(org_url, 'get', data={})
            if "workspace_id" in resp.keys():
                workspace_id.append(resp['workspace_id'])
            else:
                LOG.info("There is no workspace_id associated to ou_id---{}".format(ou_id))

        except Exception as ex:
            LOG.info("failure due to {}".format(ex))
        return workspace_id

    def get_workflow_profile_associated_OU(self, ou_id):
        workflow_profile = []
        try:
            org_url = self.generic.connection["base_url"] + "org/units/" + ou_id
            resp = self.generic.execute_api_call(org_url, 'get', data={})
            if "workflow_profile_id" in resp.keys():
                workflow_profile.append(resp['workflow_profile_id'])
            else:
                LOG.info("There is no workfloe profile associated to ou_id---{}".format(ou_id))

        except Exception as ex:
            LOG.info("failure due to {}".format(ex))
        return workflow_profile

    def retrieve_user_managed_ous(self, email=None, user_managed_trellis_ous="ALL"):
        ## retrieve complete users list
        baseurl = self.generic.connection["base_url"] + self.api_info["propelo_users_list"]
        payload = {}
        response = self.generic.execute_api_call(baseurl, "post", data=payload)
        ## users response normalisation
        response_df = pd.json_normalize(response["records"])
        if email:
            ## retrieve user managed ous
            result_df = response_df[response_df["email"]==email]
            user_ous = result_df["managed_ou_ref_ids"].values.tolist()
            user_ous = [item for sublist in user_ous for item in sublist]
            user_ous = list(map(str, user_ous))
            ## retrieve trellis profile associated ous
            trellis_profile_associated_ous = self.retrieve_trellis_profile_associated_org_units()
            if user_managed_trellis_ous=="YES":
                ## return user managed ous with trellis profile associated
                return list(set(trellis_profile_associated_ous) & set(user_ous))
            elif user_managed_trellis_ous=="NO":
                ## return non user managed trellis ous
                return list(set(trellis_profile_associated_ous) - set(user_ous))
            return user_ous
        return response
    
    def retrieve_trellis_profile_associated_org_units(self):
        ## retrieve complete ous list
        baseurl = self.generic.connection["base_url"] + self.api_info["org_list"]
        payload = {}
        response = self.generic.execute_api_call(baseurl, "post", data=payload)
        ## ous response normalisation
        response_df = pd.json_normalize(response["records"])
        response_df = response_df.fillna("")
        ## filtering trellis profile associated ous
        result_df = response_df[response_df["trellis_profile_id"]!=""]
        profile_associated_ous = result_df["id"].values.tolist()
        return profile_associated_ous
        
