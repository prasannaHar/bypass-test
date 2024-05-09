import logging
import json
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestWidgetHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.api_info = self.generic.get_api_info()
        self.env_info = self.generic.get_env_based_info()

    def generate_paginated_drilldown_data(self, url, payload, pr_lead_time=None):
        "generate the paginated drill-down data"
        payload["page_size"] = 100
        has_next = True
        page_num = 0
        resp_drilldown_df = pd.DataFrame()
        while has_next:
            payload["page"] = page_num
            resp_drilldown = self.generic.execute_api_call(url, "post", data=payload)
            resp_drilldown_df_temp = pd.json_normalize(resp_drilldown['records'], max_level=1)
            if pr_lead_time:
                resp_drilldown_df_temp = pd.json_normalize(resp_drilldown['records'], "data")
            if page_num == 0:
                resp_drilldown_df = resp_drilldown_df_temp
            else:
                resp_drilldown_df = pd.concat([resp_drilldown_df, resp_drilldown_df_temp])
            has_next = resp_drilldown['_metadata']['has_next']
            page_num = page_num + 1
        return resp_drilldown_df

    def create_assignee_allocation_report(self, product_id, integration_ids, ou_ids, epics=False):
        """create widget for the Progress Report"""
        base_url = self.generic.connection["base_url"] + "jira_issues/assignee_allocation_report"
        payload = {"filter": {"product_id": product_id, "integration_ids": integration_ids}, "across": "epic",
                   "ou_ids": ou_ids}
        if epics:
            payload = {"filter": {"product_id": product_id, "integration_ids": integration_ids, "epics": epics},
                       "across": "epic", "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def jira_drilldown_list(self, filters, ou_ids, across=None, ou_exclusions=None, sort=None,
                            ou_user_filter_designation=None):
        """gives jira drilldown list of values"""
        base_url = self.generic.connection["base_url"] + "jira_issues/list"
        payload = {"filter": filters, "ou_ids": ou_ids, "page_size": 10000}
        if across:
            payload["across"] = across
        if ou_exclusions:
            payload["ou_exclusions"] = [ou_exclusions]
        if sort:
            payload["sort"] = sort
        if ou_user_filter_designation:
            payload["ou_user_filter_designation"] = self.generic.env["ou_user_filter_designation"]
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_issue_backlog_trend_report(self, ou_ids, filters, across, interval):
        """create widget for the issue backlog trend Report"""
        base_url = self.generic.connection["base_url"] + self.api_info["jira_issue_backlog_trend"]
        payload = {"across": across, "filter": filters, "interval": interval, "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def schema_validations(self, exp_schema, response, msg):
        """Validate response for given schema"""
        try:
            response = json.dumps(response)
            re = exp_schema(json.loads(response))
        except Exception as ex:
            LOG.error(str(ex))
            raise AssertionError(msg)

    def create_scm_files_report(self, ou_ids, filters, across):
        """create widget for the scm files Report"""
        base_url = self.generic.connection["base_url"] + self.api_info["scm_files_report"]
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_scm_commit_single_stat(self, ou_ids, filters, across):
        """create widget for the scm commit single stat """
        base_url = self.generic.connection["base_url"] + self.api_info["scm-commit-single-stat"]
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_pr_activity(self, filters):
        """create widget for the PR Activity """
        base_url = self.generic.connection["base_url"] + self.api_info["scm-pr-activity"]
        payload = {"filter": filters}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_trellis_score_report(self, filters, user_type="ADMIN_DPS"):
        """create widget for the TRELLIS SCORE REPORT """
        base_url = self.generic.connection["base_url"] + self.api_info["trellis_scores_report"]
        payload = {"filter": filters}
        resp = self.generic.rbac_user(base_url, "post", data=payload, user_type=user_type)
        return resp

    def create_trellis_scores_by_org_units(self, filters, user_type="ADMIN_DPS"):
        """create widget for the TRELLIS SCORE BY ORG UNITs REPORT """
        base_url = self.generic.connection["base_url"] + self.api_info["trellis_scores_by_org_unit_report"]
        payload = {"filter": filters}
        resp = self.generic.rbac_user(base_url, "post", data=payload, user_type=user_type)
        return resp

    def create_individual_raw_stats(self, filters, user_type="ADMIN_DPS"):
        """create widget for the INDIVIDUAL RAW STATS REPORT """
        base_url = self.generic.connection["base_url"] + self.api_info["individual_raw_stats"]
        payload = {"filter": filters}
        resp = self.generic.rbac_user(base_url, "post", data=payload, user_type=user_type)
        return resp

    def create_raw_stats_by_org_units(self, filters, user_type="ADMIN_DPS"):
        """create widget for the RAW STATS BY ORG UNITs REPORT """
        base_url = self.generic.connection["base_url"] + self.api_info["raw_stats_by_org_units"]
        payload = {"filter": filters}
        resp = self.generic.rbac_user(base_url, "post", data=payload, user_type=user_type)
        return resp

    def create_trellis_relative_score(self, filters, page_size=True, user_type="ADMIN_DPS"):
        """create widget for the trellis overview page - relative score report """
        base_url = self.generic.connection["base_url"] + self.api_info["relative_score"]
        payload = {"filter": filters}
        if page_size:
            payload = {"page": 0, "page_size": 100, "filter": filters}
        resp = self.generic.rbac_user(base_url, "post", data=payload, user_type=user_type)
        return resp

    def create_trellis_pr_activity(self, filters, user_type="ADMIN_DPS"):
        """create widget for trellis overview page - pr activity report """
        base_url = self.generic.connection["base_url"] + self.api_info["trellis_scm_activity"]
        payload = {"page": 0, "page_size": 100, "filter": filters}
        resp = self.generic.rbac_user(base_url, "post", data=payload, user_type=user_type)
        return resp

    def retrieve_workitem_details(self, integration_id, workitem_ids):
        filters = {"integration_ids": integration_id, "workitem_ids": workitem_ids}
        payload = {"filter": filters}
        base_url = self.generic.connection["base_url"] + self.api_info["workitems_list"]
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def retrieve_issue_details(self, integration_id, issue_keys):
        filters = {"integration_ids": integration_id, "keys": issue_keys}
        payload = {"filter": filters}
        base_url = self.generic.connection["base_url"] + self.api_info["drill_down_api_url"]
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def trellis_profiles_retriever(self):
        """retrieve trellis profile """
        base_url = self.generic.connection["base_url"] + self.api_info["dev_prod_profile_list"]
        resp = self.generic.execute_api_call(base_url, "post", data={})
        return resp

    def trellis_profiles_settings_retriever(self, profile_id):
        """create widget for trellis overview page - pr activity report """
        dev_prod_api_url = self.generic.connection["base_url"] + self.api_info["dev_prod_profile"]
        if profile_id:
            dev_prod_api_url = dev_prod_api_url + "/" + profile_id
        else:
            dev_prod_api_url = dev_prod_api_url + "/default"
        resp = self.generic.execute_api_call(dev_prod_api_url, "get")
        return resp

    def trellis_profile_creator(self, ref_profile_id):
        payload = self.trellis_profiles_settings_retriever(profile_id=ref_profile_id)
        payload["name"] = "py_" + payload["name"]
        dev_prod_api_url = self.generic.connection["base_url"] + self.api_info["dev_prod_profile"]
        resp = self.generic.execute_api_call(dev_prod_api_url, "post", data=payload)
        return resp["id"]

    def trellis_profile_deletor(self, profile_id, new_profile_deletion=None):
        """retreive trellis profile settings """
        if new_profile_deletion:
            profile_id = self.trellis_profile_creator(ref_profile_id=profile_id)
        dev_prod_api_url = self.generic.connection["base_url"] + \
                           self.api_info["dev_prod_profile"] + "/" + profile_id
        resp = self.generic.execute_api_call(dev_prod_api_url, "delete")
        return resp

    def trellis_profile_updater(self, profile_id, payload):
        dev_prod_api_url = self.generic.connection["base_url"] + \
                           self.api_info["dev_prod_profile"] + "/" + profile_id
        resp = self.generic.execute_api_call(dev_prod_api_url, "put", data=payload)
        resp = json.loads(resp.text)
        return resp["id"]

    def create_sonarqube_code_complexity(self, ou_ids, filters, across):
        """create widget for the SonarQube code complexity report """
        base_url = self.generic.connection["base_url"] + self.api_info["sonarqube-code-complexity"]
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_bulleye_code_coverage(self, filters, across):
        """create widget for the Bullseye code coverage report """
        base_url = self.generic.connection["base_url"] + self.api_info["bullseye-code-coverage"]
        payload = {"across": across, "filter": filters}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def bullseye_files_drilldown_list(self, filters, across=None):
        """gives bullseye files drilldown list of values"""
        base_url = self.generic.connection["base_url"] + self.api_info["bullseye-files-drilldown"]
        payload = {"filter": filters, "page_size": 10000}
        if across:
            payload["across"] = across
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_lead_time_for_changes_widget(self, integration_id, metric="resolve", var_filters=False, across=None):
        """Retuns API response for Lead time For change Widget"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name_scm"])
        velocity_config_id = self.generic.env["env_scm_velocity_config_id"]
        filters = {"integration_ids": integration_id, "metric": metric, "velocity_config_id": velocity_config_id}
        if var_filters:
            filters.update(var_filters)
        base_url = self.generic.connection["base_url"] + self.api_info["lead_time_for_change"]
        payload = {"filter": filters, "ou_ids": org_id}
        if across:
            payload["across"] = across
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def create_time_to_restore_service_widget(self, integration_id, metric="resolve", var_filters=False, across=None):
        """Retuns API response for time to restore service"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name_scm"])
        velocity_config_id = self.generic.env["env_scm_velocity_config_id"]
        filters = {"integration_ids": integration_id, "metric": metric, "velocity_config_id": velocity_config_id}
        if var_filters:
            filters.update(var_filters)
        base_url = self.generic.connection["base_url"] + self.api_info["time_to_restore_service"]
        payload = {"filter": filters, "ou_ids": org_id}
        if across:
            payload["across"] = across
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def create_deployment_frequency_widget(self, integration_id, across=None, metric="resolve", var_filters=False):
        """Retuns API response for deployment frequency"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name_scm"])
        velocity_config_id = self.generic.env["env_scm_velocity_config_id"]
        filters = {"integration_ids": integration_id, "metric": metric, "velocity_config_id": velocity_config_id}
        if var_filters:
            filters.update(var_filters)
        base_url = self.generic.connection["base_url"] + self.api_info["deployment_frequency"]
        payload = {"filter": filters, "ou_ids": org_id}
        if across:
            payload["across"] = across
        resp = self.generic.execute_api_call(base_url, "post", data=payload)

        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def create_failure_rate_widget(self, integration_id, metric="resolve", var_filters=False, across=None):
        """Retuns API response for Failure Rate"""
        org_id = self.generic.get_org_unit_id(org_unit_name=self.env_info["org_unit_name_scm"])
        velocity_config_id = self.generic.env["env_scm_velocity_config_id"]
        filters = {"integration_ids": integration_id, "metric": metric, "velocity_config_id": velocity_config_id}
        if var_filters:
            filters.update(var_filters)
        base_url = self.generic.connection["base_url"] + self.api_info["failure_rate"]
        payload = {"filter": filters, "ou_ids": org_id}
        if across:
            payload["across"] = across
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        if resp["records"]:
            return resp
        else:
            LOG.warning("No Data In Widget Api")
            return None

    def create_issue_bounce_report(self, filters, across, across_limit, ou_ids, sort):
        """create widget for the Issue Bounce report """
        base_url = self.generic.connection["base_url"] + self.api_info["bounce_widget_api_url"]
        payload = {"across": across, "across_limit": across_limit, "filter": filters, "ou_ids": ou_ids, "sort": sort}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_issues_report(self, filters, across, story, ou_ids, sort=False, across_limit=False):
        """create widget for the Issues report """
        if story:
            base_url = self.generic.connection["base_url"] + self.api_info["jira_story_point_report"]
        else:
            base_url = self.generic.connection["base_url"] + self.api_info["jira_tickets_report"]

        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        if across_limit:
            payload["across_limit"] = across_limit
        if sort:
            payload["sort"] = sort
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_issue_lead_time_by_stage_report(self, filters, across, ou_ids):
        """create widget for the Issue Bounce report """
        base_url = self.generic.connection["base_url"] + self.api_info["velocity"]
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids, "ou_user_filter_designation": {}}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def jira_drilldown_velocity_values(self, filters, ou_ids, across=None, ou_exclusions=None, sort=None):
        """gives jira drilldown velocity values """
        base_url = self.generic.connection["base_url"] + self.api_info["velocity_values"]
        payload = {"filter": filters, "ou_ids": ou_ids, "page_size": 10000}
        if across:
            payload["across"] = across
        if ou_exclusions:
            payload["ou_exclusions"] = [ou_exclusions]
        if sort:
            payload["sort"] = sort
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_issue_resolution_time_report(self, filters=None, across=None, ou_ids=None,
                                            across_limit=None, sort=None, payload=None,
                                            ou_user_filter_designation=None):
        """create widget for the issue resolution time report and single stat """
        base_url = self.generic.connection["base_url"] + self.api_info["resolution_time_report"]
        payload = {}
        if payload:
            return self.generic.execute_api_call(base_url, "post", data=payload)
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        if ou_user_filter_designation:
            payload["ou_user_filter_designation"] = self.generic.env["ou_user_filter_designation"]
        if sort:
            payload["sort"] = sort
        if across_limit:
            payload["across_limit"] = across_limit
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_sprint_metric_trend_report(self, filters, ou_ids, across=None, interval=None):
        """create widget for the sprint metric trend report """
        base_url = self.generic.connection["base_url"] + self.api_info["sprint_metrics_report_jira"]

        payload = {"filter": filters, "ou_ids": ou_ids}
        if across:
            payload["across"] = across
        if interval:
            payload["interval"] = interval

        LOG.info("payload---{}".format(payload))
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_issue_time_across_stages(self, filters, across, ou_ids, across_limit=None):
        """create widget for the issue time across stages """
        base_url = self.generic.connection["base_url"] + self.api_info["issue_time_across_stages_jira"]
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        if across_limit:
            payload["across_limit"] = across_limit
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_scm_rework_report(self, ou_ids, filters, across):
        """create widget for the scm rework Report"""
        base_url = self.generic.connection["base_url"] + self.api_info["scm_rework_report"]
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def scm_commits_drilldown_list(self, filters, ou_ids, across):
        """gives scm commits drilldown list"""
        base_url = self.generic.connection["base_url"] + self.api_info["scm_commits_list_drilldown"]
        payload = {"across": across, "filter": filters, "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_widget_report(self, base_url, ou_ids, filters, across=None, interval=None, user_type="ADMIN", sort=None,
                             desc=None):
        """create widget for the widget Report"""
        payload = {"filter": filters, "ou_ids": ou_ids}
        if across:
            payload["across"] = across
        if interval:
            payload["interval"] = interval
        if sort:
            payload["sort"] = [{"id": sort, "desc": desc}]
        LOG.info("Payload------{}".format(json.dumps(payload)))
        resp = self.generic.rbac_user(base_url, "post", data=payload, user_type=user_type)
        return resp

    def scm_pr_list_drilldown_list(self, filters, ou_ids, ou_exclusions, across=None):
        """gives scm pr list drilldown"""
        base_url = self.generic.connection["base_url"] + self.api_info["scm_pr_list_drilldown"]
        payload = {"page": 0, "page_size": 10000, "filter": filters, "ou_ids": ou_ids,
                   "ou_exclusions": [ou_exclusions]}
        if across:
            payload["across"] = across
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp

    def create_issue_lead_time_by_time_spent_in_stages(self, filters, ou_ids):
        """create widget for the Issue Bounce report """
        base_url = self.generic.connection["base_url"] + self.api_info["issue_lead_time_by_time_spent_in_stages"]
        payload = {"filter": filters, "ou_ids": ou_ids}
        resp = self.generic.execute_api_call(base_url, "post", data=payload)
        return resp
