import logging
import pytest
import pandas as pd
import time
from copy import deepcopy

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCMPRsListReportVsGithubAPIData:

    @pytest.mark.run(order=1)
    @pytest.mark.scmsanity
    def test_github_api_data_vs_scm_prs_report_sanity_check(self, get_integration_obj, create_generic_object, reports_datetime_utils_object):
        """Validate alignment of scm_prs_report"""

        mismatch_users_list = []
        epoch_daterange, daterange = reports_datetime_utils_object.get_last_month_epochdate_and_date_range()

        ## retrieve scm prs report widget data
        org_id = (create_generic_object.env["set_ous"])[0]
        gt,lt = epoch_daterange
        report_payload = {
            "filter":{"sort_xaxis":"value_high-low","integration_ids":get_integration_obj,
                    "pr_created_at":{"$gt":gt,"$lt":lt},
                    "code_change_size_unit":"files",
                    "code_change_size_config":{"small":"50","medium":"150"},
                    "comment_density_size_config":{"shallow":"1","good":"5"}},
            "across":"creator","sort":[{"id":"count","desc":True}],"ou_ids":[org_id]}
        base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_prs-report"]
        prs_report_resp = create_generic_object.execute_api_call(base_url, "post", data=report_payload)
        if prs_report_resp["count"] == 0: pytest.skip("no data present in widget api")
        prs_report_resp_df = pd.json_normalize(prs_report_resp['records'], max_level=1)
        required_github_users_and_prs = prs_report_resp_df[["key",'additional_key', "count"]].iloc[:10].values.tolist()

        ## retrieve github api data for required users
        st, et = daterange
        github_integration_id = (create_generic_object.integration_ids_basedon_workspace(application="github"))[0]
        github_api_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["github_self_service_api"]
        for eachgithubuser in required_github_users_and_prs:
            if "bot" not in eachgithubuser[1]:
                print(eachgithubuser)
                time.sleep(30) ## wait time - 10 sec
                gitub_api_payload = {   "integration_id": int(github_integration_id),
                                        "users": [eachgithubuser[1]],
                                        "from": st,"to": et }
                gitub_api_resp = create_generic_object.execute_api_call(github_api_url, "post", data=gitub_api_payload)
                github_prs = pd.json_normalize(gitub_api_resp["users"][0]['pull_requests'], max_level=1)
                github_prs[["project", "number"]] = github_prs['url'].str.extract(r'github.com/(.+)/pull/(\d+)')
                if len(github_prs) != eachgithubuser[2]:
                    mismatch_users_list.append("github prs:" + str(len(github_prs)) + "scm prs report:" + str(eachgithubuser)+ "difference:"+str(len(github_prs) - eachgithubuser[2]))
        if len(mismatch_users_list) > 0:
            LOG.info("mismatch data users list {}".format(mismatch_users_list))
        assert len(mismatch_users_list) == 0, "scm prs list and github api data are not consistent"

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    def test_github_api_data_vs_scm_prs_list(self, get_integration_obj, create_generic_object, reports_datetime_utils_object):
        """Validate alignment of scm_prs_report"""

        datavalidation_flag = True
        mismatch_users_list = []
        LOG.info("==== create widget with available filter ====")
        epoch_daterange, daterange = reports_datetime_utils_object.get_last_week_epochdate_and_date_range()

        ## retrieve scm prs report widget data
        org_id = (create_generic_object.env["set_ous"])[0]
        gt,lt = epoch_daterange
        report_payload = {
            "filter":{"sort_xaxis":"value_high-low","integration_ids":get_integration_obj,
                    "pr_created_at":{"$gt":gt,"$lt":lt},
                    "code_change_size_unit":"files",
                    "code_change_size_config":{"small":"50","medium":"150"},
                    "comment_density_size_config":{"shallow":"1","good":"5"}},
            "across":"creator","sort":[{"id":"count","desc":True}],"ou_ids":[org_id]}
        base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_prs-report"]
        prs_report_resp = create_generic_object.execute_api_call(base_url, "post", data=report_payload)
        if prs_report_resp["count"] == 0: pytest.skip("no data present in widget api")
        prs_report_resp_df = pd.json_normalize(prs_report_resp['records'], max_level=1)
        required_github_users_and_prs = prs_report_resp_df[["key",'additional_key', "count"]].iloc[:10].values.tolist()

        ## retrieve github api data for required users
        st, et = daterange
        github_integration_id = (create_generic_object.integration_ids_basedon_workspace(application="github"))[0]
        github_api_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["github_self_service_api"]
        for eachgithubuser in required_github_users_and_prs:
            if "-" in eachgithubuser[1]:
                time.sleep(30) ## wait time - 10 sec
                gitub_api_payload = {   "integration_id": int(github_integration_id),
                                        "users": [eachgithubuser[1]],
                                        "from": st,"to": et }
                gitub_api_resp = create_generic_object.execute_api_call(github_api_url, "post", data=gitub_api_payload)
                github_prs = pd.json_normalize(gitub_api_resp["users"][0]['pull_requests'], max_level=1)
                github_prs[["project", "number"]] = github_prs['url'].str.extract(r'github.com/(.+)/pull/(\d+)')
                github_prs = github_prs[github_prs['project'].str.startswith("levelops")]
                assert len(github_prs) == eachgithubuser[2],  "scm prs versus github data is not matching"
                github_prs_target = deepcopy(github_prs)
                github_prs_target = github_prs_target.drop(["url"], axis=1)
                github_prs_target = github_prs_target.sort_values(by='number')

                ##drilldown data validation
                drilldown_payload = {
                    "filter":{"pr_created_at":{"$gt":gt,"$lt":lt},"integration_ids":get_integration_obj,
                            "creators":[eachgithubuser[0]],"code_change_size_unit":"files",
                            "code_change_size_config":{"small":"50","medium":"150"},
                            "comment_density_size_config":{"shallow":"1","good":"5"}},
                    "across":"creator","ou_ids":[org_id],"ou_exclusions":["creators"]}
                drilldown_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_pr_list_drilldown"]
                drilldown_resp = create_generic_object.execute_api_call(drilldown_url, "post", data=drilldown_payload)
                drilldown_prs = pd.json_normalize(drilldown_resp['records'], max_level=1)
                drilldown_prs_target = drilldown_prs[[ "number", "project", "title","state", 
                                                    "pr_created_at", "pr_updated_at", "pr_closed_at", "pr_merged_at"]] 
                # drilldown_prs_target['created_at'] = drilldown_prs_target['pr_created_at'].apply(lambda x: datetime.datetime.utcfromtimestamp(x).strftime('%Y-%m-%dT%H:%M:%SZ'))
                drilldown_prs_target['created_at'] = drilldown_prs_target['pr_created_at'].apply(
                                                        reports_datetime_utils_object.convert_epochtime_to_github_api_datetime_format)
                drilldown_prs_target['updated_at'] = drilldown_prs_target['pr_updated_at'].apply(
                                                        reports_datetime_utils_object.convert_epochtime_to_github_api_datetime_format)
                drilldown_prs_target['closed_at'] = drilldown_prs_target['pr_closed_at'].apply(
                                                        reports_datetime_utils_object.convert_epochtime_to_github_api_datetime_format)
                drilldown_prs_target['merged_at'] = drilldown_prs_target['pr_merged_at'].apply(
                                                        reports_datetime_utils_object.convert_epochtime_to_github_api_datetime_format)
                drilldown_prs_target = drilldown_prs_target.drop(["pr_created_at", "pr_updated_at", "pr_closed_at", "pr_merged_at"], axis=1)
                drilldown_prs_target = drilldown_prs_target.sort_values(by='number')
                # Reorder columns to align them
                drilldown_prs_target = drilldown_prs_target[github_prs_target.columns]
                columns_to_apply = ['created_at', 'updated_at', 'closed_at', 'merged_at' ]
                github_prs_target[columns_to_apply] = github_prs_target[columns_to_apply].apply(lambda col: col.str.split('T').str[0])
                drilldown_prs_target[columns_to_apply] = drilldown_prs_target[columns_to_apply].apply(lambda col: col.str.split('T').str[0])
                ## filter out the open PRs - to make sure there is no conflict the latest ingested data
                drilldown_prs_target_filterd = drilldown_prs_target.drop(["state", "updated_at", "closed_at", "merged_at"], axis=1)
                github_prs_target_filtered = github_prs_target.drop(["state", "updated_at", "closed_at", "merged_at" ], axis=1)
                # Compare the DataFrames
                df_diff = pd.concat([github_prs_target_filtered,drilldown_prs_target_filterd]).drop_duplicates(keep=False)                
                if len(df_diff)>0:
                    datavalidation_flag = False
                    mismatch_users_list.append(eachgithubuser)

        if len(mismatch_users_list) > 0:
            LOG.info("mismatch data users list {}".format(mismatch_users_list))
        assert datavalidation_flag == True, "scm prs list and github api data are not consistent"

