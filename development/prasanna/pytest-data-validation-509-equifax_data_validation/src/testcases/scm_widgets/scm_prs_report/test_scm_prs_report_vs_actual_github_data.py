import logging
import pytest
import pandas as pd
import time

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestSCMPRsReportVsGithubAPIData:
    @pytest.mark.run(order=1)
    @pytest.mark.regression
    def test_scm_prs_report_vs_github_api_data(self, get_integration_obj, create_generic_object, reports_datetime_utils_object):
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
        required_github_users_and_prs = prs_report_resp_df[['additional_key', "count"]].iloc[:10].values.tolist()

        ## retrieve github api data for required users
        st, et = daterange
        github_integration_id = (create_generic_object.integration_ids_basedon_workspace(application="github"))[0]
        github_api_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["github_self_service_api"]
        for eachgithubuser in required_github_users_and_prs:
            if "-" in eachgithubuser[0]:
                time.sleep(20) ## wait time - 10 sec
                gitub_api_payload = {   "integration_id": int(github_integration_id),
                                        "users": [eachgithubuser[0]],
                                        "from": st,"to": et }
                gitub_api_resp = create_generic_object.execute_api_call(github_api_url, "post", data=gitub_api_payload)
                github_prs = pd.json_normalize(gitub_api_resp["users"][0]['pull_requests'], max_level=1)
                github_prs[["project", "number"]] = github_prs['url'].str.extract(r'github.com/(.+)/pull/(\d+)')
                github_prs = github_prs[github_prs['project'].str.startswith("levelops")]
                if len(github_prs) != eachgithubuser[1]:
                    datavalidation_flag = False
                    mismatch_users_list.append(eachgithubuser)

        if len(mismatch_users_list) > 0:
            LOG.info("mismatch data users list {}".format(mismatch_users_list))
        assert datavalidation_flag == True, "scm prs list and github api data are not consistent"
        
