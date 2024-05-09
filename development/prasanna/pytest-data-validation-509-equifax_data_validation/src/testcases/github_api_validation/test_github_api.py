import inspect
import json
import logging
import pytest
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestGithubApi:
    @pytest.mark.run(order=1)
    def test_create_commits_n_prs(self, create_pygithub_object, create_generic_object):
        data = create_pygithub_object.create_pull_with_commits()
        data = json.loads(data)
        # breakpoint()
        keys = list(data.keys())
        if "Exception" in keys:
            LOG.info(f"Exception has occured---{data}")
            assert False
        else:
            try:
                for i in keys:
                    commit = data[i]
                    pull_request_number = i
                    # breakpoint()
                    df = pd.DataFrame(columns=["commit", "commit_msg", "pull_no"],
                                      data=[[commit[0], commit[1], pull_request_number]])
                    df.to_csv(
                        "log_updates/" + str(inspect.stack()[0][3])
                        + '.csv', header=True,
                        index=False)

            except Exception as ex:
                assert False, f"exception occured---{ex}"

    def test_commit_prs(self, create_pygithub_object, create_generic_object):
        found_list = []
        filepath = "log_updates/test_create_commits_n_prs.csv"
        df = pd.read_csv(filepath)
        # breakpoint()
        commit = df['commit'][0]
        pr = df['pull_no'][0]
        # check if commit and pullrequest is available in the sei

        commit_url = create_generic_object.connection['base_url'] + create_generic_object.api_data[
            'scm-commit-single-stat']
        pr_url = create_generic_object.connection['base_url'] + "scm/prs/list"
        if create_generic_object.connection['env_name'] == "testui1":
            ou_id = '2429'
            integration_id = create_generic_object.get_integrations_based_on_ou_id(ou_id)
        elif create_generic_object.connection['env_name'] == "staging":
            ou_id = ""
            integration_id = ""
        elif create_generic_object.connection['env_name'] == "prod":
            ou_id = ""
            integration_id = ""

        commit_payload = {"filter": {"visualization": "pie_chart", "code_change_size_unit": "files",
                                     "integration_ids": integration_id,
                                     "code_change_size_config": {"small": "50", "medium": "150"}},
                          "across": "repo_id", "ou_ids": [ou_id]}

        gt, lt = create_generic_object.get_epoc_time(value=5, type="days")
        pr_payload = {"page": 0, "page_size": 10,
                      "filter": {"pr_merged_at": {"$gt": gt, "$lt": lt}, "integration_ids": integration_id,
                                 "repo_ids": ["Sampleorg12345/test_create_pr"], "code_change_size_unit": "files",
                                 "code_change_size_config": {"small": "50", "medium": "150"},
                                 "comment_density_size_config": {"shallow": "1", "good": "5"}}, "across": "repo_id",
                      "ou_ids": [ou_id], "ou_exclusions": ["repo_ids"]}

        pr_response = create_generic_object.execute_api_call(pr_url, "post", data=pr_payload)

        for i in range(0, len(pr_response['records'])):
            print(f"pr_response['records'][i]['number']---->{pr_response['records'][i]['number']},str(pr)---->{str(pr)}")
            if pr_response['records'][i]['number'] == str(pr) and pr_response['records'][i]['commit_shas'] == [commit]:
                LOG.info("PR number found with the  commit sha")
                found_list.append("data_found")

        assert len(found_list) != 0, "Data not yet found"
