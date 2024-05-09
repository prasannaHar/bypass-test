import json
import logging

from github import Github
import create_commits as cm
import config as cf

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class GithubApi:
    def __init__(self):
        pass

    def create_pull_with_commits(self):
        try:
            cm.commit_create()
            token = cf.token
            g = Github(token)

            repo_owner = cf.repo_owner
            repo_name = cf.repo_name
            repo = g.get_repo(f"{repo_owner}/{repo_name}")
            #
            pull_title = cf.pull_title
            pull_body = cf.pull_body
            base_branch = cf.base_branch
            head_branch = cf.head_branch
            pull = repo.create_pull(title=pull_title, body=pull_body, base=base_branch, head=head_branch)
            # reviewers = ["user1", "user2"]

            # commits = repo.get_commits()
            # for commit in commits:
            #     # print(commit.get_pulls())
            #     # print(commit.sha, commit.commit.message)

            pulls = repo.get_pulls()
            for pull in pulls:
                LOG.info(f"pull.number---{pull.number}, pull.title---{pull.title}")
                if pull.number > 0:
                    pull_number = pull.number
                    pull = repo.get_pull(pull_number)
                    commit_message = f"Merge pull request-{pull_number}"
                    merge_method = "merge"  # Can be "merge", "squash", or "rebase"
                    merge_pr = pull.merge(commit_message=commit_message, merge_method=merge_method)
                    LOG.info(f"merge_pr.message---{merge_pr.message}")
                    if merge_pr.message == "Pull Request successfully merged":
                        commits = pull.get_commits()
                        for commit in commits:
                            LOG.info(f"commit.sha---{commit.sha}, commit.commit.message----{commit.commit.message}")
                            return json.dumps({pull_number: [commit.sha, commit.commit.message]})


        except Exception as ex:
            print(f"Exception raised is {ex}")
            return {"Exception": ex}
# data = create_pull_with_commits()
# print(data)
