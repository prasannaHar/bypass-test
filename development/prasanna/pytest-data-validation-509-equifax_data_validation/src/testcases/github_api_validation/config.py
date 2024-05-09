import os
import random

token = str(os.getenv('TOKEN'))
headers = {"Accept": "application/vnd.github+json",
           "Authorization": "Bearer " + token,
           "X-GitHub-Api-Version": "2022-11-28"}

repo_owner = "Sampleorg12345"
repo_name = "test_create_pr"

commit_base_url = str(os.getenv("COMMIT_URL"))
commit_file_path = "test" + str(random.randint(0, 999)) + ".csv"


pull_title = "New Pull Request"
pull_body = "This is a new pull request."
base_branch = "main"
head_branch = "1-new-branch"


