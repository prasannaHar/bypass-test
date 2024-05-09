import config as cf

import random
import requests
import json
import base64
import commit_file as c_file


# Set the required variables
def commit_create():
    commit_message = "edited sample csv file via API" + str(random.randint(0, 999))
    file_content = "This is the content of the new file." + str(random.randint(0, 999))

    # Encode the file content
    encoded_content = base64.b64encode(file_content.encode()).decode()

    # edit the csv file
    c_file.edit_csv_file()

    # list commits:
    response_commit_list = requests.get(url=cf.commit_base_url + "/commits", headers=cf.headers)
    print(response_commit_list.text, response_commit_list.status_code)
    # constant file path
    file_path = cf.commit_file_path

    # Define the API endpoint
    url = cf.commit_base_url + "/contents/" + file_path
    print(f"url---{url}")

    # Prepare the commit payload

    def commit_response(sha=None):
        commit_payload = {
            "message": commit_message,
            "content": encoded_content,
            "branch": "1-new-branch",
            "sha": sha,
            "committer": {"name": "vinniiii", "email": "41627875+vinniiii@users.noreply.github.com"}

        }

        response = requests.put(url, headers=cf.headers, data=json.dumps(commit_payload))
        return response

    # Check the response
    if commit_response().status_code in [200, 201]:
        print("File added successfully!")
        print(commit_response().text)
        res = json.loads(commit_response().text)


    else:
        print("Failed to add file. Error:", commit_response().text)
        raise Exception("Status code is not 200---status code---{},message---{}".format(commit_response().status_code,
                                                                                        commit_response().text))

# get the latest commit---0654d46980a1e86e8b9dde99e19e9183f8d4c0fd
# response_commit_list
