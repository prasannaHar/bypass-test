'''
This file is used to clean up the tenants.
Actions that can be performed by this file:
1. list_integrations: List the integrations available on the tenant.
2. disable_integrations: Disable the integrations on the tenant.

Pre-requisites:
1. Provide below environment variables
    -> BASE_URL - URL to the tenant (Eg: https://staging.api.propelo.ai/v1/)
    -> SEI_TOKEN - Token to execute API to the tenant
    -> INTEGRATIONS_FILE_PATH - Path to the CSV file which contains information to which integrations needs to be deleted.
        -> This CSV file should contain atleast 3 columns ("Integration Name", "Integration ID", "Integration Required?")
        -> "Integration Required?" should have 'Yes' or 'yes' for the integrations to not disable.
        -> This parameter is only used for action 'disable_integrations'

Sample execution of this script:
1. python3 cleanup_tenants.py list_integrations     -> Lists integration in /tmp/int_list.csv file.
2. python3 cleanup_tenants.py disable_integrations  -> Disables integration in the file provided in 
                                                       $INTEGRATIONS_FILE_PATH variable.
'''

import requests
import json
import csv
import os
import requests
import sys

class TenantCleanup:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def list_tenant_integrations(self):

        url = self.base_url + "/integrations/list"

        page = 0
        payload = {
            "page": page,
            "page_size": 100,
            "filter": {}
            }
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        integration_list = [["Application", "Integration Name", "Integration ID", "Integration Status", "Integration Required?", "Used By", "Mention Usage"]]

        has_next = True
        while has_next:
            print(f"Collecting data from page: {page}")
            response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
            print("***** URL: " + url)
            if response.status_code == 200:
                resp_data = response.json()
            else:
                print(f"ERROR: Error while collecting data for Integration list. status_code: "
                    f"{response.status_code}. Error: {response.json()}")
                return []

            for integration in resp_data.get("records", []):
                integration_list.append(
                    [
                        integration["application"],
                        integration["name"],
                        integration["id"],
                        integration["status"]
                    ]
                )

            if resp_data.get("_metadata", {}).get("next_page", 0) == False:
                has_next = False
            else:
                page = resp_data["_metadata"]["next_page"]
                payload["page"] = page

        return integration_list

    def read_integration_list_from_csv(self, file_path):
        integrations_to_disable = {}
        with open(file_path, newline='') as csvfile:
            data = csv.DictReader(csvfile, delimiter=',')

            for d in data:
                if d['Integration Required?'].lower() == "yes":
                    print("Not deleting ", d['Integration Name'], " ---> ", d['Integration ID'])
                    continue

                integrations_to_disable[d["Integration ID"]] = d['Integration Name']

        return integrations_to_disable

    def disable_integrations(self, integration_list):
        failed_to_disable = {}
        successful_disable = {}
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        for id, name in integration_list.items():

            url = self.base_url + f"/ingestion/{id}/pause"

            resp = requests.put(url, headers=headers)
            if resp.status_code == 200:
                resp_json = resp.json()
                if resp_json.get("status") == "running":
                    failed_to_disable[id] = name
                else:
                    successful_disable[id] = name
            elif resp.status_code == 404:
                continue
            else:
                print(f"Error while disabling integration. Error code: {resp.status_code}, {resp.json()}")
                failed_to_disable[id] = name

            print(f"Integration Name: '{name}', Integration ID: {id}, Disable Status Code: {resp.status_code}")

        return failed_to_disable, successful_disable

    def check_integration_disabled(self, integration_list):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        import pdb
        pdb.set_trace()

        for id, name in integration_list.items():
            url = self.base_url + f"/ingestion/{id}/frequency"

            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                resp_json = resp.json()
                if resp_json.get("status") == "paused":
                    print(f"Integration: {name} -> Integration ID: {id} successully disabled.")
                else:
                    print(f"Integration: {name} -> Integration ID: {id} not disable. Status: {resp_json.get('status')}")
            else:
                print(f"Error while checking integration status. Error code: {resp.status_code}, {resp.json()}")

if __name__ == "__main__":

    url = os.environ.get("BASE_URL")
    token = os.environ.get("SEI_TOKEN")
    file_path = os.environ.get("INTEGRATIONS_FILE_PATH")
    print("file_path: {}".format(file_path))

    action = sys.argv[1]

    print(f"Action selected: {action}")
    cleanup_obj = TenantCleanup(base_url=url, token=token)

    if action == "disable_integrations":
        print("Clean up process start...")
        int_list = cleanup_obj.read_integration_list_from_csv(file_path)
        failed_integrations, successfully_disabled = cleanup_obj.disable_integrations(int_list)

        cleanup_obj.check_integration_disabled(successfully_disabled)

        print("\n#### Summary ####")
        print("Below integrations failed to get disabled.")
        print(failed_integrations)
        print("\n")

        print(f"Total integrations disabled: {len(int_list) - len(failed_integrations)} .")

        print("Tenant clean up task completed.")
    elif action == "list_integrations":
        print("Listing integrations...")
        int_list = cleanup_obj.list_tenant_integrations()

        with open("/tmp/int_list.csv", "w") as csvfile:
            write = csv.writer(csvfile)
            write.writerows(int_list)

        print("Integration list prepared: /tmp/int_list.csv")

