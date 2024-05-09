import os
import sys

import requests

token = os.getenv("TOKEN")
url = os.getenv("ZEPHYR_URL")


def test_zephyr_tc_creation():
    xml_report_path = "/harness/result/result.xml"
    payload = {'type': 'application/xml'}
    files = [
        ('file', ('file', open(xml_report_path, 'rb'), 'application/octet-stream'))
    ]
    headers = {
        'Authorization': 'Bearer ' + token
    }
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    print(response.text)
