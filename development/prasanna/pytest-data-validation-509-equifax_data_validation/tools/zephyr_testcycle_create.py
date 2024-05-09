import os
import sys

import requests

arguments = sys.argv

xml_report_path = "result/result.xml"

url = arguments[2]
payload = {'type': 'application/xml'}
files = [
    ('file', ('file', open(xml_report_path, 'rb'), 'application/octet-stream'))
]
headers = {
    'Authorization': 'Bearer ' + arguments[1]
}

response = requests.request("POST", url, headers=headers, data=payload, files=files)
# breakpoint()

print(response.text)
