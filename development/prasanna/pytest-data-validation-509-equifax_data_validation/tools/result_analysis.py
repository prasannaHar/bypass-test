import sys
import zipfile
import os
sys.path.append('../')
from datetime import datetime
now = datetime.now()
TodaysDateGenerated = now.strftime("%d-%m-%Y %H:%M:%S")

from src.lib.automatic_mail_sender import *

def zip_directory(directory_path, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(file_path, os.path.relpath(file_path, directory_path))

# zip log files directory
directory_path = '../src/log_updates'
output_path = '../src/log_updates/log_files.zip'

zip_directory(directory_path, output_path)

# Access command-line arguments
arguments = sys.argv
env = arguments[1]
tenant = arguments[2]
sender_email = arguments[3]
target_emails = arguments[4]
accesscode = arguments[5]

subject = "Test Report - Environment:%s Tenant:%s Job Completion Date: "% (env, tenant)  + str(TodaysDateGenerated)

automatic_mail_using_gmail_fun(
    arg_sender = sender_email,
    arg_req_email_address=target_emails,
    arg_subject= subject,
    arg_mail_content="Test execution html report is attached to this mail chain",
    arg_required_attachments=["../result/report.html", "../src/log_updates/log_files.zip"],
    arg_smtp_access_code=accesscode)

