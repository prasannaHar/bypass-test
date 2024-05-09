import logging
import json
import pandas as pd
import pytest
import requests
import urllib.request
import re
import ssl
from urllib.parse import urlparse, parse_qs
import time

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class MailinatorHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.mailinator_url = self.generic.get_env_var_info("MAILINATOR_URL")
        self.mailinator_token = self.generic.get_env_var_info("MAILINATOR_TOKEN")        
        self.connection = self.generic.get_connect_info()
        self.config_info = self.generic.get_api_info()
        self.role_mapping = self.config_info["rbac_role_identifier"]
        
    def retrieve_mailinator_password_reset_token(self, email):
        ## resource group creation
        try:
            messages = self.retrieve_mailinator_emails(email=email)
            filtered_emails_ids = self.filter_required_emails( mails=messages, 
                        subject_pattern=r"Invitation to join the account .* on Harness")
            message_links = self.retrieve_mailinator_message_links(
                                        email=email, message_id=filtered_emails_ids[0])
            reset_passsword_link = self.retrieve_reset_password_link(message_links=message_links)
            url_end_string = reset_passsword_link.split("?")[1]
            url_params = url_end_string.split("&")
            filtered_strings = [string for string in url_params if string.startswith("token")]
            return filtered_strings[0].split("=")[1]
        except Exception as e:
            LOG.error(f" ===== retrieve_mailinator_password_reset_token: Error occurred. Error: {e}")
            return ""
    

    def retrieve_mailinator_emails(self, email, limit=1):
        try:
            time.sleep(10) ## wait time - 10 sec
            url = self.mailinator_url + self.config_info["mailinator_inboxes"] + "/" + email
            url = url + "?token=" + self.mailinator_token + "&limit=" + str(limit) + "&sort=descending"
            resp = requests.request("GET", url, headers={}, data={})
            messages = json.loads(resp.text)
            return messages["msgs"]
        except Exception as e:
            LOG.error(f" ===== retrieve_mailinator_emails: Error occurred. Error: {e}")
            return []


    def filter_required_emails(self, mails, subject_pattern, req_column="id"):
        try:
            messages_df = pd.json_normalize(mails)
            filtered_df = messages_df[messages_df['subject'].str.contains(subject_pattern, regex=True, case=False)]
            if req_column:
                return filtered_df[req_column].values.tolist()
            return filtered_df
        except Exception as e:
            LOG.error(f" ===== filter_required_emails: Error occurred. Error: {e}")
            return []


    def retrieve_mailinator_message_content(self, email, message_id, limit=1):
        try:
            url = self.mailinator_url + self.config_info["mailinator_inboxes"] + "/" + email
            url = url + "/messages/" + message_id
            url = url + "?token=" + self.mailinator_token + "&limit=" + str(limit) + "&sort=descending"
            resp = requests.request("GET", url, headers={}, data={})
            return json.loads(resp.text)
        except Exception as e:
            LOG.error(f" ===== retrieve_message_content: Error occurred. Error: {e}")
            return {}

    def retrieve_mailinator_message_links(self, email, message_id, limit=1):
        try:
            url = self.mailinator_url + self.config_info["mailinator_inboxes"] + "/" + email
            url = url + "/messages/" + message_id + "/links"
            url = url + "?token=" + self.mailinator_token + "&limit=" + str(limit) + "&sort=descending"
            resp = requests.request("GET", url, headers={}, data={})
            assert resp.status_code == 200, "unable to retrieve message links"
            resp_text = json.loads(resp.text)
            return resp_text["links"]
        except Exception as e:
            LOG.error(f" ===== retrieve_mailinator_message_links: Error occurred. Error: {e}")
            return {}

    def retrieve_reset_password_link(self, message_links):
        try:
            for each_link in message_links:
                parsed_url = self.retrieve_redirect_url(url=each_link)
                if "token" in parsed_url:
                    return parsed_url
        except Exception as e:
            LOG.error(f" ===== filter_required_emails: Error occurred. Error: {e}")
            return ""
        return ""

    def retrieve_redirect_url(self, url):
        try:
            context = ssl._create_unverified_context()
            response = urllib.request.urlopen(url, context=context)
            return response.geturl()
        except Exception as e:
            LOG.error(f" ===== retrieve_redirect_email: Error occurred. Error: {e}")
            return ""
