"""Get agile Raw data required for the calculations from table report"""
import json
import logging
import os
from datetime import datetime
from src.equifax import config as cf


import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import pandas as pd
from dateutil.relativedelta import relativedelta

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class AgileData:

    def __init__(self, generic_helper):
        self.generic = generic_helper

    def read_config_json_data(self):
        path = os.path.join("equifax", "config.json")
        f = open(path)
        data = json.load(f)
        f.close()
        return data

    def compare_last_two_month(self):
        url = self.generic.connection["base_url"] + self.read_config_json_data(
        )['raw_stats_url']
        body = {
            "filter": {
                "ou_ref_ids":
                [self.read_config_json_data()["raw_stat_org_id"]],
                "interval":
                self.read_config_json_data()["time_interval_quarter"]
            }
        }

        resp = self.generic.execute_api_call(request_type="post",
                                             url=url,
                                             data=body)

        pr_df = self.get_number_of_pr(resp)

        json_file_path = 'quater_response.json'

        with open(json_file_path, 'w') as json_file:
            json.dump(resp, json_file, indent=4)
        df = pd.json_normalize((resp["records"][0])["records"])

        return resp

    def get_number_of_pr(self, records):
        res_col = {
            'Number of PRs approved per month': [],
            'Number of PRs per month': [],
            'Average time spent working on Issues': [],
            'Number of stories worked on per month': [],
            'Number of Story Points worked on per month': [],
        }

        for i in records['records'][0]['records']:
            pr_col_obj = {
                'Number of PRs approved per month': 0,
                'Number of PRs per month': 0,
                'Average time spent working on Issues': 0,
                'Number of stories worked on per month': 0,
                'Number of Story Points worked on per month': 0
            }
            isFound = False
            for j in i['raw_stats']:
                if j['name'] in pr_col_obj.keys():
                    if 'count' in j:
                        pr_col_obj[j['name']] = j['count']

            for i in pr_col_obj.keys():

                res_col[i].append(pr_col_obj[i])
        pr_df = pd.DataFrame(res_col)
        return pr_df

    def fuzzy_magic_log(self, df1, df2, columnA, columnB):
        # Create an empty list to store the mappings
        mappings = []

        # Iterate through the values in df1['column_A']
        for value1 in df1[columnA]:
            # Use fuzzywuzzy's process.extractOne to find the best match from df2['column_B']
            match = process.extractOne(str(value1),
                                       df2[columnB],
                                       scorer=fuzz.token_set_ratio)

            # You can set a threshold score to consider a match (e.g., 80)
            if match and match[1] >= 80:
                mappings.append(match[0])
            else:
                mappings.append(None)

        # Create a new column with the mappings
        df1['mapped_column_B'] = mappings

        # Now, df1 will have a new column 'mapped_column_B' containing the best matching values from df2
        return df1

    def get_raw_stats_with_user_data(self):
        url = self.generic.connection["base_url"] + self.read_config_json_data(
        )['raw_stats_url']
        body = {
            "filter": {
                "ou_ref_ids":
                [self.read_config_json_data()["raw_stat_org_id"]],
                "interval": self.read_config_json_data()["time_interval"]
            }
        }

        resp = self.generic.execute_api_call(request_type="post",
                                             url=url,
                                             data=body)
        # json_file_path = 'output_file.json'

        # # Read the JSON file
        # with open(json_file_path, 'r') as file:
        #     json_data = json.load(file)

        # Now, json_data is a Python data structure (typically a dictionary or a list)
        # print(json_data)
        # resp = json_data
        pr_df = self.get_number_of_pr(resp)

        df = pd.json_normalize((resp["records"][0])["records"])
        result_df = pd.concat([df, pr_df], axis=1)

        return result_df

    def get_ou_details_from_ou_category(self):
        url = self.generic.connection["base_url"] + self.read_config_json_data(
        )['org_unit_list']

        page = 0
        has_next = True
        df = pd.DataFrame()
        while has_next:
            body = {
                "page": page,
                "page_size": 100,
                "filter": {
                    "ou_category_id":
                    self.read_config_json_data()['ou_category_id']
                }
            }
            resp = self.generic.execute_api_call(request_type="post",
                                                 url=url,
                                                 data=body)
            df1 = pd.json_normalize(resp['records'], max_level=2)
            df = df.append(df1)
            page = page + 1
            has_next = resp["_metadata"]["has_next"]

        return df

    def get_dora_details(self):

        url = self.generic.connection["base_url"] + self.read_config_json_data(
        )["table_url"]
        resp = self.generic.execute_api_call(url=url, request_type='get')

        #---------------------------
        # Specify the path to your JSON file
        json_file_path = 'dora.json'

        # # Read the JSON file
        # with open(json_file_path, 'r') as file:
        #     json_data = json.load(file)

        # # Now, json_data is a Python data structure (typically a dictionary or a list)
        # # print(json_data)
        # resp = json_data
        #------------------

        df = pd.DataFrame()
        data = resp['rows']
        df = pd.DataFrame(data).T

        df = df.astype(str)
        df[['total_lead_time', 'url']] = df['2'].str.split("]", expand=True)
        df['total_lead_time'] = df['total_lead_time'].str.replace("[",
                                                                  '',
                                                                  regex=False)
        df['url'] = df['url'].str.replace("(", '', regex=False)
        df['url'] = df['url'].str.replace(")", '', regex=False)
        df = df[df['url'].notna()]
        df.rename(columns={
            "0": "tribe",
            "1": "ou_id",
            "2": "Lead Time(Days)",
            "3": "Total Deployment",
            "4": "Average Deployments",
            "5": "Average Deployments_color_code",
            "6": "CFR(%)",
            "7": "MTTR(Minutes)"
        },
                  inplace=True)

        return df

    def get_lead_time_stories(self, ou_id, gt, lt):
        page = 0
        has_next = True
        payload = cf.lead_time_stories_payload(gt, lt)
        payload['page'] = page
        velocity_df1 = pd.DataFrame()
        while has_next:
            LOG.info(f"ou_id----{ou_id}")
            url = self.generic.connection[
                "base_url"] + self.read_config_json_data(
                )["velocity_story_url"]
            payload["ou_ids"] = [ou_id]
            resp = self.generic.execute_api_call(request_type="post",
                                                 url=url,
                                                 data=payload)

            if len(resp['records']) != 0:
                velocity_lead_time_resp_df = pd.json_normalize(resp['records'])
                velocity_df1 = velocity_df1.append(velocity_lead_time_resp_df)

                total_count = resp['_metadata']['total_count']

            has_next = resp['_metadata']['has_next']
            page = page + 1
            payload['page'] = page

        if len(velocity_df1) == 0:
            Sum = 0

        else:
            # breakpoint()
            sum = velocity_df1['velocity_stage_total_time'].sum()
            count_zero_velocity_stage = velocity_df1[
                velocity_df1["velocity_stage_total_time"] ==
                0]['velocity_stage_total_time'].count()
            total_count = total_count - count_zero_velocity_stage
            Sum = (sum / (total_count * 86400))

        data = {"lead_time_total_stories": [Sum], "ou_id": [ou_id]}
        df = pd.DataFrame(data)
        # breakpoint()

        return df

    def get_sprint_raw_data(self, gt, lt):
        url = self.generic.connection["base_url"] + self.read_config_json_data(
        )['agile_raw_data_table_url']
        resp = self.generic.execute_api_call(request_type="get", url=url)

        columns_list = {}
        for j in range(0, len(resp["schema"]['columns'])):
            columns_list[str(j)] = resp["schema"]['columns'][str(j)]['key']
        df = pd.DataFrame()

        keys = list(resp['rows'].keys())
        for k in keys:

            df2 = pd.json_normalize(resp['rows'][str(k)])
            df = df.append(df2)

        df = df.drop(columns=["id", "index"])
        df = df.rename(columns=columns_list)
        df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')

        # Get the current date
        current_date = datetime.now()
        # Calculate the date of the last month
        last_month_date = current_date - relativedelta(months=1)
        # Get the month number of the last month (1 for January, 2 for February, and so on)
        last_month_number = last_month_date.month

        last_month_data = df[df['date'].dt.month ==
                             last_month_number].reset_index()
        filtered_df = last_month_data[last_month_data['ou_id'] == '243']

        last_month_data[['commit_to_done', 'scope_creep', 'predictability', 'sprint_hygiene', 'sprint_velocity']] = \
            last_month_data[
                ['commit_to_done', 'scope_creep', 'predictability', 'sprint_hygiene', 'sprint_velocity']].replace('--',
                                                                                                                  0).apply(
                pd.to_numeric)

        commit_to_done = last_month_data.groupby(
            ['ou_id', "ou_name"])['commit_to_done', 'scope_creep',
                                  'predictability', 'sprint_hygiene',
                                  "sprint_velocity"].mean().reset_index()
        commit_to_done = commit_to_done.rename(
            columns={"sprint_velocity": "sprint_velocity_mean"})
        sprint_velocity = last_month_data.groupby(
            ['ou_id', "ou_name"])['sprint_velocity'].sum().reset_index()
        # breakpoint()
        raw_stats = self.get_raw_stats_with_user_data()
        values_to_filter = ["QE", "Developer", "SRE"]
        mask = raw_stats['ou_attributes.role'].isin(values_to_filter)
        filtered_df = raw_stats[mask]
        count_of_people_by_ous = filtered_df.groupby(
            ['ou_attributes.tribe'])['org_user_id'].count().reset_index()

        result_df = self.fuzzy_magic_log(sprint_velocity,
                                         count_of_people_by_ous, "ou_name",
                                         "ou_attributes.tribe")
        combined_velocity_count_by_people = pd.merge(
            result_df,
            count_of_people_by_ous,
            left_on="mapped_column_B",
            right_on="ou_attributes.tribe",
            how="left")
        combined_velocity_count_by_people['velocity/engg'] = combined_velocity_count_by_people['sprint_velocity'] / \
                                                             combined_velocity_count_by_people['org_user_id']
        combined_main_df = pd.merge(commit_to_done,
                                    combined_velocity_count_by_people,
                                    on=["ou_id"],
                                    how="inner")

        combined_main_df = combined_main_df.rename(
            columns={"org_user_id": "org_user_count"})
        combined_main_df["predictability_range"] = (
            combined_main_df["predictability"] /
            combined_main_df["sprint_velocity_mean"]) * 100
        required_fields_only = round(combined_main_df[[
            'ou_id', 'ou_name_x', 'commit_to_done', 'scope_creep',
            'predictability_range', 'sprint_hygiene', 'sprint_velocity',
            'org_user_count', 'velocity/engg'
        ]])

        ou_ids_list = required_fields_only['ou_id'].to_list()
        # required_fields_only = []
        # ou_ids_list = [379, 324]
        velocity_df = pd.DataFrame()
        for i in ou_ids_list:
            velocity_lead_time_resp_df = self.get_lead_time_stories(
                str(i), gt, lt)
            velocity_df = velocity_df.append(velocity_lead_time_resp_df)
            velocity_df['lead_time_total_stories'] = velocity_df[
                'lead_time_total_stories'].round(1)

        required_fields_only = pd.merge(required_fields_only,
                                        velocity_df,
                                        on=['ou_id'],
                                        how='inner')
        return required_fields_only

    def dev_prod_results(self):
        values_to_filter = ["QE", "Developer", "SRE"]
        raw_stat_df = self.get_raw_stats_with_user_data()
        raw_stat_df = raw_stat_df.fillna(0)
        ou_details = self.get_ou_details_from_ou_category()
        result_df = self.fuzzy_magic_log(raw_stat_df, ou_details,
                                         "ou_attributes.tribe", "name")
        ou_details = ou_details[['id', "name"]]
        merged = pd.merge(result_df,
                          ou_details,
                          left_on='mapped_column_B',
                          right_on="name",
                          how='left')
        # merged = merged[(merged['id'] == '326') & (merged['ou_attributes.role'] == 'Developer')]
        values = {"QE": 2, "Developer": 5, "SRE": 2}
        master_df = pd.DataFrame()
        dev_df = pd.DataFrame()

        # breakpoint()
        for i in values_to_filter:
            condition = (merged['ou_attributes.role'] == i) & (
                merged['Number of PRs per month'] >= values[i])
            condition1 = (merged['ou_attributes.role'] == i) & (merged['id'])

            # Filter the DataFrame based on the condition
            # breakpoint()
            filtered_df = merged[condition].groupby(['id', 'name'
                                                     ]).count().reset_index()
            total_role_based = merged[condition1].groupby(
                ['id', 'name']).count().reset_index()
            merge_filter_df = pd.merge(
                filtered_df[["id", 'Number of PRs per month', 'name']],
                total_role_based[["id", 'Number of PRs per month']],
                on=["id"],
                how='left')
            merge_filter_df[
                'percentage of prs greater than value of roles'] = (
                    filtered_df['org_user_id'] /
                    total_role_based['org_user_id']) * 100
            merge_filter_df['role'] = i
            master_df = master_df.append(merge_filter_df)

            if i == "Developer":
                filtered_df_prs = merged[condition].groupby(
                    ['id', 'name']).agg({
                        'Number of PRs per month':
                        'sum',
                        'Number of PRs approved per month':
                        'sum'
                    }).reset_index()
                filtered_df_prs['pr_approval_comments'] = round((filtered_df_prs['Number of PRs per month'] / \
                                                                 filtered_df_prs['Number of PRs approved per month']) * 100)
                filtered_df_prs['role'] = i
                avg_time = round(merged[condition1].groupby(
                    ['id', 'name'])['Average time spent working on Issues'].
                                 mean()).reset_index()
                avg_time['Average time spent working on Issues'] = round(
                    avg_time['Average time spent working on Issues'] / 86400)
                # breakpoint()
                filtered_df_prs = pd.merge(filtered_df_prs,
                                           avg_time,
                                           on=['id'],
                                           how='left')
                zero_stories = (merged[condition1].groupby(
                    ['id',
                     'name'])['Number of stories worked on per month'].count()
                                ).reset_index()

                zero_stories1 = (merged[(condition1) & (
                    merged['Number of stories worked on per month'] == 0
                )].groupby(
                    ['id',
                     'name'])['Number of stories worked on per month'].count()
                                 ).reset_index()

                zero_stories_total = pd.merge(zero_stories,
                                              zero_stories1,
                                              on=['id'],
                                              how='left')
                zero_stories_total = zero_stories_total.fillna(0)

                zero_stories_total['story_zero_to_total_devs'] = round(
                    (zero_stories_total[
                        'Number of stories worked on per month_y'] /
                     zero_stories_total[
                         'Number of stories worked on per month_x']) * 100)

                filtered_df_prs = pd.merge(filtered_df_prs,
                                           zero_stories_total,
                                           on=['id'],
                                           how='left')
                dev_df = dev_df.append(filtered_df_prs)

        final_df = pd.merge(master_df, dev_df, on=['id', 'role'], how="left")

        # breakpoint()

        dora_df = self.get_dora_details()
        result_df = pd.merge(dora_df,
                             final_df[['id', 'name_x_x']],
                             left_on=['ou_id'],
                             right_on=["id"],
                             how='inner')
        result_df = result_df.dropna(subset=['name_x_x'])
        result_df = result_df[[
            "name_x_x", "Total Deployment", "total_lead_time", "CFR(%)",
            "MTTR(Minutes)"
        ]]

        # "total_lead_time", "Total Deployment", "CFR(%)", "MTTR(Minutes)"

        return final_df[[
            'id', 'name', 'role',
            'percentage of prs greater than value of roles',
            'pr_approval_comments', 'story_zero_to_total_devs',
            'Average time spent working on Issues'
        ]], result_df

        # breakpoint()

    def data_validation(self):
        values_to_filter = ["QE", "Developer", "SRE"]
        raw_stat_df = self.get_raw_stats_with_user_data()
        raw_stat_df = raw_stat_df.fillna(0)
        ou_details = self.get_ou_details_from_ou_category()
        # breakpoint()
        result_df = self.fuzzy_magic_log(raw_stat_df, ou_details,
                                         "ou_attributes.tribe", "name")
        ou_details = ou_details[['id', "name"]]
        merged = pd.merge(result_df,
                          ou_details,
                          left_on='mapped_column_B',
                          right_on="name",
                          how='left')

        zero_prs = merged[(merged['Number of PRs per month'] == 0
                           | merged['Number of PRs per month'].isna())
                          & (merged['ou_attributes.role'] == 'Developer')]
        zero_prs = zero_prs[[
            'full_name', "mapped_column_B", "id", "Number of PRs per month"
        ]]
        zero_stories = merged[
            (merged['Number of Story Points worked on per month'] == 0
             | merged['Number of Story Points worked on per month'].isna())
            & (merged['ou_attributes.role'] == 'Developer')]
        zero_stories = zero_stories[[
            'full_name', "mapped_column_B", "id",
            "Number of Story Points worked on per month"
        ]]
        return zero_prs, zero_stories, result_df
