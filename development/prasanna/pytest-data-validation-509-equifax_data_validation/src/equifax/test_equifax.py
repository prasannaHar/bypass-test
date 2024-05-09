import inspect
import os
from copy import deepcopy

import numpy as np
import pytest
import logging
import json
import pandas as pd

from src.equifax import config as cf
from src.equifax import color_grading as cm

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestApiValidation:
    path = os.path.join("equifax", "config.json")
    f = open(path)
    data = json.load(f)
    f.close()
    tribe_ous = data["tribe_ous"]

    # def highlight_scope_creep(self, val):
    #     if val == '':
    #         color = 'yellow'
    #         return f'background-color: {color}'
    #     else:
    #         color = 'red' if int(val) < 20 else 'limegreen'
    #         return f'background-color: {color}'
    #
    # def highlight_commit_done(self, val):
    #     if val == '':
    #         color = 'yellow'
    #         return f'background-color: {color}'
    #     else:
    #         color = 'red' if int(val) < 70 else 'limegreen'
    #         return f'background-color: {color}'
    #
    # def highlight_predicability_range(self, val):
    #     if val == '':
    #         color = 'yellow'
    #         return f'background-color: {color}'
    #     else:
    #         color = 'red' if int(val) < 30 else 'limegreen'
    #         return f'background-color: {color}'

    # tribe_ous=['376']

    def test_all_product_lines(self, create_generic_object,
                               widgetreusable_object):
        
        print("testting all product liness")
        url = create_generic_object.connection["base_url"] + self.data[
            "table_url"]
        resp = create_generic_object.execute_api_call(url=url,
                                                      request_type='get')
        df = pd.DataFrame()
        flag_list = []
        a, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods(
            "LAST_MONTH")

        for i in range(0, len(resp['rows'])):
            df1 = pd.json_normalize(resp['rows'][str(i)])
            df = df.append(df1, ignore_index=True)

        df = df.astype(str)
        df[['total_lead_time', 'url']] = df['2'].str.split("]", expand=True)
        df['total_lead_time'] = df['total_lead_time'].str.replace("[",
                                                                  '',
                                                                  regex=False)
        df['url'] = df['url'].str.replace("(", '', regex=False)
        df['url'] = df['url'].str.replace(")", '', regex=False)
        df = df[df['url'].notna()]
        for i in df["1"]:
            # breakpoint()
            print("OU_ID", i)
            if i in ["336", "380", "55", "321", "340", "377", "379"]:
                continue
            payload = cf.tribe_level_lead_time_payload(gt, lt)
            payload['ou_ids'] = [i]
            url1 = create_generic_object.connection["base_url"] + self.data[
                "velocity_url"]
            resp_leadtime = create_generic_object.execute_api_call(
                url=url1, data=payload, request_type="post")
            lead_df = pd.DataFrame()
            for j in range(0, len(resp_leadtime['records'])):
                df2 = pd.json_normalize(resp_leadtime['records'][j])
                lead_df = lead_df.append(df2, ignore_index=True)
            mean1 = df2['mean'].mean()
            mean_time_convert = str(round((mean1 / 86400), 1))
            location = df.loc[df['1'] == i].index[0]
            total_lead_time = df.loc[location, 'total_lead_time']
            if mean_time_convert != total_lead_time:
                flag_list.append({
                    "OU_ID": i,
                    "mean_time_calculated": mean_time_convert,
                    "total_lead_time": total_lead_time
                })

        # breakpoint()
        if len(flag_list) != 0:
            df_csv = pd.DataFrame(flag_list)
            df_csv.to_csv("log_updates/" + str(inspect.stack()[0][3]) + '.csv',
                          header=True,
                          index=False,
                          mode='a')
        assert len(
            flag_list
        ) == 0, f"difference found in lead time and table---{flag_list}"

    @pytest.mark.parametrize("i", tribe_ous)
    def test_jira_releases_equifax(self, create_generic_object, i,
                                   widgetreusable_object):
        print("testting all test_jira_releases_equifax")

        release_table_report = self.data["release_table_report"]
        release_table_report_list = self.data["release_table_report_list"]
        jira_list = self.data["jira_list"]
        flag_list = []
        a, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods(
            "LAST_MONTH")
        release_table_report_url = create_generic_object.connection[
            "base_url"] + release_table_report
        release_table_list_url = create_generic_object.connection[
            "base_url"] + release_table_report_list
        jira_list_url = create_generic_object.connection["base_url"] + jira_list
        release_table_payload_for_tribes = cf.release_table_payload_for_tribes(
            gt, lt)
        velocity_list_call_payload_for_tribes = deepcopy(
            cf.velocity_list_call_payload_for_tribes(gt, lt))
        list_payload_release_table = cf.list_payload_release_table(gt, lt)

        release_table_payload_for_tribes['ou_ids'] = [i]
        release_names = []
        resp_release_table = create_generic_object.execute_api_call(
            url=release_table_report_url + "?there_is_no_cache=true",
            data=release_table_payload_for_tribes,
            request_type="post")
        for k in range(0, len(resp_release_table['records'])):
            release_names.append(resp_release_table['records'][k]['name'])

        velocity_list_call_payload_for_tribes["ou_ids"] = [i]
        LOG.info(
            f"velocity_list_call_payload_for_tribes-----{json.dumps(velocity_list_call_payload_for_tribes)}"
        )

        has_next = True
        velocity_df = pd.DataFrame()
        page = 0
        while has_next:
            resp_velocity_list = create_generic_object.execute_api_call(
                url=jira_list_url + "?there_is_no_cache=true",
                data=velocity_list_call_payload_for_tribes,
                request_type="post")
            # LOG.info(f"resp_velocity_list---{resp_velocity_list}")
            has_next = resp_velocity_list['_metadata']['has_next']
            page = page + 1
            velocity_list_call_payload_for_tribes['page'] = page
            velocity_df1 = pd.json_normalize(resp_velocity_list['records'],
                                             max_level=1)
            velocity_df = velocity_df.append(velocity_df1)

        release_df = pd.DataFrame()
        for j in release_names:
            # breakpoint()
            list_payload_release_table["ou_ids"] = [i]
            list_payload_release_table['filter']["fix_versions"] = [j]
            list_payload_release_table['page'] = 0
            page = 0
            has_next = True
            while has_next:
                LOG.info(
                    f"list_payload_release_table-----{json.dumps(list_payload_release_table)}"
                )
                resp_release_table_list = create_generic_object.execute_api_call(
                    url=release_table_list_url + "?there_is_no_cache=true",
                    data=list_payload_release_table,
                    request_type="post")
                page = page + 1
                list_payload_release_table['page'] = page
                has_next = resp_release_table_list['_metadata']['has_next']
                df1 = pd.json_normalize(resp_release_table_list['records'],
                                        max_level=1)
                release_df = release_df.append(df1)

        if len(velocity_df) != 0 and len(release_df) != 0:
            release_key_list = release_df['key'].tolist()
            velocity_key_list = velocity_df['key'].tolist()
            if set(release_key_list) != set(velocity_key_list):
                flag_list.append({
                    "ou_id":
                    i,
                    "Not matching keys between lead time and release table":
                    list(set(release_key_list) ^ set(velocity_key_list))
                })

        elif len(velocity_df) == 0:
            if len(velocity_df) == len(release_df):
                pass
            else:
                flag_list.append({
                    f"there is mismatch len(velocity_df)--{len(velocity_df)} and release_df---{len(release_df)} for the OU --{i}"
                })

        assert len(flag_list) == 0, f"flag list is not empty -----{flag_list}"
        LOG.info("Test case executed successfully")

    def test_sprint_details_equifax(self, create_generic_object,
                                    agile_raw_data_obj, widgetreusable_object):
        # breakpoint()
        a, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods(
            "LAST_MONTH")
        sprint_details = agile_raw_data_obj.get_sprint_raw_data(gt, lt)
        dev_prod, dora_details = agile_raw_data_obj.dev_prod_results()
        breakpoint()
        data = agile_raw_data_obj.compare_last_two_month()
        sprint_details = sprint_details[[
            "ou_name_x", "commit_to_done", "scope_creep",
            "predictability_range", "sprint_hygiene", "sprint_velocity",
            "lead_time_total_stories", "velocity/engg"
        ]].transpose().reset_index()

        sprint_details['Goal'] = [
            "", "70%", "+/- 20%", "+/- 30%", "of Velocity 70%", "", "", ""
        ]
        # breakpoint()

        colums_list = sprint_details.columns.to_list()
        index_pos = colums_list.index('index')
        goal_pos = colums_list.index('Goal')
        new_list = [colums_list[index_pos], colums_list[goal_pos]] + [
            item for item in colums_list if item not in ['index', 'Goal']
        ]

        sprint_details = sprint_details[new_list]
        sprint_details = sprint_details.style.applymap(
            cm.highlight_commit_done, subset=pd.IndexSlice[1, 0:]).applymap(
                cm.highlight_scope_creep,
                subset=pd.IndexSlice[2, 0:]).applymap(
                    cm.highlight_predicability_range,
                    subset=pd.IndexSlice[3, 0:]).applymap(
                        cm.highlight_commit_done, subset=pd.IndexSlice[4, 0:])

        # sprint_details.to_excel("log_updates/" + str(inspect.stack()[0][3]) + '.xlsx', engine='openpyxl', index=False,
        #                         sheet_name="sprint_details" )
        
        dora_details['Total Deployment'] = dora_details[
            'Total Deployment'].astype(float)
        dora_details['total_lead_time'] = dora_details[
            'total_lead_time'].astype(float)
        dora_details['CFR(%)'] = dora_details['CFR(%)'].astype(float)
        dora_details = dora_details.transpose().reset_index()
        
        dora_details['Goal'] = ["", "4", "21", "5 %", "2hrs"]
        dora_columns = dora_details.columns.to_list()
        index_pos = dora_columns.index('index')
        goal_pos = dora_columns.index('Goal')
        new_list_dora = [dora_columns[index_pos], dora_columns[goal_pos]] + [
            item for item in dora_columns if item not in ['index', 'Goal']
        ]
        dora_details = dora_details[new_list_dora]
        

        dora_details = dora_details.style.applymap(
            cm.highlight_dfreq, subset=pd.IndexSlice[1, 1:]).applymap(
                cm.highlight_leadtime, subset=pd.IndexSlice[2, 1:]).applymap(
                    cm.highlight_cfr, subset=pd.IndexSlice[3, 1:]).applymap(
                        cm.highlight_mttr, subset=pd.IndexSlice[4, 1:])

        excel_file = "log_updates/" + str(inspect.stack()[0][3]) + '.xlsx'

        condition = (dev_prod['role'] == 'Developer')
        condition1 = (dev_prod['role'] == 'QE')
        condition2 = (dev_prod['role'] == 'SRE')
        dev_prod_dev = dev_prod[condition]
        dev_prod_QE = dev_prod[condition1]
        dev_prod_SRE = dev_prod[condition2]
        dev_prod_QE = dev_prod_QE[[
            "name", "role", "percentage of prs greater than value of roles"
        ]]
        dev_prod_SRE = dev_prod_SRE[[
            "name", "role", "percentage of prs greater than value of roles"
        ]]
        dev_prod_dev = dev_prod_dev[[
            "name", "role", "percentage of prs greater than value of roles",
            "pr_approval_comments", "story_zero_to_total_devs",
            "Average time spent working on Issues"
        ]]
        dev_prod_dev_T = dev_prod_dev.transpose()
        dev_prod_SRE_T = dev_prod_SRE.transpose()
        dev_prod_QE_T = dev_prod_QE.transpose()
        dev_prod_dev_T = dev_prod_dev_T.rename(
            index={
                "percentage of prs greater than value of roles":
                "percentage of prs greater than value of roles(DEV)"
            })
        dev_prod_SRE_T = dev_prod_SRE_T.rename(
            index={
                "percentage of prs greater than value of roles":
                "percentage of prs greater than value of roles(SRE)"
            })
        dev_prod_QE_T = dev_prod_QE_T.rename(
            index={
                "percentage of prs greater than value of roles":
                "percentage of prs greater than value of roles(QE)"
            })

        dev_prod_dev_T.insert(0, ' ', [
            "Name", "role",
            "percentage of prs greater than value of roles(QE)",
            "pr_approval_comments", 'story_zero_to_total_devs',
            'Average time spent working on Issues'
        ])
        dev_prod_QE_T.insert(0, ' ', [
            "Name", "role QE",
            "percentage of prs greater than value of roles(QE)"
        ])
        dev_prod_SRE_T.insert(0, ' ', [
            "Name", "role SRE",
            "percentage of prs greater than value of roles(SRE)"
        ])

        dev_prod.to_excel("log_updates/" + str(inspect.stack()[0][3]) +
                          '.xlsx',
                          engine='openpyxl',
                          index=False)

        breakpoint()
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:

            # Write the first DataFrame to the first sheet
            sprint_details.to_excel(writer,
                                    sheet_name='sprint_details',
                                    index=False)

            # Write the second DataFrame to the second sheet
            dora_details.to_excel(writer,
                                  sheet_name='dora_details',
                                  index=False)

            dev_prod_dev_T.to_excel(writer,
                                    sheet_name="dev_details",
                                    index=False)
            dev_prod_SRE_T.to_excel(writer, sheet_name="SRE", index=False)
            dev_prod_QE_T.to_excel(writer, sheet_name="QE", index=False)

    def test_data_validation(self, create_generic_object, agile_raw_data_obj):
        print("testing all  test_data_validation")
        zero_prs, zero_stories, result_df = agile_raw_data_obj.data_validation(
        )
        breakpoint()
        agile_raw_data_obj.compare_last_two_month()

        styled_df = result_df.style.applymap(
            cm.style_negative_red,
            subset=['Number of stories worked on per month'
                    ]).applymap(cm.style_negative_red,
                                subset=['Number of PRs per month'])

        excel_file = "log_updates/" + str(inspect.stack()[0][3]) + '.xlsx'
        breakpoint()
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            zero_prs.to_excel(writer, sheet_name='zero_prs', index=False)
            zero_stories.to_excel(writer,
                                  sheet_name='zero_stories',
                                  index=False)
            styled_df.to_excel(writer, sheet_name='raw_data', index=False)
