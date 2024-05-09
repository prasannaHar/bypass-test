import inspect
import logging
import numpy as np
import pandas as pd
import pytest
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.utils.generate_scm_coding_days_payload import generate_scm_coding_days_widget_payload
from src.lib.core_reusable_functions import epoch_timeStampsGenerationForRequiredTimePeriods as TPhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestCommitsReport:
    generic_object = TGhelper()
    time_period = generic_object.api_data["time_period"]
    trellis_ou_id = generic_object.env["trellis_ou_ids"]
    trellis_intervals = generic_object.env["trellis_intervals"]


    @pytest.mark.run(order=1)
    @pytest.mark.dataaccuracy
    @pytest.mark.parametrize("time_period", time_period)
    def test_commit_report(self, time_period, create_dataaccuracy_object, create_generic_object):
        int_value = ""
        df_sprint = create_generic_object.env["set_ous"]
        # df_sprint=['7']
        # ou_user_filter = create_generic_object.env["ou_user_filter_designation"]
        dropdown_value = []
        zero_list = []
        list_not_match = []
        not_executed_list = []
        gt_created, lt_created = create_generic_object.get_epoc_time(value=int(time_period), type="days")
        for val in df_sprint:
            try:
                int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=val)
                custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=val)
                custom_fields = list(custom_fields_set)

                for k in custom_fields:
                    if "Sprint" in k:
                        custom_sprint = k.split("-")
                        sprint_field = custom_sprint[1]

                scm_customfield = {"sprint": [sprint_field]}
                scm_customfield['github'] = ["committer"]
                scm_payload = create_dataaccuracy_object.accuracy_payload(
                    agg_type="total",
                    time_period=time_period,
                    committed_at={"$gt": gt_created, "$lt": lt_created}, across="trend", ou_ids=val,
                    customfield=scm_customfield, integration_ids=int_ids)

                scmcommit_url = create_generic_object.connection[
                                    "base_url"] + create_generic_object.api_data["scm-commit-single-stat"]
                scmcommit_response = create_generic_object.execute_api_call(scmcommit_url, "post", data=scm_payload)
                if time_period == 1:
                    int_value = "day_of_week"
                elif time_period == 7:
                    int_value = "day"
                elif time_period == 14:
                    int_value = "week"
                elif time_period == 30:
                    int_value = "month"

                # breakpoint()
                commiter_payload = create_dataaccuracy_object.accuracy_payload(
                    interval=int_value,
                    committed_at={"$gt": gt_created, "$lt": lt_created}, across="trend", ou_ids=val,
                    integration_ids=int_ids)
                commiter_response = create_generic_object.execute_api_call(scmcommit_url, "post",
                                                                           data=commiter_payload)
                if commiter_response["records"] and scmcommit_response["records"]:
                    rl_df = pd.json_normalize(scmcommit_response['records'])
                    rl_df = rl_df["count"]
                    total_scmcount = rl_df.sum()
                    scm_df=pd.json_normalize(commiter_response["records"])
                    scm_df=scm_df["count"]
                    total_commit_count=scm_df.sum()

                    if int(total_scmcount) != int(total_commit_count):
                        LOG.info("count of the SCM COMMITS SINGLE STAT : {} ".format(total_scmcount))
                        LOG.info(
                            "count of the SCM COMMITS REPORT : {} ".format(commiter_response["records"][0]["count"]))
                        list_not_match.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("count of dropdown values {}".format(dropdown_value))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, " OU is not executed- list is {}".format(
            set(not_executed_list))
        assert len(list_not_match) == 0, "Not Matching List-  {}".format(set(list_not_match))
        assert len(zero_list) == 0, "zero data List-  {}".format(set(zero_list))

    @pytest.mark.run(order=2)
    @pytest.mark.parametrize("ou_id", trellis_ou_id)
    @pytest.mark.parametrize("trellis_interval", trellis_intervals)
    def test_scm_coding_days_vs_individual_raw_stats_coding_days(self, create_dataaccuracy_object,
                                                                 create_generic_object, trellis_interval, ou_id,
                                                                 create_api_reusable_funct_object):

        # trellis_interval = "LAST_MONTH"
        # ou_id = "45"
        no_of_months, gt_created, lt_created = TPhelper(trellis_interval)
        scm_payload = generate_scm_coding_days_widget_payload(arg_gt=str(gt_created), arg_lt=str(lt_created),
                                                              arg_req_dynamic_fiters=[
                                                                  ["metrics", "avg_coding_day_month"],
                                                                  ["sort_xaxis", "value_high-low"]],
                                                              arg_ou_ids=[ou_id], arg_across="author",
                                                              arg_interval="month")
        LOG.info("scm_coding_days_payload---{}".format(scm_payload))
        scm_coding_day_url = create_generic_object.connection["base_url"] + create_generic_object.api_data[
            "scm_coding_days_report"]
        LOG.info("scm_coding_day_url----{}".format(scm_coding_day_url))
        scm_coding_days_response = create_generic_object.execute_api_call(scm_coding_day_url, "post", data=scm_payload)
        # LOG.info("scm_coding_days_response---{}".format(scm_coding_days_response))

        users_payload = {"filter": {"ou_ref_ids": [ou_id], "interval": trellis_interval}}
        LOG.info("users_payload---{}".format(users_payload))
        user_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["individual_raw_stats"]
        LOG.info("user_url----{}".format(user_url))
        user_response = create_generic_object.execute_api_call(user_url, "post", data=users_payload)
        # LOG.info("user_response---{}".format(user_response))

        """Using dataframe compare the 2 apis"""
        if len(scm_coding_days_response['records']) != 0 and len(user_response['records']) != 0:

            for i in range(0, len(user_response['records'])):
                if user_response['records'][i]['interval'] == trellis_interval:
                    user_response_df = pd.json_normalize(user_response['records'][i]['records'])
                    print(user_response_df.head().to_string())

            scm_coding_days_response_df = pd.json_normalize(scm_coding_days_response['records'])
            # print(scm_coding_days_response_df.head().to_string())
            """As the names dont match check for match string by combining 2 dataframes over join 
                        and add match field when atleast one string is present in the name of teh other"""
            scm_coding_days_response_df['join'] = 1
            user_response_df['join'] = 1
            dfFull = scm_coding_days_response_df.merge(user_response_df, on='join').drop('join', axis=1)
            user_response_df.drop('join', axis=1, inplace=True)

            dfFull['match'] = dfFull.apply(
                lambda x: create_api_reusable_funct_object.check_matching_word(words=[x.full_name],
                                                                               sentence=[x.additional_key]),
                axis=1)

            df_new = dfFull[dfFull['match'] == True]
            df_new['raw_stats.Average Coding days per week'] = df_new['raw_stats.Average Coding days per week'].div(
                86400).round()
            df_new_required_columns = df_new[
                ['full_name', 'additional_key', 'raw_stats.Average Coding days per week', 'mean']]
            df_new_required_columns['new'] = np.where(
                (df_new_required_columns['raw_stats.Average Coding days per week'] == df_new_required_columns['mean']),
                "matching", "not_matching")
            df_new = df_new_required_columns[df_new_required_columns['new'] == "not_matching"]
            if len(df_new) != 0:
                df = pd.DataFrame(["**********" + trellis_interval + "-" + ou_id + "***********"])
                df.to_csv("log_updates/" + str(inspect.stack()[0][3]) + '.csv', header=True,
                          index=False, mode='a')
                df_new.to_csv("log_updates/" + str(inspect.stack()[0][3]) + '.csv', header=True,
                              index=False, mode='a')

            assert len(
                df_new) == 0, "Not matching coding days between trellis raw stats user report and coding days report----{}".format(
                df_new.values.tolist())

        elif len(scm_coding_days_response['records']) == 0 and len(user_response['records']) == 0:
            LOG.info(
                "Both the responses donot have data, len(scm_coding_days_response['records'])--{},len(user_response['records'])---{}", )
            assert True

        else:
            assert False, "The len(scm_coding_days_response['records'])---{} and len(user_response['records'])---{}".format(
                len(scm_coding_days_response['records']), len(user_response['records']))
