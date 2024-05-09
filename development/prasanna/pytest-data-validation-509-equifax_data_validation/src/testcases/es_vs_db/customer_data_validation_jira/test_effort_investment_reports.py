import inspect
import pandas as pd
import json
import logging
import pytest
import random

# from src.lib.core_reusable_functions import epoch_timeStampsGenerationForRequiredTimePeriods
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestEffortInvestmentReport:
    generic_object = TGhelper()
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="effort_investment_trend_report")
    jira_filter_ticket_df = filter.retrieve_widget_test_parameters(
        report_name="effort_investment_trend_report_dashbrd_filter")

    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("param", jira_filter_ticket)
    def test_effort_investment_trend(self, create_generic_object, create_customer_object,
                                     create_effort_helper_object, widgetreusable_object,
                                     param
                                     ):
        custom_value = None
        # param = ('assignees', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'versions', 'None', 'month', 'ticket_count', 'ticket_count', 'current_assignee', 'None', 'None')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]

        # gt, lt = create_generic_object.get_epoc_time(value=9)
        num, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods("LAST_QUARTER")
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), 3)
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            datetime_filters = param[1]
            across = param[2]
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:
                exclude = "custom-" + param[4]
            else:
                exclude = param[4]

            sprint = param[5]
            interval = param[6]
            active_work_unit = param[7]
            effort_unit = param[8]
            ba_attribution_mode = param[9]
            ba_historical_assignees_statuses = param[10]
            ba_completed_work_statuses = param[11]

            # breakpoint()
            ba_in_progress_statuses = param[12]

            # breakpoint()
            filter = create_effort_helper_object.effort_investment_filter_creation(filter_type=filter,
                                                                                   datetime_filters=datetime_filters,
                                                                                   filter2=filter2, exclude=exclude,
                                                                                   sprint=sprint,
                                                                                   ba_attribution_mode=ba_attribution_mode,
                                                                                   ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                                                                                   ba_completed_work_statuses=ba_completed_work_statuses,
                                                                                   int_ids=int_ids, gt=gt, lt=lt,
                                                                                   custom_values=custom_value,
                                                                                   ticket_categorization_scheme=ticket_categorization_scheme,
                                                                                   ticket_categories=None,
                                                                                   dashboard_filter=None,
                                                                                   ba_in_progress_statuses=ba_in_progress_statuses)

            db_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="db")

            es_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="es")
            # es_df.rename(columns={'total_effort': 'total'}, inplace=True)
            are_identical = db_df.equals(es_df)
            # breakpoint()
            LOG.info(f"db_df-----{db_df}")
            LOG.info(f"es_db----{es_df}")
            try:
                if len(db_df) != 0:
                    if len(es_df) != len(db_df):
                        val1 = pd.merge(db_df, es_df, on=['key', "ticket_categories"], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        flag_list.append(
                            f"The db and es responses are not of same length--db_length-{len(db_df)},es_length-{len(es_df)}")

                    elif not are_identical:
                        val1 = pd.merge(db_df, es_df, on=['key', "ticket_categories"], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        if list(es_df.columns) != list(db_df.columns):
                            LOG.info(
                                f"Column names dont match with db---{list(db_df.columns)} and es---{list(es_df.columns)} for the OU---{i}")
                        # Find the differences between the DataFrames
                        differences = db_df.compare(es_df)
                        print("Differences between the DataFrames:\n", differences)
                        differences.to_csv(
                            "log_updates/" + str(inspect.stack()[0][3])
                            + '.csv', header=True,
                            index=False, mode='a')
                        flag_list.append(f"The db and es responses are not identical for the OU --{i}")

                else:
                    LOG.info("No data returned in DB ")

            except Exception as ex:
                LOG.info(f"Exception occured for the OU---{i},exception ---{ex}")
                flag_list.append({i: ex})

        assert len(flag_list) == 0, f"Flag list is not empty----{flag_list}"

    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("param", jira_filter_ticket)
    def test_effort_investment_single_stat(self, create_generic_object, create_customer_object,
                                           create_effort_helper_object, widgetreusable_object
                                           , param
                                           ):
        custom_value = None
        # param = (
        # 'assignees', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'versions', 'None', 'month', 'ticket_count',
        # 'ticket_count', 'current_assignee', 'None', 'None')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]
        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=9)
        num, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods("LAST_QUARTER")
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), 3)
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            datetime_filters = param[1]
            across = "ticket_category"
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:
                exclude = "custom-" + param[4]
            else:
                exclude = param[4]

            sprint = param[5]
            interval = None
            active_work_unit = None
            effort_unit = param[8]
            ba_attribution_mode = param[9]
            ba_historical_assignees_statuses = None
            ba_completed_work_statuses = param[11]
            ba_in_progress_statuses = param[12]

            # breakpoint()
            filter = create_effort_helper_object.effort_investment_filter_creation(filter_type=filter,
                                                                                   datetime_filters=datetime_filters,
                                                                                   filter2=filter2, exclude=exclude,
                                                                                   sprint=sprint,
                                                                                   ba_attribution_mode=ba_attribution_mode,
                                                                                   ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                                                                                   ba_completed_work_statuses=ba_completed_work_statuses,
                                                                                   int_ids=int_ids, gt=gt, lt=lt,
                                                                                   custom_values=custom_value,
                                                                                   ticket_categorization_scheme=ticket_categorization_scheme,
                                                                                   ticket_categories=None,
                                                                                   dashboard_filter=None,
                                                                                   ba_in_progress_statuses=ba_in_progress_statuses)

            db_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="db")

            es_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="es")
            # es_df.rename(columns={'total_effort': 'total'}, inplace=True)
            are_identical = db_df.equals(es_df)
            # breakpoint()

            LOG.info(f"db_df-----{db_df}")
            LOG.info(f"es_db----{es_df}")

            try:
                if len(db_df) != 0:
                    if len(es_df) != len(db_df):
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        flag_list.append(
                            f"The db and es responses are not of same length--db_length-{len(db_df)},es_length-{len(es_df)}")

                    elif not are_identical:
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        if list(es_df.columns) != list(db_df.columns):
                            LOG.info(
                                f"Column names dont match with db---{list(db_df.columns)} and es---{list(es_df.columns)} for the OU---{i}")
                        # Find the differences between the DataFrames
                        db_df = db_df.sort_values(by='key', ignore_index=True)
                        es_df = es_df.sort_values(by='key', ignore_index=True)
                        differences = db_df.compare(es_df)
                        print("Differences between the DataFrames:\n", differences)
                        differences.to_csv(
                            "log_updates/" + str(inspect.stack()[0][3])
                            + '.csv', header=True,
                            index=False, mode='a')
                        flag_list.append(f"The db and es responses are not identical for the OU --{i}")

                else:
                    LOG.info("No data returned in DB ")

            except Exception as ex:
                LOG.info(f"Exception occured for the OU---{i},exception ---{ex}")
                flag_list.append({i: ex})

        assert len(flag_list) == 0, f"Flag list is not empty----{flag_list}"

    @pytest.mark.parametrize("param", jira_filter_ticket)
    def test_effort_investment_engineer_report_completed_effort(self, create_generic_object, create_customer_object,
                                                                create_effort_helper_object, widgetreusable_object
                                                                , param
                                                                ):
        custom_value = None
        # param = (
        # 'assignees', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'versions', 'None', 'month', 'ticket_count',
        # 'ticket_count', 'current_assignee', 'None', 'None')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]
        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=9)
        num, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods("LAST_QUARTER")

        count = 0

        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), 3)
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            datetime_filters = param[1]
            across = "assignee"
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:
                exclude = "custom-" + param[4]
            else:
                exclude = param[4]

            sprint = param[5]
            interval = None
            active_work_unit = None
            effort_unit = param[8]
            ba_attribution_mode = param[9]
            ba_historical_assignees_statuses = None
            ba_completed_work_statuses = param[11]

            ba_in_progress_statuses = param[12]

            # breakpoint()
            filter = create_effort_helper_object.effort_investment_filter_creation(filter_type=filter,
                                                                                   datetime_filters=datetime_filters,
                                                                                   filter2=filter2, exclude=exclude,
                                                                                   sprint=sprint,
                                                                                   ba_attribution_mode=ba_attribution_mode,
                                                                                   ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                                                                                   ba_completed_work_statuses=ba_completed_work_statuses,
                                                                                   int_ids=int_ids, gt=gt, lt=lt,
                                                                                   custom_values=custom_value,
                                                                                   ticket_categorization_scheme=ticket_categorization_scheme,
                                                                                   ticket_categories=None,
                                                                                   dashboard_filter=None,
                                                                                   ba_in_progress_statuses=ba_in_progress_statuses)

            db_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="db")

            es_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="es")
            # es_df.rename(columns={'total_effort': 'total'}, inplace=True)
            are_identical = db_df.equals(es_df)
            # breakpoint()

            LOG.info(f"db_df-----{db_df}")
            LOG.info(f"es_db----{es_df}")

            try:
                if len(db_df) != 0:
                    if len(es_df) != len(db_df):
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        flag_list.append(
                            f"The db and es responses are not of same length--db_length-{len(db_df)},es_length-{len(es_df)}")

                    elif not are_identical:
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        if list(es_df.columns) != list(db_df.columns):
                            LOG.info(
                                f"Column names dont match with db---{list(db_df.columns)} and es---{list(es_df.columns)} for the OU---{i}")
                        # Find the differences between the DataFrames
                        db_df = db_df.sort_values(by='key', ignore_index=True)
                        es_df = es_df.sort_values(by='key', ignore_index=True)
                        differences = db_df.compare(es_df)
                        print("Differences between the DataFrames:\n", differences)
                        differences.to_csv(
                            "log_updates/" + str(inspect.stack()[0][3])
                            + '.csv', header=True,
                            index=False, mode='a')
                        flag_list.append(f"The db and es responses are not identical for the OU --{i}")

                else:
                    LOG.info("No data returned in DB ")

            except Exception as ex:
                LOG.info(f"Exception occured for the OU---{i},exception ---{ex}")
                flag_list.append({i: ex})

        assert len(flag_list) == 0, f"Flag list is not empty----{flag_list}"

    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("param", jira_filter_ticket_df)
    def test_effort_investment_trend_dashboard_filter(self, create_generic_object, create_customer_object,
                                                      create_effort_helper_object, widgetreusable_object
                                                      ,
                                                      param
                                                      ):
        custom_value = None
        # param = ('None', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'None', 'None', 'month', 'story_point', 'story_point', 'current_assignee', 'None', 'None', 'None', 'custom_field2')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]

        # gt, lt = create_generic_object.get_epoc_time(value=9)
        num, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods("LAST_QUARTER")
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), 3)
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            datetime_filters = param[1]
            across = param[2]
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:

                exclude = "custom-" + param[4]
            else:
                exclude = param[4]

            sprint = param[5]
            interval = param[6]
            active_work_unit = param[7]
            effort_unit = param[8]
            ba_attribution_mode = param[9]
            ba_historical_assignees_statuses = param[10]
            ba_completed_work_statuses = param[11]
            ba_in_progress_statuses = param[12]
            if param[13] is None:
                dashboard_filter = None
            elif "custom" in param[13]:

                dashboard_filter = "custom-" + param[13]
            else:
                dashboard_filter = param[13]
            # breakpoint()
            filter = create_effort_helper_object.effort_investment_filter_creation(filter_type=filter,
                                                                                   datetime_filters=datetime_filters,
                                                                                   filter2=filter2, exclude=exclude,
                                                                                   sprint=sprint,
                                                                                   ba_attribution_mode=ba_attribution_mode,
                                                                                   ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                                                                                   ba_completed_work_statuses=ba_completed_work_statuses,
                                                                                   int_ids=int_ids, gt=gt, lt=lt,
                                                                                   custom_values=custom_value,
                                                                                   dashboard_filter=dashboard_filter,
                                                                                   ticket_categorization_scheme=ticket_categorization_scheme,
                                                                                   ticket_categories=None,
                                                                                   ba_in_progress_statuses=ba_in_progress_statuses)

            # breakpoint()
            db_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="db")

            es_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="es")
            # es_df.rename(columns={'total_effort': 'total'}, inplace=True)
            are_identical = db_df.equals(es_df)
            # breakpoint()
            LOG.info(f"db_df-----{db_df}")
            LOG.info(f"es_db----{es_df}")
            try:
                if len(db_df) != 0:
                    if len(es_df) != len(db_df):
                        val1 = pd.merge(db_df, es_df, on=['key', "ticket_categories"], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        flag_list.append(
                            f"The db and es responses are not of same length--db_length-{len(db_df)},es_length-{len(es_df)}")

                    elif not are_identical:
                        val1 = pd.merge(db_df, es_df, on=['key', "ticket_categories"], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        if list(es_df.columns) != list(db_df.columns):
                            LOG.info(
                                f"Column names dont match with db---{list(db_df.columns)} and es---{list(es_df.columns)} for the OU---{i}")
                        # Find the differences between the DataFrames
                        differences = db_df.compare(es_df)
                        print("Differences between the DataFrames:\n", differences)
                        differences.to_csv(
                            "log_updates/" + str(inspect.stack()[0][3])
                            + '.csv', header=True,
                            index=False, mode='a')
                        flag_list.append(f"The db and es responses are not identical for the OU --{i}")

                else:
                    LOG.info("No data returned in DB ")

            except Exception as ex:
                LOG.info(f"Exception occured for the OU---{i},exception ---{ex}")
                flag_list.append({i: ex})

        assert len(flag_list) == 0, f"Flag list is not empty----{flag_list}"

    @pytest.mark.parametrize("param", jira_filter_ticket_df)
    def test_effort_investment_engineer_report_completed_effort(self, create_generic_object, create_customer_object,
                                                                create_effort_helper_object, widgetreusable_object
                                                                , param
                                                                ):
        custom_value = None
        # param = (
        # 'assignees', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'versions', 'None', 'month', 'ticket_count',
        # 'ticket_count', 'current_assignee', 'None', 'None')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]
        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=9)
        num, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods("LAST_QUARTER")

        count = 0

        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), 3)
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            datetime_filters = param[1]
            across = "assignee"
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:
                exclude = "custom-" + param[4]
            else:
                exclude = param[4]

            sprint = param[5]
            interval = None
            active_work_unit = None
            effort_unit = param[8]
            ba_attribution_mode = param[9]
            ba_historical_assignees_statuses = None
            ba_completed_work_statuses = param[11]
            ba_in_progress_statuses = param[12]
            if param[13] is None:
                dashboard_filter = None
            elif "custom" in param[13]:

                dashboard_filter = "custom-" + param[13]
            else:
                dashboard_filter = param[13]

            # breakpoint()
            filter = create_effort_helper_object.effort_investment_filter_creation(filter_type=filter,
                                                                                   datetime_filters=datetime_filters,
                                                                                   filter2=filter2, exclude=exclude,
                                                                                   sprint=sprint,
                                                                                   ba_attribution_mode=ba_attribution_mode,
                                                                                   ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                                                                                   ba_completed_work_statuses=ba_completed_work_statuses,
                                                                                   int_ids=int_ids, gt=gt, lt=lt,
                                                                                   custom_values=custom_value,
                                                                                   dashboard_filter=dashboard_filter,
                                                                                   ticket_categorization_scheme=ticket_categorization_scheme,
                                                                                   ticket_categories=None,
                                                                                   ba_in_progress_statuses=ba_in_progress_statuses)

            db_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="db")

            es_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="es")
            # es_df.rename(columns={'total_effort': 'total'}, inplace=True)
            are_identical = db_df.equals(es_df)
            # breakpoint()

            LOG.info(f"db_df-----{db_df}")
            LOG.info(f"es_db----{es_df}")

            try:
                if len(db_df) != 0:
                    if len(es_df) != len(db_df):
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        flag_list.append(
                            f"The db and es responses are not of same length--db_length-{len(db_df)},es_length-{len(es_df)}")

                    elif not are_identical:
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        if list(es_df.columns) != list(db_df.columns):
                            LOG.info(
                                f"Column names dont match with db---{list(db_df.columns)} and es---{list(es_df.columns)} for the OU---{i}")
                        # Find the differences between the DataFrames
                        db_df = db_df.sort_values(by='key', ignore_index=True)
                        es_df = es_df.sort_values(by='key', ignore_index=True)
                        differences = db_df.compare(es_df)
                        print("Differences between the DataFrames:\n", differences)
                        differences.to_csv(
                            "log_updates/" + str(inspect.stack()[0][3])
                            + '.csv', header=True,
                            index=False, mode='a')
                        flag_list.append(f"The db and es responses are not identical for the OU --{i}")

                else:
                    LOG.info("No data returned in DB ")

            except Exception as ex:
                LOG.info(f"Exception occured for the OU---{i},exception ---{ex}")
                flag_list.append({i: ex})

        assert len(flag_list) == 0, f"Flag list is not empty----{flag_list}"

    @pytest.mark.esdbfilter
    @pytest.mark.parametrize("param", jira_filter_ticket_df)
    def test_effort_investment_single_stat_dashbrd_filter(self, create_generic_object, create_customer_object,
                                                          create_effort_helper_object, widgetreusable_object
                                                          , param
                                                          ):
        custom_value = None
        # param = (
        # 'assignees', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'versions', 'None', 'month', 'ticket_count',
        # 'ticket_count', 'current_assignee', 'None', 'None')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]
        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=9)
        num, gt, lt = widgetreusable_object.epoch_timeStampsGenerationForRequiredTimePeriods("LAST_QUARTER")
        count = 0
        for i in ou_id:
            int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
            custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
            custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
            custom_fields = list(custom_fields_set)

            for k in custom_fields:
                if "Sprint" in k:
                    custom_sprint = k.split("-")
                    sprint_field = custom_sprint[1]
            # breakpoint()
            random_keys = random.sample(list(custom.keys()), 3)
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            datetime_filters = param[1]
            across = "ticket_category"
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:
                exclude = "custom-" + param[4]
            else:
                exclude = param[4]

            sprint = param[5]
            interval = None
            active_work_unit = None
            effort_unit = param[8]
            ba_attribution_mode = param[9]
            ba_historical_assignees_statuses = None
            ba_completed_work_statuses = param[11]
            ba_in_progress_statuses = param[12]
            if param[13] is None:
                dashboard_filter = None
            elif "custom" in param[13]:

                dashboard_filter = "custom-" + param[13]
            else:
                dashboard_filter = param[13]

            # breakpoint()
            filter = create_effort_helper_object.effort_investment_filter_creation(filter_type=filter,
                                                                                   datetime_filters=datetime_filters,
                                                                                   filter2=filter2, exclude=exclude,
                                                                                   sprint=sprint,
                                                                                   ba_attribution_mode=ba_attribution_mode,
                                                                                   ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                                                                                   ba_completed_work_statuses=ba_completed_work_statuses,
                                                                                   int_ids=int_ids, gt=gt, lt=lt,
                                                                                   custom_values=custom_value,
                                                                                   dashboard_filter=dashboard_filter,
                                                                                   ticket_categorization_scheme=ticket_categorization_scheme,
                                                                                   ticket_categories=None,
                                                                                   ba_in_progress_statuses=ba_in_progress_statuses)

            db_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="db")

            es_df = create_effort_helper_object.effort_investment_payload_generation_for_compare(
                filter=filter,
                interval=interval,
                ou_id=i,
                sprint_field=sprint_field,
                ticket_categorization_scheme=ticket_categorization_scheme,
                across=across,
                effort_unit=effort_unit,
                type_of="es")
            # es_df.rename(columns={'total_effort': 'total'}, inplace=True)
            are_identical = db_df.equals(es_df)
            # breakpoint()

            LOG.info(f"db_df-----{db_df}")
            LOG.info(f"es_db----{es_df}")

            try:
                if len(db_df) != 0:
                    if len(es_df) != len(db_df):
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        flag_list.append(
                            f"The db and es responses are not of same length--db_length-{len(db_df)},es_length-{len(es_df)}")

                    elif not are_identical:
                        val1 = pd.merge(db_df, es_df, on=['key'], how='outer', indicator=True)
                        LOG.info("DB data - {}".format(val1[val1['_merge'] == 'left_only']))
                        if len(val1[val1['_merge'] == 'left_only']) != 0:
                            val1[val1['_merge'] == 'left_only'].to_csv(
                                "log_updates/" + str(inspect.stack()[0][3])
                                + '.csv', header=True,
                                index=False, mode='a')
                        if list(es_df.columns) != list(db_df.columns):
                            LOG.info(
                                f"Column names dont match with db---{list(db_df.columns)} and es---{list(es_df.columns)} for the OU---{i}")
                        # Find the differences between the DataFrames
                        db_df = db_df.sort_values(by='key', ignore_index=True)
                        es_df = es_df.sort_values(by='key', ignore_index=True)
                        differences = db_df.compare(es_df)
                        print("Differences between the DataFrames:\n", differences)
                        differences.to_csv(
                            "log_updates/" + str(inspect.stack()[0][3])
                            + '.csv', header=True,
                            index=False, mode='a')
                        flag_list.append(f"The db and es responses are not identical for the OU --{i}")

                else:
                    LOG.info("No data returned in DB ")

            except Exception as ex:
                LOG.info(f"Exception occured for the OU---{i},exception ---{ex}")
                flag_list.append({i: ex})

        assert len(flag_list) == 0, f"Flag list is not empty----{flag_list}"
