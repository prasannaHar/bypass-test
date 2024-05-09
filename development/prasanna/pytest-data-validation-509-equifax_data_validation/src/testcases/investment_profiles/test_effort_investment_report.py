import inspect
import logging
import pytest
import random
import pandas as pd
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestEffortInvestmentReport:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="effort_investment_trend_report")

    @pytest.mark.parametrize("param", jira_filter_ticket)
    @pytest.mark.regression
    def test_effort_investment_trend_1x_report_verification(self, create_generic_object, create_effort_helper_object
                                                            ,
                                                            param
                                                            ):
        custom_value = None
        # param = ('priorities', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'custom', 'None', 'month', 'ticket_time', 'ticket_time', 'current_assignee', 'None', 'None', 'None')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        lt = str(int(lt) - (30 * 86400))
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
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys()))))

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

            # breakpoint()
            flag = create_effort_helper_object.effort_investment_trend_report_widget_creation(
                filter_type=filter,
                filter2=filter2,
                int_ids=int_ids,
                gt=gt,
                lt=lt,
                custom_value=custom_value,
                interval=interval,
                exclude=exclude,
                ou_id=i,
                datetime_filters=datetime_filters,
                sprint=sprint,
                sprint_field=sprint_field,
                across=across,
                active_work_unit=active_work_unit,
                effort_unit=effort_unit,
                ba_attribution_mode=ba_attribution_mode,
                ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                ba_completed_work_statuses=ba_completed_work_statuses,
                ticket_categorization_scheme=ticket_categorization_scheme,
                ba_in_progress_statuses=ba_in_progress_statuses,
                dashboard_filter=None)

            if len(flag) != 0:
                LOG.info(f"flag is not empty----{flag}")
                df = pd.DataFrame(
                    {'param': [str(param)], 'flag_list': [str(flag)]})
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')
                flag_list.append(flag)
        assert len(flag_list) == 0, f"the flag list i9s not empty----{flag_list}"

    @pytest.mark.parametrize("param", jira_filter_ticket)
    @pytest.mark.regression
    def test_effort_investment_engineer_1x_report_verification(self, create_generic_object, create_effort_helper_object
                                                               ,
                                                               param
                                                               ):
        custom_value = None
        # param = ('issue_types', 'issue_resolved_at', 'issue_resolved_at', 'projects', 'None', 'None', 'month', 'story_point', 'story_point', 'current_assignee', 'None', 'None', 'None')
        ticket_categorization_scheme = create_generic_object.env["effort_investment_profile_id"]
        LOG.info("Param----,{}".format(param))
        flag_list = []
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
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
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys()))))
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
            active_work_unit = param[7]
            effort_unit = param[8]
            ba_attribution_mode = param[9]
            ba_historical_assignees_statuses = param[10]
            ba_completed_work_statuses = param[11]
            ba_in_progress_statuses = param[12]

            # breakpoint()
            flag = create_effort_helper_object.effort_investment_engineer_report_widget_creation(
                filter_type=filter,
                filter2=filter2,
                int_ids=int_ids,
                gt=gt,
                lt=lt,
                custom_value=custom_value,
                interval=interval,
                exclude=exclude,
                ou_id=i,
                datetime_filters=datetime_filters,
                sprint=sprint,
                sprint_field=sprint_field,
                across=across,
                active_work_unit=active_work_unit,
                effort_unit=effort_unit,
                ba_attribution_mode=ba_attribution_mode,
                ba_historical_assignees_statuses=ba_historical_assignees_statuses,
                ba_completed_work_statuses=ba_completed_work_statuses,
                ticket_categorization_scheme=ticket_categorization_scheme,
                ba_in_progress_statuses=ba_in_progress_statuses, dashboard_filter=None)

            if len(flag) != 0:
                LOG.info(f"flag is not empty----{flag}")
                df = pd.DataFrame(
                    {'param': [str(param)], 'flag_list': [str(flag)]})
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')
                flag_list.append(flag)
        assert len(flag_list) == 0, f"the flag list i9s not empty----{flag_list}"
        LOG.info("Test case executed successfully")
