import inspect
import logging
import random
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam
import pytest
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesTrendReport:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="issues_trend_report")

    @pytest.mark.parametrize("param", jira_filter_ticket)
    @pytest.mark.regression
    def test_issue_trend_report(self, create_generic_object, create_issue_trend_object, param):
        custom_value = None
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=15 , type="days")
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
            random_keys = random.sample(list(custom.keys()), min(3, len(list(custom.keys()))) )
            LOG.info("param---".format(param))
            for l in range(0, len(param)):
                if param[l] is None:
                    pass
                elif "custom" in param[l]:
                    param[l] = param[l].replace(param[l], random_keys[count])
                    custom_value = (custom[random_keys[count]])
            count = count + 1

            filter = param[0]
            across = param[1]
            datetime_filters = param[2]
            filter2 = param[3]
            if param[4] is None:
                exclude = None
            elif "custom" in param[4]:
                exclude = "custom-" + param[4]
            else:
                exclude = param[4]
            sprint = param[5]
            sort_x_axis = param[6]
            report = [{"report": "resolution_time_report", "df_name": "resolution_time_report_df"},
                      {"report": "jira_response_time", "df_name": "jira_response_time_df"},
                      {"report": "jira_tickets_report", "df_name": "jira_tickets_report_df"},
                      {"report": "bounce_widget_api_url", "df_name": "bounce_widget_api_url_df"},
                      {"report": "jira_hops_report", "df_name": "jira_hops_report_df"}]

            filter = create_issue_trend_object.filter_creation(filter_type=filter, filter2=filter2,
                                                               integration_id=int_ids, gt=gt, lt=lt,
                                                               custom_values=custom_value, sort_x_axis=sort_x_axis,
                                                               exclude=exclude, datetime_filters=datetime_filters,
                                                               sprint=sprint
                                                               )

            for k in range(0, len(report)):
                report[k]['df_name'] = create_issue_trend_object.widget_creation_issues_trend_report(filter,
                                                                                                     report=report[k][
                                                                                                         'report'],
                                                                                                     ou_id=i,
                                                                                                     sprint_field=sprint_field,
                                                                                                     across=across)

                report[k]["df_name"] = report[k]["df_name"].assign(report=report[k]['report'])
                LOG.info("---{}".format(report[k]["df_name"].head()))

            df1 = pd.merge(report[0]["df_name"], report[1]["df_name"], how='left', on=['key', 'total_tickets'])
            df2 = pd.merge(report[2]["df_name"], report[3]["df_name"], how='left', on=['key', 'total_tickets'])
            df3 = pd.merge(report[4]["df_name"], report[1]["df_name"], how='left', on=['key', 'total_tickets'])
            df4 = pd.merge(report[1]["df_name"], report[2]["df_name"], how='left', on=['key', 'total_tickets'])
            if (len(df1) == len(df2) and len(df3) == len(df4)) is False:
                tc_name = str(inspect.stack()[0][3])
                df1.to_csv(
                    "log_updates/" + tc_name + '.csv', header=True, index=False, mode='a')
                df2.to_csv(
                    "log_updates/" + tc_name + '.csv', header=True, index=False, mode='a')
                df3.to_csv(
                    "log_updates/" + tc_name + '.csv', header=True, index=False, mode='a')
                df4.to_csv(
                    "log_updates/" + tc_name + '.csv', header=True, index=False, mode='a')

                flag.append(f"Dataframes are not matching--ou_id-{i}")

        assert len(flag) == 0, "Dataframes are not matching"
