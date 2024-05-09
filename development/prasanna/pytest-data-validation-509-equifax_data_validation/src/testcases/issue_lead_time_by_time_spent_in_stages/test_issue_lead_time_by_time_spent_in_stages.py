import inspect
import logging
import random

import pandas as pd
import itertools
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestLeadtimeByTimeSpentStagesReport:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="lead_time_by_spent_in_stages")
    generic_object = TGhelper()
    jira_velocity_ids = generic_object.env['jira_velocity']

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_lead_time_by_time_spent_in_stages_001(self, create_issue_lead_time_by_time_spent_in_stages_object,
                                                         create_generic_object):
        """Validate alignment of issue_lead_time_by_time_spent_in_stages"""

        LOG.info("==== create widget with available filter ====")
        # gt, lt = create_generic_object.get_epoc_time(value=2)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolved_at = {"$gt": gt, "$lt": lt}
        create_issue_lead_time_by_time_spent_in_stages = create_issue_lead_time_by_time_spent_in_stages_object.issue_lead_time_by_time_spent_in_stages(
            issue_resolved_at=issue_resolved_at)

        assert create_issue_lead_time_by_time_spent_in_stages, "widget is not created"

    @pytest.mark.run(order=2)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_issue_lead_time_by_time_spent_in_stages_002(self, create_issue_lead_time_by_time_spent_in_stages_object,
                                                         create_generic_object):
        """Validate alignment of issue_lead_time_by_time_spent_in_stages"""
        # gt, lt = create_generic_object.get_epoc_time(value=1)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        issue_resolved_at = {"$gt": gt, "$lt": lt}
        LOG.info("==== create widget with available filter ====")
        create_issue_lead_time_by_time_spent_in_stages = create_issue_lead_time_by_time_spent_in_stages_object.issue_lead_time_by_time_spent_in_stages(
            issue_resolved_at=issue_resolved_at)

        assert create_issue_lead_time_by_time_spent_in_stages, "widget is not created"

        LOG.info("==== Validate the data in the widget of issue_lead_time_by_time_spent_in_stages drilldown  ====")
        key_value = create_issue_lead_time_by_time_spent_in_stages_object.issue_lead_time_by_time_spent_in_stages(
            issue_resolved_at=issue_resolved_at, keys=True)
        LOG.info("key , total tickets, mean  of widget : {}".format(key_value))
        mismatch = []
        for eachRecord in key_value:
            if eachRecord['total_tickets'] != 0:
                drilldown = create_issue_lead_time_by_time_spent_in_stages_object.issue_lead_time_by_time_spent_in_stages_drilldown(
                    key=eachRecord['key'], issue_resolved_at=issue_resolved_at, mean=True)
                if eachRecord['total_tickets'] != drilldown['total_tickets'] or ( 1 <= (eachRecord['mean'] - drilldown['mean']) <= 1 ) :
                    mismatch.append(eachRecord)
        assert len(mismatch) == 0, "Data mismatch For following Keys in Widget: " + str(
            mismatch) + "for Drilldown Response : " + str(drilldown)

    @pytest.mark.parametrize("param", jira_filter_ticket)
    @pytest.mark.regression
    def test_issue_lead_time_by_time_spent_in_stages_1x_params(self, create_generic_object,
                                                               create_issue_lead_time_by_time_spent_in_stages_object,
                                                               param):
        custom_value = None
        jira_velocity_ids = create_generic_object.env['jira_velocity']
        LOG.info("Param----,{}".format(param))
        flag = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        # gt, lt = create_generic_object.get_epoc_time(value=90, type="days")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        count = 0
        for i, j in itertools.zip_longest(ou_id, jira_velocity_ids, fillvalue=jira_velocity_ids[0]):
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
            datetime_filters = param[1]
            filter2 = param[2]
            if param[3] is None:
                exclude = None
            elif "custom" in param[3]:
                exclude = "custom-" + param[3]
            else:
                exclude = param[3]
            sprint = param[4]
            fixed_date_time = param[5]

            # breakpoint()
            flag_list, payload = create_issue_lead_time_by_time_spent_in_stages_object.issue_lead_time_spent_in_stages_1x_widget(
                filter=filter,
                filter2=filter2,
                int_ids=int_ids,
                gt=gt,
                lt=lt,
                custom_values=custom_value,
                exclude=exclude,
                ou_id=i,
                datetime_filters=datetime_filters,
                sprint=sprint,
                sprint_field=sprint_field,
                velocity_config_id=j, fixed_date_time=fixed_date_time
            )

            df = pd.DataFrame(
                {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(payload)],
                 "ou_id": [str(i)]})
            if len(flag_list) != 0:
                df.to_csv(
                    "log_updates/" + str(inspect.stack()[0][3])
                    + '.csv', header=True,
                    index=False, mode='a')
                flag.append(flag_list)

        assert len(flag) == 0, "Flag list is not empty---{}".format(flag_list)



