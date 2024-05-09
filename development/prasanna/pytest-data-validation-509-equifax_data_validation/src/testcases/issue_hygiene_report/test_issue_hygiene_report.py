import inspect
import logging
import random

import pandas as pd
import pytest
from src.utils.retrieve_report_paramaters import ReportTestParametersRetrieve as reportparam
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssueHygiene:
    filter = reportparam()
    jira_filter_ticket = filter.retrieve_widget_test_parameters(report_name="issue_hygiene_report")
    generic_object = TGhelper()

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.parametrize("param", jira_filter_ticket)
    def test_hygiene_report_1x_widget_filters(self, create_generic_object, create_issue_hygiene_object, param):
        custom_value = None
        LOG.info("Param----,{}".format(param))
        flag = []
        not_executed_list = []
        # for param in jira_filter:
        param = [None if x == 'None' else x for x in param]

        ou_id = create_generic_object.env["set_ous"]
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt, lt = create_generic_object.get_epoc_time(value=20, type="days")
        count = 0

        for i in ou_id:
            try:
                int_ids = create_generic_object.get_integrations_based_on_ou_id(ou_id=i)
                custom = create_generic_object.aggs_get_custom_value_with_value(int_ids=int_ids)
                custom_fields_set = create_generic_object.get_aggregration_fields(only_custom=True, ou_id=i)
                custom_fields = list(custom_fields_set)

                for k in custom_fields:
                    if "Sprint" in k:
                        custom_sprint = k.split("-")
                        sprint_field = custom_sprint[1]
                # breakpoint()
                random_keys = random.sample(list(custom.keys()), min( 3, len(list(custom.keys())) ))
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
                hygiene_types = param[5]

                flag_list, widget_payload = create_issue_hygiene_object.create_widget_and_verify_drilldown(
                    filter_type=filter,
                    filter2=filter2,
                    integration_id=int_ids, gt=gt,
                    lt=lt,
                    custom_values=custom_value,
                    exclude=exclude,
                    datetime_filters=datetime_filters,
                    sprint=sprint,
                    hygiene_types=hygiene_types,
                    ou_id=i,
                    sprint_field=sprint_field
                )

                if len(flag_list) != 0:
                    df = pd.DataFrame(
                        {'param': [str(param)], 'flag_list': [str(flag_list)], "widget_payload": [str(widget_payload)]})

                    df.to_csv(
                        "log_updates/" + str(inspect.stack()[0][3])
                        + '.csv', header=True,
                        index=False, mode='a')
                    flag.append(flag_list)

            except Exception as ex:
                LOG.info(f"Exception Occured--{ex}")
                not_executed_list.append(i)

        assert len(flag) == 0, "Flag list is not empty"
        assert len(not_executed_list) == 0, f"OU is not executed check for exception---{not_executed_list}"
