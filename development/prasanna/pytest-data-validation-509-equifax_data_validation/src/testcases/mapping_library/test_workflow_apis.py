import json
import logging
import datetime
from copy import deepcopy

import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestWorkFlowProfile:
    generic_object = TGhelper()
    workflow_int_ids = generic_object.env["workflow_int_ids"]
    base_url = generic_object.connection["base_url"]
    dora_ou_ids = generic_object.env["dora_ou_ids"]
    list_url = generic_object.api_data["dora_drilldown_list_api"]
    scm_int_list = workflow_int_ids['SCM']
    time_periods = ['LAST_MONTH', "LAST_QUARTER", "LAST_TWO_QUARTERS"]

    @pytest.mark.run(order=1)
    def test_IM_jira_deploy_freq_change_fail_rate(self, create_generic_object, create_mapping_library_object,
                                                  create_api_reusable_funct_object):
        # breakpoint()
        """Post call create a workflow - Validate teh same created workflow using get call
           and finally delete the profile created using del call """
        name = create_api_reusable_funct_object.random_alpha_numeric_string(5)
        jira_int = self.workflow_int_ids['Jira']
        url = self.base_url + create_generic_object.api_data["velocity_config_api"]
        filter_jira = create_generic_object.env["filter_jira"]
        filter_jira_update = create_generic_object.env["filter_jira_update"]
        create_mapping_library_object.IM_deploy_change_failrate_workflow(name=name, filter=filter_jira,
                                                                         integration_id=int(jira_int),
                                                                         failed_deployment_filter=filter_jira,
                                                                         total_deployment_filter=filter_jira, url=url,
                                                                         filter_update=filter_jira_update,
                                                                         failed_deployment_filter_update=filter_jira_update,
                                                                         total_deployment_filter_update=filter_jira_update)

    @pytest.mark.run(order=2)
    @pytest.mark.skip
    def test_IM_ado_deploy_freq_change_fail_rate(self, create_generic_object, create_mapping_library_object,
                                                 create_api_reusable_funct_object):
        # breakpoint()
        """Post call create a workflow - Validate teh same created workflow using get call
                   and finally delete the profile created using del call """
        name = create_api_reusable_funct_object.random_alpha_numeric_string(5)
        ado_int = self.workflow_int_ids['ADO']
        url = self.base_url + create_generic_object.api_data["velocity_config_api"]
        filter_ado = create_generic_object.env["filter_ado"]
        filter_ado_update = create_generic_object.env["filter_ado_update"]
        create_mapping_library_object.IM_deploy_change_failrate_workflow(name=name, filter=filter_ado,

                                                                         integration_id=int(ado_int),
                                                                         failed_deployment_filter=filter_ado,
                                                                         total_deployment_filter=filter_ado, url=url,
                                                                         filter_update=filter_ado_update,
                                                                         failed_deployment_filter_update=filter_ado_update,
                                                                         total_deployment_filter_update=filter_ado_update)

    @pytest.mark.run(order=3)
    @pytest.mark.skip(reason="this is WIP")
    def test_ado_cicd_using_IM_deploy_freq_change_fail_rate(self, create_generic_object, create_mapping_library_object,
                                                            create_api_reusable_funct_object):
        name = create_api_reusable_funct_object.random_alpha_numeric_string(5)
        ado_ci_IM_int = self.workflow_int_ids['ADO_IM_cicd']
        url = self.base_url + create_generic_object.api_data["velocity_config_api"]
        filter_ado = create_generic_object.env["filter_IM_cicd_ado"]
        filter_ado_update = create_generic_object.env["filter_IM_cicd_ado_update"]
        create_mapping_library_object.IM_deploy_change_failrate_workflow(name=name, filter=filter_ado,
                                                                         integration_id=int(ado_ci_IM_int),
                                                                         failed_deployment_filter=filter_ado,
                                                                         total_deployment_filter=filter_ado, url=url,
                                                                         filter_update=filter_ado_update,
                                                                         failed_deployment_filter_update=filter_ado_update,
                                                                         total_deployment_filter_update=filter_ado_update)

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.skip(reason="this has been moved to test_workflow_apis_SCM")
    def test_SCM_deploy_freq_change_fail_rate(self, scm_int, create_mapping_library_object, create_generic_object,
                                              create_api_reusable_funct_object):
        """Post call create a workflow - Validate teh same created workflow using get call
               and finally delete the profile created using delete call ."""
        flagged_list = []
        LOG.info("**********************")
        name = "auto_" + create_api_reusable_funct_object.random_alpha_numeric_string(5)
        url = self.base_url + create_generic_object.api_data["velocity_config_api"]

        scm_filter = create_mapping_library_object.release_dict(release=['release'], labels=['release'],
                                                                commit_branch=['release'], source_branch=['release'],
                                                                target_branch=['release'])

        failed_deployment_scm_filters = create_mapping_library_object.release_dict(release=["hotfix", "hf"],
                                                                                   labels=["hotfix", "hf"],
                                                                                   commit_branch=["hotfix", "hf"],
                                                                                   source_branch=["hotfix", "hf"],
                                                                                   target_branch=["hotfix", "hf"])

        LOG.info("SCM filter-----{}".format(scm_filter))
        LOG.info("failed_deployment_scm_filters---{}".format(failed_deployment_scm_filters))
        payload = {
            "name": name,
            "default_config": False,
            "description": "this is all in one json for workflow object.",
            "created_at": round(datetime.datetime.now().timestamp()),
            "updated_at": round(datetime.datetime.now().timestamp()),
            "lead_time_for_changes": create_mapping_library_object.lead_time_for_changes(),
            "mean_time_to_restore": create_mapping_library_object.mean_time_to_restore(),
            "deployment_frequency": create_mapping_library_object.deployment_freq_SCM(scm_filters=scm_filter,
                                                                                      int_id=int(scm_int)),
            "change_failure_rate": create_mapping_library_object.change_failure_rate_scm(
                failed_deployment_scm_filters=failed_deployment_scm_filters,
                total_deployment_scm_filter=scm_filter, int_id=int(scm_int)),
            "is_new": True
        }

        payload_change_failurerate = payload["change_failure_rate"]
        # breakpoint()
        del payload_change_failurerate['filters']['integration_type']
        LOG.info("Payload ----{}".format(json.dumps(payload)))
        resp = create_generic_object.execute_api_call(url, "post", data=payload)
        #
        id = resp['id']

        """Use the get call to pull the details of the created id and validate with the i/p"""

        get_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="get")
        if get_resp['name'] != name:
            flagged_list.append("Name is not matching in get and post request")
        if get_resp['deployment_frequency'] != payload['deployment_frequency']:
            flagged_list.append("Deployment request is not matching in get and post response")
        if get_resp['change_failure_rate'] != payload['change_failure_rate']:
            flagged_list.append("Change failure rate not matching get n post response")
        # breakpoint()

        """update the payload by deleting certain fields"""

        del payload['change_failure_rate']['filters']['failed_deployment']['scm_filters']['tags']
        del payload['deployment_frequency']['filters']['deployment_frequency']['scm_filters']['labels']

        payload.update({"id": id})

        LOG.info("Payload ----{}".format(json.dumps(payload)))

        updated_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="put", data=payload)

        """get the response and validate if the update took place"""

        get_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="get")
        #
        if get_resp['deployment_frequency'] != payload['deployment_frequency']:
            flagged_list.append("Deployment Freq in get response not matching to updated payload")

        if get_resp['change_failure_rate'] != payload['change_failure_rate']:
            flagged_list.append("change failure rate is not matching in get and updated post response")

        """Delete teh created profile"""
        resp_del = create_generic_object.execute_api_call(url=url + "/" + id, request_type='delete')
        get_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="get")
        json_resp = json.loads(get_resp.text)
        if json_resp['status'] != 404:
            flagged_list.append("json_resp['status'] is not 404")
        if json_resp['message'] != "Could not find Velocity Config with id=" + id:
            flagged_list.append("json_resp['message'] is not as expected")

        assert len(flagged_list) == 0, "Test case failed with flagged_list failured---{}".format(flagged_list)

    @pytest.mark.run(order=5)
    @pytest.mark.skip(reason="this is WIP")
    def test_cicd_deploy_freq_change_fail_rate(self, create_mapping_library_object, create_generic_object,
                                               create_api_reusable_funct_object):
        """Post call create a workflow - Validate teh same created workflow using get call
               and finally delete the profile created using delete call ."""

        flagged_list = []

        LOG.info("**********************")
        values_list = create_generic_object.env["workflow_values_list_cicd"]
        name = "auto_" + create_api_reusable_funct_object.random_alpha_numeric_string(5)
        cicd_int = self.workflow_int_ids['CICD']
        url = self.base_url + create_generic_object.api_data["velocity_config_api"]

        payload = {
            "name": name,
            "default_config": False,
            "description": "this is all in one json for workflow object.",
            "created_at": round(datetime.datetime.now().timestamp()),
            "updated_at": round(datetime.datetime.now().timestamp()),
            "lead_time_for_changes": create_mapping_library_object.lead_time_for_changes(),
            "mean_time_to_restore": create_mapping_library_object.mean_time_to_restore(),
            "deployment_frequency": create_mapping_library_object.deployment_freq_cicd(int_id=int(cicd_int),
                                                                                       calculation_field="end_time",
                                                                                       values_list=values_list),
            "change_failure_rate": create_mapping_library_object.change_failure_rate_cicd(
                int_id=int(cicd_int), calculation_field="end_time",
                failed_deployment={"values": values_list},
                total_deployment={"values": values_list},
                is_absolute=False),
            "is_new": True
        }
        payload_change_failurerate = payload["change_failure_rate"]
        LOG.info("Payload ----{}".format(json.dumps(payload)))
        resp = create_generic_object.execute_api_call(url, "post", data=payload)
        # LOG.info("resp----{}".format(json.dumps(resp)))
        id = resp['id']

        """Use the get call to pull the details of the created id and validate with the i/p"""

        get_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="get")
        if get_resp['name'] != name:
            flagged_list.append("Name is not matching in get and post request")
        if get_resp['deployment_frequency'] != payload['deployment_frequency']:
            flagged_list.append("Deployment request is not matching in get and post response")
        if get_resp['change_failure_rate'] != payload['change_failure_rate']:
            flagged_list.append("Change failure rate not matching get n post response")

        """update the payload by deleting certain fields"""

        del payload['change_failure_rate']['filters']['total_deployment']
        payload['change_failure_rate']['is_absolute'] = True

        payload['change_failure_rate'].update({'is_absolute': True})
        payload['deployment_frequency'].update(({'calculation_field': "start_time"}))
        LOG.info("Payload ----{}".format(json.dumps(payload)))
        updated_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="put", data=payload)

        """get the response and validate if the update took place"""

        get_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="get")

        if get_resp['deployment_frequency'] != payload['deployment_frequency']:
            flagged_list.append("Deployment Freq in get response not matching to updated payload")

        if get_resp['change_failure_rate'] != payload['change_failure_rate']:
            flagged_list.append("change failure rate is not matching in get and updated post response")

        """Delete teh created profile"""
        resp_del = create_generic_object.execute_api_call(url=url + "/" + id, request_type='delete')
        get_resp = create_generic_object.execute_api_call(url=url + "/" + id, request_type="get")
        json_resp = json.loads(get_resp.text)
        if json_resp['status'] != 404:
            flagged_list.append("json_resp['status'] is not 404")
        if json_resp['message'] != "Could not find Velocity Config with id=" + id:
            flagged_list.append("json_resp['message'] is not as expected")

        assert len(flagged_list) == 0, "Test case failed with flagged_list failured---{}".format(flagged_list)

    @pytest.mark.parametrize("ou_id", dora_ou_ids)
    @pytest.mark.run(order=6)
    @pytest.mark.regression
    @pytest.mark.parametrize("time_period", time_periods)
    def test_change_failure_rate_widget(self, time_period, ou_id, create_mapping_library_object, create_generic_object,
                                        create_widget_reusable_funct_object,
                                        diff_filter_values=None):
        """check the total deployment is in line with teh month week and day with teh associated OU , need to have different OU with different change failure
        rate configs like Jira , SCM and CiCD """
        # ou_id="32796"
        # breakpoint()
        list_url = self.base_url + self.list_url
        # gt, lt = create_generic_object.get_epoc_utc(value=2)

        number, gt, lt = create_widget_reusable_funct_object.epoch_timeStampsGenerationForRequiredTimePeriods_utc(
            arg_required_time_period=time_period)
        # breakpoint()
        date_gt, date_lt, month_date_gt, month_date_lt, last_day_month, first_day_month = create_mapping_library_object.week_date_check(
            gt=gt, lt=lt)
        url = self.base_url + create_generic_object.api_data["dora_change_failure_rate"]
        LOG.info("widget_url______{}".format(url))
        LOG.info("month_date_gt,month_date_lt---{}--{}".format(month_date_gt, month_date_lt))
        filter = create_mapping_library_object.dora_change_failure_rate_filter(gt=gt, lt=lt,
                                                                               diff_filter_values=diff_filter_values)
        payload = {
            "filter": filter,
            "ou_ids": [ou_id]
        }
        LOG.info("widget_payload----{}".format(json.dumps(payload)) + "\n")
        try:
            resp = create_generic_object.execute_api_call(url, "post", data=payload)
            # breakpoint()
            day = resp['time_series']['day']
            week = resp['time_series']['week']
            month = resp['time_series']['month']
            total_deployment = resp['stats']["total_deployment"]

        except Exception as ex:
            LOG.warning("exception---{}".format(ex))
        day_count = 0
        for i in range(0, len(day)):
            day_count = day_count + day[i]['count']
            # breakpoint()
            # assert (int(gt) - 86400) <= day[i]['key'] <= (
            #         int(lt) + 86400), "key of day doesnot fall between the given timeframe"
        print("day_count----", day_count)
        week_count = 0
        week_list = []
        for i in range(0, len(week)):
            week_list.append(week[i]['key'])
            week_count = week_count + week[i]['count']
            # assert date_gt <= week[i]['key'] <= date_lt, "key of week doesnot fall between the given timeframe"
        print("week_count---", week_count)
        month_count = 0
        month_list = []
        for i in range(0, len(month)):
            month_list.append(month[i]['key'])
            month_count = month_count + month[i]['count']
            # assert month_date_gt <= month[i][
            #     'key'] <= month_date_lt, "key of month doesnot fall between the given timeframe"
        print("month_count---", month_count)

        if not (day_count == week_count and week_count == month_count):
            # breakpoint()
            assert False, "day_count / week_count / month_count not matching "

        else:
            # breakpoint()
            if resp['stats']['is_absolute'] is False:
                failure_rate = resp["stats"]['failure_rate']
                if day_count == 0:
                    failure_rate_calculated = 0
                else:
                    failure_rate_calculated = (day_count / total_deployment) * 100
                    if round(failure_rate_calculated) <= 15:
                        # breakpoint()
                        assert resp["stats"][
                                   'band'].lower() == 'Elite'.lower(), "failure_rate_calculated is <=15 but band is not elite "
                    elif 16 <= round(failure_rate_calculated) <= 30:
                        assert resp["stats"][
                                   'band'].lower() == 'High'.lower(), "failure_rate_calculated is 16-30 but band is not high "
                    elif 31 <= round(failure_rate_calculated) <= 45:
                        assert resp["stats"][
                                   'band'].lower() == 'MEDIUM'.lower(), "failure_rate_calculated is 31-45 but band is not medium "
                    else:
                        assert resp["stats"][
                                   'band'].lower() == 'LOW'.lower(), "failure_rate_calculated is above 45 but band is not Low "

                assert round(failure_rate_calculated, 2) == round(failure_rate,
                                                                  2), "Failure rate calculated not matchin with failure rate returned in api"

            else:
                assert total_deployment == day_count, "total deployment is not equal to the count"

        "*****************widget_drilldown Verification**************************"

        """By month drilldown verification with the widget data"""
        # breakpoint()
        drill_down_list = create_mapping_library_object.drill_down_verification_monthly(month, month_list, lt,
                                                                                        ou_id, list_url,
                                                                                        widget="change_failure_rate")
        assert len(drill_down_list) == 0, "mismtach between drilldown and the bar value"

        """By week drilldown verification with the widget data"""

        drill_down_list_week = create_mapping_library_object.drill_down_verification_weekly(week, week_list, lt, gt ,
                                                                                            ou_id, list_url,
                                                                                            widget="change_failure_rate")
        assert len(drill_down_list_week) == 0, "mismtach between drilldown and the bar value"

        LOG.info("********Test Case Exceuted Successfully****************")

    @pytest.mark.parametrize("ou_id", dora_ou_ids)
    @pytest.mark.run(order=7)
    @pytest.mark.regression
    @pytest.mark.parametrize("time_period", time_periods)
    def test_deployment_freq_widget(self, time_period, ou_id, create_mapping_library_object, create_generic_object,
                                    create_widget_reusable_funct_object,
                                    diff_filter_values=None):
        """check the total deployment is in line with teh month week and day with teh associated OU , need to have different OU with different change failure
        rate configs like Jira , SCM and CiCD """
        # ou_id = "21"
        list_url = self.base_url + self.list_url
        # gt, lt = create_generic_object.get_epoc_utc(value=2)
        number, gt, lt = create_widget_reusable_funct_object.epoch_timeStampsGenerationForRequiredTimePeriods_utc(
            arg_required_time_period=time_period)

        date_gt, date_lt, month_date_gt, month_date_lt, last_day_month, first_day_month = create_mapping_library_object.week_date_check(
            gt=gt, lt=lt)
        LOG.info("date_gt-----{}".format(date_gt))
        LOG.info("date_lt-----{}".format(date_lt))
        LOG.info("last_day_month,first_day_month----{}--{}".format(last_day_month, first_day_month))
        url = self.base_url + create_generic_object.api_data["dora_deployment_frequency"]
        time_range = {"$gt": gt, "$lt": lt}
        payload = create_mapping_library_object.dora_deployment_freq_filter([ou_id], time_range,
                                                                           diff_filter_values=diff_filter_values)
        # payload = {
        #     "filter": filter,
        #     "ou_ids": [ou_id]
        # }
        days_from_timeperiod = round((int(lt) - int(gt)) / 86400)

        LOG.info("widget_url******{}".format(url))

        LOG.info("payload----{}".format(json.dumps(payload)))

        try:
            resp = create_generic_object.execute_api_call(url, "post", data=payload)
            print("response---", json.dumps(resp))
            day = resp['time_series']['day']
            week = resp['time_series']['week']
            month = resp['time_series']['month']
            total_deployment = resp['stats']["total_deployment"]
            count_per_day = resp['stats']['count_per_day']
            band = resp["stats"]["band"]

        except Exception as ex:
            LOG.warning("exception---{}".format(ex))

        day_count = 0
        for i in range(0, len(day)):
            day_count = day_count + day[i]['count']
            # assert (int(gt) - 86400) <= day[i]['key'] <= (
            #         int(lt) + 86400), "key of day doesnot fall between the given timeframe"
        print("day_count----", day_count)

        week_count = 0
        week_list=[]
        for i in range(0, len(week)):
            week_list.append( week[i]['key'])
            week_count = week_count + week[i]['count']
            # assert date_gt <= week[i]['key'] <= date_lt, "key of week doesnot fall between the given timeframe"
        print("week_count---", week_count)

        month_count = 0
        month_list=[]
        for i in range(0, len(month)):
            month_list.append(month[i]['key'])
            month_count = month_count + month[i]['count']
            # assert month_date_gt <= month[i][
            #     'key'] <= month_date_lt, "key of month doesnot fall between the given timeframe"
        print("month_count---", month_count)
        # breakpoint()
        if not (day_count == week_count and week_count == month_count):
            assert False, "day_count / week_count / month_count not matching "

        if total_deployment / days_from_timeperiod == 0:

            assert total_deployment == count_per_day, "Total deployment is zero and not matching with count_per_day"

        else:
            assert round((total_deployment / days_from_timeperiod), 2) == round(count_per_day, 2), \
                "total_deployment/days_from_timeperiod is not matching count_per_day"

        """if(perDay > 1)
            return DoraSingleStateDTO.Band.ELITE;

        if (perDay <= 1 && (perDay*7) >= 1)
            return DoraSingleStateDTO.Band.HIGH;

        if((perDay*7) < 1 && (perDay*30) >= 1)
            return  DoraSingleStateDTO.Band.MEDIUM;

        return DoraSingleStateDTO.Band.LOW;"""

        if (total_deployment / days_from_timeperiod) > 1:
            assert band.lower() == "ELITE".lower(), "band should be elite but found as {}".format(band)

        elif (total_deployment / days_from_timeperiod) <= 1 and ((total_deployment / days_from_timeperiod) * 7) >= 1:
            assert band.lower() == "HIGH".lower(), "band should be high but found as {}".format(band)

        elif ((total_deployment / days_from_timeperiod) * 7) < 1 and (
                (total_deployment / days_from_timeperiod) * 30) >= 1:
            assert band.lower() == "MEDIUM".lower(), "band should be medium but found as {}".format(band)

        else:
            assert band.lower() == "LOW".lower(), "band should be low but found as {}".format(band)

        "*****************widget_drilldown Verification**************************"

        """By month drilldown verification with the widget data"""

        drill_down_list = create_mapping_library_object.drill_down_verification_monthly(month, month_list, lt,
                                                                                        ou_id,
                                                                                        list_url,
                                                                                        widget="deployment_frequency_report")
        assert len(drill_down_list) == 0, "mismtach between drilldown and the bar value"
        """By week drilldown verification with the widget data"""
        drill_down_list_week = create_mapping_library_object.drill_down_verification_weekly(week, week_list, lt, gt ,
                                                                                            ou_id, list_url,
                                                                                            widget="deployment_frequency_report")
        assert len(drill_down_list_week) == 0, "mismtach between drilldown and the bar value"
        LOG.info("********Test Case Exceuted Successfully****************")
