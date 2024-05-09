import logging
import pytest

from src.lib.generic_helper import generic_helper
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper
from src.testcases.issue_lead_time_by_stage_report.conftest import create_ou_helper_object

from src.utils.OU_helper import Ouhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestIssuesReport:
    generic_object = TGhelper()
    OuHelper = Ouhelper(generic_object)
    ou_ids = generic_object.env['dora_ou_ids']
    ou_filter = OuHelper.ou_id_and_filter_type()

    # LOG.info("ou_filter-----{}".format(ou_filter))
    # breakpoint()
    # ou_ids = ["734"]

    @pytest.mark.parametrize("ou_id, filter_type,type", [(d["ou_ids"], d["filter"], d['type']) for d in ou_filter])
    def test_issues_lead_time_by_stage_ticket_velocity_jira(self, ou_id, filter_type,
                                                            create_issue_lead_time_by_stage_report_object,
                                                            create_widget_helper_object, create_generic_object,
                                                            type):

        # breakpoint()
        if type != "jira":
            pytest.skip("No jira integration present in the given dora ou's")
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt, lt = create_generic_object.get_epoc_time(value=8)
        # gt="1664582400"
        # lt="1681171199"

        lead_time_stages = []
        lead_time_stage_total_dict = []
        lead_time_resp, stages, filter, rating = create_issue_lead_time_by_stage_report_object.new_dora_lead_time_ticket_velocity(
            gt, lt, filter_type, ou_id)
        for i in range(0, len(lead_time_resp['records'])):
            # print("lead_time_resp['records'][i]['key']---",i,lead_time_resp['records'][i]['key'])
            lead_time_stages.append(lead_time_resp['records'][i]['key'])
            lead_time_stage_total_dict.append(
                {"stage": lead_time_resp['records'][i]['key'], "mean": lead_time_resp['records'][i]['mean'],
                 "total_tickets": lead_time_resp['records'][i]['count']})

        LOG.info("lead_time_stages-----{}".format(stages))
        assert set(stages) == set(
            lead_time_stages), "workflow profile stage is not matching with dora lead time stage"
        drilldown_verification_list = []
        # breakpoint()
        for i in stages:
            drilldown_verification, flag_list, drilldown_rating_list = create_issue_lead_time_by_stage_report_object.lead_time_drilldown_verification(
                filter, ou_id, stage=i)
            assert len(flag_list) == 0, "the flag list is not empty-----{}".format(flag_list)
            drilldown_verification_list.append(drilldown_verification)
            LOG.info("drilldown_verification------{}".format(drilldown_verification))
        LOG.info("lead_time_stage_total_dict------{}".format(lead_time_stage_total_dict))
        LOG.info("drilldown_verification list------{}".format(drilldown_verification_list))

        total_mean = 0
        total_stage_mean = 0
        for k in range(0, len(drilldown_verification_list)):
            # LOG.info(drilldown_verification_list[j]['total_of_all_tickets'])
            total_stage_mean = total_stage_mean + drilldown_verification_list[k]['total_stage_mean']
        for i in range(0, len(lead_time_stage_total_dict)):
            total_mean = total_mean + lead_time_stage_total_dict[i]['mean']
            for j in range(0, len(drilldown_verification_list)):
                if lead_time_stage_total_dict[i]['stage'] == drilldown_verification_list[j]['stage']:
                    assert lead_time_stage_total_dict[i]['total_tickets'] == drilldown_verification_list[j][
                        'drilldown_count'], \
                        "drilldown and widget count not matching-----{},{}".format(
                            drilldown_verification_list[j]['drilldown_count'],
                            lead_time_stage_total_dict[i]['total_tickets'])
        LOG.info("mean and total_stage_mean------.{},{}".format(total_mean, total_stage_mean))

        assert round(total_mean) == round(
            total_stage_mean), "total mean from all the stages is not matching the total mean from all the stages drilldown "

        assert len(rating) == 0, "the rating is not matcging-------{}".format(rating)

    @pytest.mark.parametrize("ou_id, filter_type,type", [(d["ou_ids"], d["filter"], d['type']) for d in ou_filter])
    def test_issues_lead_time_by_stage_ticket_velocity_ado(self,
                                                           create_issue_lead_time_by_stage_report_object,
                                                           create_widget_helper_object, create_generic_object, ou_id,
                                                           filter_type, type):

        # ou_id="324"
        # filter_type="fix_version"
        # type="azure_devops"
        # breakpoint()
        if type != "azure_devops":
            pytest.skip("No ado integration present in the given dora ou's")
        # gt, lt = create_generic_object.get_epoc_time(value=8)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt="1664582400"
        # lt="1681171199"

        lead_time_stages = []
        lead_time_stage_total_dict = []
        lead_time_resp, stages, filter, rating = create_issue_lead_time_by_stage_report_object.new_dora_lead_time_ticket_velocity(
            gt, lt, filter_type, ou_id)
        for i in range(0, len(lead_time_resp['records'])):
            # print("lead_time_resp['records'][i]['key']---",i,lead_time_resp['records'][i]['key'])
            lead_time_stages.append(lead_time_resp['records'][i]['key'])
            lead_time_stage_total_dict.append(
                {"stage": lead_time_resp['records'][i]['key'], "mean": lead_time_resp['records'][i]['mean'],
                 "total_tickets": lead_time_resp['records'][i]['count']})

        LOG.info("lead_time_stages-----{}".format(stages))
        assert set(stages) == set(
            lead_time_stages), "workflow profile stage is not matching with dora lead time stage"
        drilldown_verification_list = []
        # breakpoint()
        for i in stages:
            drilldown_verification, flag_list, drilldown_rating_list = create_issue_lead_time_by_stage_report_object.lead_time_drilldown_verification(
                filter, ou_id, stage=i)
            assert len(flag_list) == 0, "the flag list is not empty-----{}".format(flag_list)
            drilldown_verification_list.append(drilldown_verification)
            LOG.info("drilldown_verification------{}".format(drilldown_verification))
        LOG.info("lead_time_stage_total_dict------{}".format(lead_time_stage_total_dict))
        LOG.info("drilldown_verification list------{}".format(drilldown_verification_list))

        total_mean = 0
        total_stage_mean = 0
        for k in range(0, len(drilldown_verification_list)):
            # LOG.info(drilldown_verification_list[j]['total_of_all_tickets'])
            total_stage_mean = total_stage_mean + drilldown_verification_list[k]['total_stage_mean']
        for i in range(0, len(lead_time_stage_total_dict)):
            total_mean = total_mean + lead_time_stage_total_dict[i]['mean']
            for j in range(0, len(drilldown_verification_list)):
                if lead_time_stage_total_dict[i]['stage'] == drilldown_verification_list[j]['stage']:
                    assert lead_time_stage_total_dict[i]['total_tickets'] == drilldown_verification_list[j][
                        'drilldown_count'], \
                        "drilldown and widget count not matching-----{},{}".format(
                            drilldown_verification_list[j]['drilldown_count'],
                            lead_time_stage_total_dict[i]['total_tickets'])
        LOG.info("mean and total_stage_mean------.{},{}".format(total_mean, total_stage_mean))

        assert round(total_mean) == round(
            total_stage_mean), "total mean from all the stages is not matching the total mean from all the stages drilldown "

        assert len(rating) == 0, "the rating is not matcging-------{}".format(rating)
        assert len(drilldown_rating_list) == 0, "the issue is in drilldown rating list------{}".format(
            drilldown_rating_list)

    @pytest.mark.parametrize("ou_id", ou_ids)
    @pytest.mark.regression
    def test_issues_lead_time_by_stage_ticket_velocity_none(self, ou_id,
                                                            create_issue_lead_time_by_stage_report_object,
                                                            create_widget_helper_object, create_generic_object):

        # gt, lt = create_generic_object.get_epoc_time(value=6)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        filter_type = "projects"
        lead_time_stages = []
        lead_time_stage_total_dict = []
        lead_time_resp, stages, filter, rating = create_issue_lead_time_by_stage_report_object.new_dora_lead_time_ticket_velocity(
            gt, lt, filter_type, ou_id)
        for i in range(0, len(lead_time_resp['records'])):
            # print("lead_time_resp['records'][i]['key']---",i,lead_time_resp['records'][i]['key'])
            lead_time_stages.append(lead_time_resp['records'][i]['key'])
            lead_time_stage_total_dict.append(
                {"stage": lead_time_resp['records'][i]['key'], "mean": lead_time_resp['records'][i]['mean'],
                 "total_tickets": lead_time_resp['records'][i]['count'],
                 "velocity_stage_result": lead_time_resp['records'][i]['velocity_stage_result']})

        LOG.info("lead_time_stages-----{}".format(stages))
        assert set(stages) == set(
            lead_time_stages), "workflow profile stage is not matching with dora lead time stage"
        drilldown_verification_list = []
        # breakpoint()
        for i in stages:
            drilldown_verification, flag_list, drilldown_rating_list = create_issue_lead_time_by_stage_report_object.lead_time_drilldown_verification(
                filter, ou_id, stage=i)
            assert len(flag_list) == 0, "the flag list is not empty-----{}".format(flag_list)
            drilldown_verification_list.append(drilldown_verification)
            LOG.info("drilldown_verification------{}".format(drilldown_verification))
        LOG.info("lead_time_stage_total_dict------{}".format(lead_time_stage_total_dict))
        LOG.info("drilldown_verification list------{}".format(drilldown_verification_list))

        total_mean = 0
        total_stage_mean = 0
        for k in range(0, len(drilldown_verification_list)):
            # LOG.info(drilldown_verification_list[j]['total_of_all_tickets'])
            total_stage_mean = total_stage_mean + drilldown_verification_list[k]['total_stage_mean']

        for i in range(0, len(lead_time_stage_total_dict)):
            total_mean = total_mean + lead_time_stage_total_dict[i]['mean']
            for j in range(0, len(drilldown_verification_list)):
                if lead_time_stage_total_dict[i]['stage'] == drilldown_verification_list[j]['stage']:
                    assert lead_time_stage_total_dict[i]['total_tickets'] == drilldown_verification_list[j][
                        'drilldown_count'], \
                        "drilldown and widget count not matching-----{},{}".format(
                            drilldown_verification_list[j]['drilldown_count'],
                            lead_time_stage_total_dict[i]['total_tickets'])
        LOG.info("mean and total_stage_mean------.{},{}".format(total_mean, total_stage_mean))

        assert round(total_mean) == round(
            total_stage_mean), "total mean from all the stages is not matching the total mean from all the stages drilldown "

        assert len(rating) == 0, "the rating is not matcging-------{}".format(rating)
        assert len(drilldown_rating_list) == 0, "the issue is in drilldown rating list------{}".format(
            drilldown_rating_list)

    @pytest.mark.parametrize("ou_id, filter_type,type", [(d["ou_ids"], d["filter"], d['type']) for d in ou_filter])
    def test_issues_lead_time_by_stage_ticket_velocity_jira_exclude(self, ou_id, filter_type,
                                                                    create_issue_lead_time_by_stage_report_object,
                                                                    create_widget_helper_object, create_generic_object,
                                                                    type, exclude=True):
        # breakpoint()
        if type != "jira":
            pytest.skip("No jira integration present in the given dora ou's")
        # gt, lt = create_generic_object.get_epoc_time(value=8)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt="1664582400"
        # lt="1681171199"

        lead_time_stages = []
        lead_time_stage_total_dict = []
        lead_time_resp, stages, filter, rating = create_issue_lead_time_by_stage_report_object.new_dora_lead_time_ticket_velocity(
            gt, lt, filter_type, ou_id, exclude)
        for i in range(0, len(lead_time_resp['records'])):
            # print("lead_time_resp['records'][i]['key']---",i,lead_time_resp['records'][i]['key'])
            lead_time_stages.append(lead_time_resp['records'][i]['key'])
            lead_time_stage_total_dict.append(
                {"stage": lead_time_resp['records'][i]['key'], "mean": lead_time_resp['records'][i]['mean'],
                 "total_tickets": lead_time_resp['records'][i]['count']})

        LOG.info("lead_time_stages-----{}".format(stages))
        assert set(stages) == set(
            lead_time_stages), "workflow profile stage is not matching with dora lead time stage"
        drilldown_verification_list = []
        # breakpoint()
        for i in stages:
            drilldown_verification, flag_list, drilldown_rating_list = create_issue_lead_time_by_stage_report_object.lead_time_drilldown_verification(
                filter, ou_id, stage=i)
            assert len(flag_list) == 0, "the flag list is not empty-----{}".format(flag_list)
            drilldown_verification_list.append(drilldown_verification)
            LOG.info("drilldown_verification------{}".format(drilldown_verification))
        LOG.info("lead_time_stage_total_dict------{}".format(lead_time_stage_total_dict))
        LOG.info("drilldown_verification list------{}".format(drilldown_verification_list))

        total_mean = 0
        total_stage_mean = 0
        for k in range(0, len(drilldown_verification_list)):
            # LOG.info(drilldown_verification_list[j]['total_of_all_tickets'])
            total_stage_mean = total_stage_mean + drilldown_verification_list[k]['total_stage_mean']
        for i in range(0, len(lead_time_stage_total_dict)):
            total_mean = total_mean + lead_time_stage_total_dict[i]['mean']
            for j in range(0, len(drilldown_verification_list)):
                if lead_time_stage_total_dict[i]['stage'] == drilldown_verification_list[j]['stage']:
                    assert lead_time_stage_total_dict[i]['total_tickets'] == drilldown_verification_list[j][
                        'drilldown_count'], \
                        "drilldown and widget count not matching-----{},{}".format(
                            drilldown_verification_list[j]['drilldown_count'],
                            lead_time_stage_total_dict[i]['total_tickets'])
        LOG.info("mean and total_stage_mean------.{},{}".format(total_mean, total_stage_mean))

        assert round(total_mean) == round(
            total_stage_mean), "total mean from all the stages is not matching the total mean from all the stages drilldown "

        assert len(rating) == 0, "the rating is not matcging-------{}".format(rating)
        assert len(drilldown_rating_list) == 0, "the issue is in drilldown rating list------{}".format(
            drilldown_rating_list)

    #
    @pytest.mark.parametrize("ou_id, filter_type,type", [(d["ou_ids"], d["filter"], d['type']) for d in ou_filter])
    def test_issues_lead_time_by_stage_ticket_velocity_ado_exclude(self, ou_id, filter_type,
                                                                   create_issue_lead_time_by_stage_report_object,
                                                                   create_widget_helper_object, create_generic_object,
                                                                   type, exclude=True):
        # breakpoint()
        # ou_id="734"
        # filter_type="teams"

        if type != "azure_devops":
            pytest.skip("No ado integration present in the given dora ou's")
        # gt, lt = create_generic_object.get_epoc_time(value=8)
        jira_default_time_range = create_generic_object.env["jira_default_time_range"]
        gt, lt = create_generic_object.get_epoc_time(value=jira_default_time_range[0], type=jira_default_time_range[1])
        # gt="1664582400"
        # lt="1681171199"

        lead_time_stages = []
        lead_time_stage_total_dict = []
        lead_time_resp, stages, filter, rating = create_issue_lead_time_by_stage_report_object.new_dora_lead_time_ticket_velocity(
            gt, lt, filter_type, ou_id, exclude)
        for i in range(0, len(lead_time_resp['records'])):
            # print("lead_time_resp['records'][i]['key']---",i,lead_time_resp['records'][i]['key'])
            lead_time_stages.append(lead_time_resp['records'][i]['key'])
            lead_time_stage_total_dict.append(
                {"stage": lead_time_resp['records'][i]['key'], "mean": lead_time_resp['records'][i]['mean'],
                 "total_tickets": lead_time_resp['records'][i]['count']})

        LOG.info("lead_time_stages-----{}".format(stages))
        assert set(stages) == set(
            lead_time_stages), "workflow profile stage is not matching with dora lead time stage"
        drilldown_verification_list = []
        # breakpoint()
        for i in stages:
            drilldown_verification, flag_list, drilldown_rating_list = create_issue_lead_time_by_stage_report_object.lead_time_drilldown_verification(
                filter, ou_id, stage=i)
            assert len(flag_list) == 0, "the flag list is not empty-----{}".format(flag_list)
            drilldown_verification_list.append(drilldown_verification)
            LOG.info("drilldown_verification------{}".format(drilldown_verification))
        LOG.info("lead_time_stage_total_dict------{}".format(lead_time_stage_total_dict))
        LOG.info("drilldown_verification list------{}".format(drilldown_verification_list))

        total_mean = 0
        total_stage_mean = 0
        for k in range(0, len(drilldown_verification_list)):
            # LOG.info(drilldown_verification_list[j]['total_of_all_tickets'])
            total_stage_mean = total_stage_mean + drilldown_verification_list[k]['total_stage_mean']
        for i in range(0, len(lead_time_stage_total_dict)):
            total_mean = total_mean + lead_time_stage_total_dict[i]['mean']
            for j in range(0, len(drilldown_verification_list)):
                if lead_time_stage_total_dict[i]['stage'] == drilldown_verification_list[j]['stage']:
                    assert lead_time_stage_total_dict[i]['total_tickets'] == drilldown_verification_list[j][
                        'drilldown_count'], \
                        "drilldown and widget count not matching-----{},{}".format(
                            drilldown_verification_list[j]['drilldown_count'],
                            lead_time_stage_total_dict[i]['total_tickets'])
        LOG.info("mean and total_stage_mean------.{},{}".format(total_mean, total_stage_mean))

        assert round(total_mean) == round(
            total_stage_mean), "total mean from all the stages is not matching the total mean from all the stages drilldown "

        assert len(rating) == 0, "the rating is not matcging-------{}".format(rating)
        assert len(drilldown_rating_list) == 0, "the issue is in drilldown rating list------{}".format(
            drilldown_rating_list)
