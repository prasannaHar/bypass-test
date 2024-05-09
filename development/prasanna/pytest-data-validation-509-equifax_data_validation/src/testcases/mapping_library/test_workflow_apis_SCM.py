import json
import logging
import datetime
from collections import OrderedDict
from copy import deepcopy

import pytest
from dateutil.relativedelta import relativedelta

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

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=1)
    def test_SCM_deploy_freq_change_fail_rate_pr_merged_at(self, scm_int, create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="pr_merged_at",
                                                      deployment_criteria="pr_merged",
                                                      deployment_route="pr")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=2)
    def test_SCM_deploy_freq_change_fail_rate_pr_closed_at(self, scm_int, create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="pr_closed_at",
                                                      deployment_criteria="pr_closed",
                                                      deployment_route="pr")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=3)
    def test_SCM_deploy_freq_change_fail_rate_pr_merged_closed(self, scm_int, create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="pr_merged_at",
                                                      deployment_criteria="pr_merged_closed",
                                                      deployment_route="pr")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=4)
    def test_SCM_deploy_freq_change_fail_rate_pr_merged_closed_calcfield_pr_closed_at(self, scm_int,
                                                                                      create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="pr_closed_at",
                                                      deployment_criteria="pr_merged_closed",
                                                      deployment_route="pr")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=5)
    def test_SCM_deploy_freq_change_fail_rate_commit_merged_to_branch(self, scm_int,
                                                                      create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="commit_pushed_at",
                                                      deployment_criteria="commit_merged_to_branch",
                                                      deployment_route="commit")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=6)
    def test_SCM_deploy_freq_change_fail_rate_commit_with_tag(self, scm_int,
                                                              create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="committed_at",
                                                      deployment_criteria="commit_with_tag",
                                                      deployment_route="commit")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=7)
    def test_SCM_deploy_freq_change_fail_rate_commit_merged_to_branch_with_tag_commit_pushed_at(self, scm_int,
                                                                                                create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="commit_pushed_at",
                                                      deployment_criteria="commit_merged_to_branch_with_tag",
                                                      deployment_route="commit")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=8)
    def test_SCM_deploy_freq_change_fail_rate_commit_merged_to_branch_with_tag_committed_at(self, scm_int,
                                                                                            create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="committed_at",
                                                      deployment_criteria="commit_merged_to_branch_with_tag",
                                                      deployment_route="commit")

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=9)
    def test_SCM_deploy_freq_change_fail_rate_commit_merged_to_branch_with_tag_committed_at_with_isabsolute(self,
                                                                                                            scm_int,
                                                                                                            create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="committed_at",
                                                      deployment_criteria="commit_merged_to_branch_with_tag",
                                                      deployment_route="commit", is_absolute=True)

    @pytest.mark.parametrize("scm_int", scm_int_list)
    @pytest.mark.regression
    @pytest.mark.run(order=10)
    def test_SCM_deploy_freq_change_fail_rate_pr_merged_at_with_isabsolute(self, scm_int,
                                                                           create_mapping_library_object,create_api_reusable_funct_object):
        create_mapping_library_object.cfr_dora_scm_tc(create_api_reusable_funct_object,scm_int, calculation_field="pr_merged_at",
                                                      deployment_criteria="pr_merged",
                                                      deployment_route="pr", is_absolute=True)
