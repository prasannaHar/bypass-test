import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper
from src.lib.widget_details.widget_helper import TestWidgetHelper

from src.utils.widget_schemas import Schemas

from src.testcases.scm_widgets.PR_Activity.PR_Activity_testcase_helper import TestPrActivityHelper
from src.testcases.scm_widgets.scm_coding_days_report.scm_coding_days_report_testcase_helper import TestScmCodingDaysHelper
from src.testcases.scm_widgets.scm_commit_single_stat.scm_commit_singleStat_testcase_helper import TestScmCommitSingleStatHelper
from src.testcases.scm_widgets.scm_commits_report.scm_commits_report_testcase_helper import TestScmCommitsHelper
from src.testcases.scm_widgets.scm_files_report.scm_files_report_testcase_helper import TestScmFilesHelper
from src.testcases.scm_widgets.scm_pr_lead_time_by_stage.scm_pr_lead_time_by_stage_helper import TestScmPrLeadTimeHelper
from src.testcases.scm_widgets.scm_pr_response_time_report.scm_pr_response_time_report_testcase_helper import TestScmPrResponseTimeHelper
from src.testcases.scm_widgets.scm_prs_report.scm_prs_report_testcase_helper import TestScmPrsHelper
from src.testcases.scm_widgets.scm_review_collaboration_report.scm_review_collaboration_report_testcase_helper import TestScmReviewCollaborationHelper
from src.testcases.scm_widgets.scm_rework_report.scm_rework_report_testcase_helper import TestScmReworkHelper


@pytest.fixture(scope="session", autouse=True)
def create_generic_object():
    generic_obj = TestGenericHelper()
    return generic_obj

@pytest.fixture(scope="session", autouse=True)
def create_widget_helper_object(create_generic_object):
    widget_helper_obj = TestWidgetHelper(create_generic_object)
    return widget_helper_obj


@pytest.fixture(scope="session", autouse=True)
def widget_schema_validation():
    widget_schema_obj = Schemas()
    return widget_schema_obj


@pytest.fixture(scope="session", autouse=True)
def create_pr_activity_object(create_generic_object):
    testdemo_obj = TestPrActivityHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session", autouse=True)
def create_scm_coding_days_object(create_generic_object):
    testdemo_obj = TestScmCodingDaysHelper(create_generic_object)
    return testdemo_obj


@pytest.fixture(scope="session", autouse=True)
def create_scm_commit_singleStat_object(create_generic_object):
    testdemo_obj = TestScmCommitSingleStatHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def create_scm_commits_report_object(create_generic_object):
    testdemo_obj = TestScmCommitsHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session", autouse=True)
def create_scm_files_object(create_generic_object):
    testdemo_obj = TestScmFilesHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(autouse=True)
def create_scm_pr_lead_time_helper_object(create_generic_object):
    testdemo_obj = TestScmPrLeadTimeHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session", autouse=True)
def create_scm_pr_response_time_object(create_generic_object):
    testdemo_obj = TestScmPrResponseTimeHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session", autouse=True)
def create_scm_prs_report_object(create_generic_object):
    testdemo_obj = TestScmPrsHelper(create_generic_object)
    return testdemo_obj


@pytest.fixture(scope="session", autouse=True)
def create_scm_review_collaboration_object(create_generic_object):
    testdemo_obj = TestScmReviewCollaborationHelper(create_generic_object)
    return testdemo_obj

@pytest.fixture(scope="session", autouse=True)
def create_scm_rework_object(create_generic_object):
    testdemo_obj = TestScmReworkHelper(create_generic_object)
    return testdemo_obj
