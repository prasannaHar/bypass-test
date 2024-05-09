import logging
import pytest
import random

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)
project_id = 14


class TestScmPrsReport:
    @pytest.mark.run(order=1)
    def test_scm_prs_report_001(self, create_scm_prs_report_object, get_integration_obj):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        create_scm_files_report = create_scm_prs_report_object.scm_prs_report(
            integration_id=get_integration_obj)

        assert create_scm_files_report, "widget is not created"

    @pytest.mark.run(order=2)
    def test_scm_prs_report_002(self, create_scm_prs_report_object, get_integration_obj):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        create_scm_files_report = create_scm_prs_report_object.scm_prs_report(
            integration_id=get_integration_obj)

        assert create_scm_files_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of scm_prs_report with drilldown ====")
        key_value = create_scm_prs_report_object.scm_prs_report(integration_id=get_integration_obj, keys=True)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_prs_report_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj,
                key=eachRecord['key'])
            assert eachRecord['count'] == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + eachRecord['key']

    @pytest.mark.run(order=3)
    def test_scm_prs_report_filter_assignee_003(self, create_scm_prs_report_object, get_integration_obj,
                                                create_generic_object):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        get_filter_response_assignee = create_generic_object.get_scm_filter_options(arg_filter_type=["assignee"],
                                                                                    arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_assignee['records'][0]['assignee']]

        assignee_key = []
        print(temp_records_project[0])
        for eachassignee in temp_records_project[0]:
            try:
                if eachassignee['key'] != "NONE":
                    assignee_key.append(eachassignee['key'])
            except:
                continue
        filter = {"assignees": random.sample(assignee_key, min(5, len(assignee_key)))}

        create_scm_files_report = create_scm_prs_report_object.scm_prs_report(
            integration_id=get_integration_obj, var_filters=filter)

        assert create_scm_files_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of scm_prs_report with drilldown ====")
        key_value = create_scm_prs_report_object.scm_prs_report(integration_id=get_integration_obj, keys=True,
                                                                var_filters=filter)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_prs_report_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj, var_filters=filter,
                key=eachRecord['key'])
            assert eachRecord['count'] == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + eachRecord['key']

    @pytest.mark.run(order=4)
    def test_scm_prs_report_filter_repo_id_004(self, create_scm_prs_report_object, get_integration_obj,
                                               create_generic_object):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        get_filter_response_repoid = create_generic_object.get_scm_filter_options(arg_filter_type=["repo_id"],
                                                                                  arg_integration_ids=get_integration_obj)
        temp_records = [get_filter_response_repoid['records'][0]['repo_id']]

        repo_id_key = []
        for eachRepo in temp_records[0]:
            repo_id_key.append(eachRepo['key'])
        filter = {"repo_ids": random.sample(repo_id_key, min(2, len(repo_id_key)))}

        create_scm_files_report = create_scm_prs_report_object.scm_prs_report(
            integration_id=get_integration_obj, var_filters=filter)

        assert create_scm_files_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of scm_prs_report with drilldown ====")
        key_value = create_scm_prs_report_object.scm_prs_report(integration_id=get_integration_obj, keys=True,
                                                                var_filters=filter)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_prs_report_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj,
                key=eachRecord['key'])
            assert eachRecord['count'] == drilldown['_metadata']['total_count'], "Mismatch data in the drill down: " + eachRecord['key']

    @pytest.mark.run(order=5)
    def test_scm_prs_report_filter_pr_created_in_005(self, create_scm_prs_report_object, get_integration_obj,
                                                create_generic_object):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        env_info = create_generic_object.get_env_based_info()
        gt, lt = create_generic_object.get_epoc_utc(value_and_type=env_info['scm_default_time_range'])
        filter = {"pr_created_at":{"$gt":gt,"$lt":lt}}

        create_scm_files_report = create_scm_prs_report_object.scm_prs_report(
            integration_id=get_integration_obj, var_filters=filter)

        assert create_scm_files_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of scm_prs_report with drilldown ====")
        key_value = create_scm_prs_report_object.scm_prs_report(integration_id=get_integration_obj, keys=True,
                                                                var_filters=filter)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_prs_report_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj, var_filters=filter,
                key=eachRecord['key'])
            assert eachRecord['count'] == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + eachRecord['key']

    @pytest.mark.run(order=6)
    def test_scm_prs_report_filter_state_006(self, create_scm_prs_report_object, get_integration_obj,
                                                create_generic_object):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        get_filter_response_assignee = create_generic_object.get_scm_filter_options(arg_filter_type=["state"],
                                                                                    arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_assignee['records'][0]['state']]

        state_key = []
        for eachassignee in temp_records_project[0]:
            state_key.append(eachassignee['key'])
        filter = {"states": random.sample(state_key, min(1, len(state_key)))}

        create_scm_files_report = create_scm_prs_report_object.scm_prs_report(
            integration_id=get_integration_obj, var_filters=filter)

        assert create_scm_files_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of scm_prs_report with drilldown ====")
        key_value = create_scm_prs_report_object.scm_prs_report(integration_id=get_integration_obj, keys=True,
                                                                var_filters=filter)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_prs_report_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj, var_filters=filter,
                key=eachRecord['key'])
            assert eachRecord['count'] == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + eachRecord['key']

    @pytest.mark.run(order=7)
    def test_scm_prs_report_filter_project_007(self, create_scm_prs_report_object, get_integration_obj,
                                             create_generic_object):
        """Validate alignment of scm_prs_report"""

        LOG.info("==== create widget with available filter ====")
        get_filter_response_assignee = create_generic_object.get_scm_filter_options(arg_filter_type=["project"],
                                                                                    arg_integration_ids=get_integration_obj)
        temp_records_project = [get_filter_response_assignee['records'][0]['project']]

        state_key = []
        for eachassignee in temp_records_project[0]:
            state_key.append(eachassignee['key'])
        filter = {"projects": random.sample(state_key, min(1, len(state_key)))}

        create_scm_files_report = create_scm_prs_report_object.scm_prs_report(
            integration_id=get_integration_obj, var_filters=filter)

        assert create_scm_files_report, "widget is not created"

        LOG.info("==== Validate the data in the widget of scm_prs_report with drilldown ====")
        key_value = create_scm_prs_report_object.scm_prs_report(integration_id=get_integration_obj, keys=True,
                                                                var_filters=filter)
        LOG.info("key and total tickets of widget : {}".format(key_value))
        for eachRecord in key_value:
            drilldown = create_scm_prs_report_object.scm_pr_response_time_drilldown(
                integration_id=get_integration_obj, var_filters=filter,
                key=eachRecord['key'])
            assert eachRecord['count'] == drilldown['_metadata'][
                'total_count'], "Mismatch data in the drill down: " + eachRecord['key']