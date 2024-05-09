import json
import logging
import random

import pandas as pd
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

agg_type = ["average", "total"]
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmPrsListDrilldownCustomer:
    generic_object = TGhelper()
    across_type = ["creator"]

    @pytest.mark.esvsdbscm
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", sorted(across_type))
    def test_scm_prs_list_drilldown(self, across_type, drilldown_object, create_generic_object,
                                    create_customer_object, get_integration_obj):

        df_sprint = create_generic_object.env["set_ous"]
        zero_list = []
        list_not_match = []
        not_executed_list = []
        interval_val = ""
        for val in df_sprint:
            try:
                if "-" in across_type:
                    across = across_type.split("-")
                    across_type = across[0]
                    interval_val = across[1]

                filters_widget = {"integration_ids": get_integration_obj, "code_change_size_unit": "files",
                                  "code_change_size_config": {"small": "50", "medium": "150"},
                                  "comment_density_size_config": {"shallow": "1", "good": "5"}}

                payload_wigdet = {"filter": filters_widget, "across": across_type,
                                  "ou_ids": [val]}
                if len(interval_val) != 0:
                    payload_wigdet["interval"] = interval_val
                LOG.info("payload_wigdet {} ".format(json.dumps(payload_wigdet)))

                widget_url = create_generic_object.connection[
                                 "base_url"] + create_generic_object.api_data["scm_prs-report"]
                widget_reponse = create_generic_object.execute_api_call(widget_url, "post", data=payload_wigdet)

                ids = []
                for eachrecord in widget_reponse['records']:
                    ids.append(eachrecord['key'])
                for eachId in ids:
                    filters = {"integration_ids": get_integration_obj,
                               "creators": [eachId],
                               "code_change_size_unit": "lines",
                               "code_change_size_config": {"small": "100", "medium": "1000"},
                               "comment_density_size_config": {"shallow": "2000", "good": "5300"}}

                    payload = {"filter": filters, "across": across_type, "ou_ids": [val]}
                    LOG.info("payload_drilldown {} ".format(json.dumps(payload)))

                    es_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_pr_list_drilldown"] + "?there_is_no_cache=true&force_source=es"
                    es_response = create_generic_object.execute_api_call(es_base_url, "post", data=payload)

                    db_base_url = create_generic_object.connection[
                                      "base_url"] + create_generic_object.api_data[
                                      "scm_pr_list_drilldown"] + "?there_is_no_cache=true&force_source=db"
                    db_response = create_generic_object.execute_api_call(db_base_url, "post", data=payload)
                    flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                                                                                               columns=['id', 'project',
                                                                                                        'number',
                                                                                                        'integration_id',
                                                                                                        'creator',
                                                                                                        'merge_sha',
                                                                                                        'title',
                                                                                                        'source_branch',
                                                                                                        'target_branch',
                                                                                                        'state',
                                                                                                        'merged',
                                                                                                        'lines_deleted',
                                                                                                        'lines_changed',
                                                                                                        'files_changed',
                                                                                                        'code_change',
                                                                                                        'comment_density',
                                                                                                        'has_issue_keys',
                                                                                                        'review_type',
                                                                                                        'collab_state',
                                                                                                        'pr_updated_at',
                                                                                                        'pr_merged_at',
                                                                                                        'pr_closed_at',
                                                                                                        'pr_created_at',
                                                                                                        'created_at',
                                                                                                        'avg_author_response_time',
                                                                                                        'avg_reviewer_response_time',
                                                                                                        'approval_time',
                                                                                                        'creator_id',
                                                                                                        'reviewer_count',
                                                                                                        'approver_count',
                                                                                                        'approval_status',
                                                                                                        'repo_id',
                                                                                                        'assignees',
                                                                                                        'assignee_ids',
                                                                                                        'reviewers',
                                                                                                        'reviewer_ids',
                                                                                                        'commenters',
                                                                                                        'commenter_ids',
                                                                                                        'approver_ids',
                                                                                                        'approvers',
                                                                                                        'labels',
                                                                                                        'commit_shas'], unique_id="filter with ou id: {}".format(str(across_type)+" "+ str(val))

                                                                                               )
                    if not flag:
                        list_not_match.append(val)
                    if not zero_flag:
                        zero_list.append(val)
            except Exception as ex:
                not_executed_list.append(val)

        LOG.info("No data list {}".format(set(zero_list)))
        LOG.info("Not match list {}".format(list_not_match))
        LOG.info("not executed List {}".format(set(not_executed_list)))
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(set(not_executed_list))
        assert len(list_not_match) == 0, " Not Matching List-  {}".format(set(list_not_match))

