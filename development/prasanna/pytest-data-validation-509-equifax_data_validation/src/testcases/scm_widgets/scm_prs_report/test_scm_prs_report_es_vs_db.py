import json
import logging
import pytest

from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestScmPrsReportESvsDB:
    generic_object = TGhelper()
    across_type = ["creator","repo_id"]

    @pytest.mark.scmsanity
    @pytest.mark.run(order=1)
    @pytest.mark.parametrize("across_type", sorted(across_type))
    def test_scm_prs_report_es_vs_db_sanity(self, across_type, create_generic_object,
                            create_customer_object, get_integration_obj, reports_datetime_utils_object):
        interval_val = ""

        epoch_daterange, daterange = reports_datetime_utils_object.get_last_month_epochdate_and_date_range()
        org_id = (create_generic_object.env["set_ous"])[0]
        gt,lt = epoch_daterange
        report_payload = {
            "filter":{"sort_xaxis":"value_high-low","integration_ids":get_integration_obj,
                    "pr_created_at":{"$gt":gt,"$lt":lt},
                    "code_change_size_unit":"files",
                    "code_change_size_config":{"small":"50","medium":"150"},
                    "comment_density_size_config":{"shallow":"1","good":"5"}},
            "across":across_type,"sort":[{"id":"count","desc":True}],"ou_ids":[org_id]}
        base_url = create_generic_object.connection["base_url"] + create_generic_object.api_data["scm_prs-report"]
        db =  base_url + "?there_is_no_cache=true&force_source=db"
        es =  base_url + "?there_is_no_cache=true&force_source=es"

        ## dynamic payload in case of across with interval
        if "-" in across_type:
            across = across_type.split("-")
            across_type = across[0]
            interval_val = across[1]
        if len(interval_val) != 0:
            report_payload["interval"] = interval_val
        LOG.info("payload {} ".format(json.dumps(report_payload)))
        ## es and db comparison
        es_response = create_generic_object.execute_api_call(es, "post", data=report_payload)
        db_response = create_generic_object.execute_api_call(db, "post", data=report_payload)
        flag, zero_flag = create_customer_object.comparing_es_vs_db_without_prefix(es_response, db_response,
                        columns=['key', 'count'], 
                        unique_id="SCM PRs Report"+across_type)
        assert flag, "ES v/s db data is not matching"

