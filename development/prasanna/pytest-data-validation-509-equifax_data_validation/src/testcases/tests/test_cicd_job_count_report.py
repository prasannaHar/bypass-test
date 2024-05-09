import logging
from datetime import datetime, time
import pytest

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

from dateutil.relativedelta import relativedelta
from src.utils.generate_test_cicd_payload import *

temp_project_id = "10182"


class TestCicdJobCount:
    @pytest.mark.run(order=1)
    @pytest.mark.need_maintenance
    def test_create_cicd_job_countreport_widget(self, create_generic_object, widgetreusable_object,get_integration_obj):
        """create cicd job count report widgets"""

        cicd_job_count_single_stat_api_url = create_generic_object.connection["base_url"] + "cicd_scm/job_counts"
        gt, lt = create_generic_object.get_epoc_time(value=7)

        cicd_job_count_widget_payload = generate_create_cicd_job_count_widget_payload(
            arg_req_integration_ids=get_integration_obj,
            arg_endTime={"$gt": gt, "$lt": lt}

        )
        LOG.info(cicd_job_count_widget_payload)
        LOG.info("==== retrieving the widget response ====")
        cicd_job_count_widget_response = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=cicd_job_count_single_stat_api_url,
            arg_req_payload=cicd_job_count_widget_payload,
        )
        data_check_flag = True
        try:
            api_records = (cicd_job_count_widget_response['records'])
            assert len(api_records) > 0, "unable to create the report , No Data Available"
        except:
            data_check_flag = False
        assert data_check_flag is True, "unable to create the report , No Data Available"
