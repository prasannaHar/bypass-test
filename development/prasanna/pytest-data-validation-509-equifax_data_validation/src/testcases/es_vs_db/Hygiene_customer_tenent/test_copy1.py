import json
import logging
import pytest

from src.utils.generate_Api_payload import GenericPayload

api_payload = GenericPayload()

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestCustomerTenent:
    @pytest.mark.run(order=1)
    def test_all_tickets(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate All tickets in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values = []
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match[ou_id] = list_missing_values
                list_not_match[ou_id] = list_missing_values
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        LOG.info("total ou's data is not matching - {}".format(list_not_match))
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    def test_orphan_story(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate to the Orphan Story  in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values =[]
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                payload["filter"]["missing_fields"] = {"customfield_10001": True}
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match.append(ou_id)
                assert es_response == db_response, "missing matching the response for the ou id {}".format(ou_id)
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    def test_missing_SP(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate to the missing SP  in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values = []
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                payload["filter"]["missing_fields"] = {"customfield_10008": True}
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match.append(ou_id)
                assert es_response == db_response, "missing matching the response for the ou id {}".format(ou_id)
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    def test_missing_fix_version(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate to the Missing Fix version  in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values = []
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                payload["filter"]["exclude"] = {
                    "statuses": ["COMPLETE", "CLOSED", "DONE", "RESOLVED", "WON'T FIX", "COMPLETED"],
                    "custom_fields": {}}
                payload["filter"]["missing_fields"] = {"fix_version": True}
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match.append(ou_id)
                assert es_response == db_response, "missing matching the response for the ou id {}".format(ou_id)
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    def test_large_stories(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate to the Large Stories in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values = []
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                payload["filter"]["story_points"] = {"$gt": "5"}
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match.append(ou_id)
                assert es_response == db_response, "missing matching the response for the ou id {}".format(ou_id)
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    def test_poor_description(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate to the POOR_DESCRIPTION in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values = []
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                payload["filter"]["hygiene_types"] = ["POOR_DESCRIPTION"]
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match.append(ou_id)
                assert es_response == db_response, "missing matching the response for the ou id {}".format(ou_id)
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    def test_no_components(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate to the No Components in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values = []
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                payload["filter"]["hygiene_types"] = ["NO_COMPONENTS"]
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match.append(ou_id)
                assert es_response == db_response, "missing matching the response for the ou id {}".format(ou_id)
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)

    def test_ac_missing(self, create_generic_object, create_ou_object, widgetreusable_object, get_integration_obj):
        """Validate to the AC Missing in propels"""

        ou_ids = create_ou_object.get_ou_id(filter="[")
        LOG.info("Total count of the OU - {}".format(len(ou_ids)))
        list_not_match = {}
        not_executed_list = []
        for ou_id in ou_ids[0:30]:
            list_missing_values=[]
            try:
                payload = api_payload.generate_customer_all_ticket(get_integration_obj, ou_id)
                payload["filter"]["missing_fields"] = {"customfield_10117": True}
                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=es"
                es_response = create_generic_object.execute_api_call(base_url, "post", data=payload)

                base_url = create_generic_object.connection[
                               "base_url"] + "jira_issues/tickets_report?there_is_no_cache=true&force_source=db"
                db_response = create_generic_object.execute_api_call(base_url, "post", data=payload)
                for keys in es_response["records"]:
                    if keys["additional_key"] != "_UNASSIGNED_":
                        try:
                            if keys in db_response["records"]:
                                LOG.info("value is present")
                            else:
                                LOG.info("value is not present")
                                list_missing_values.append(keys)
                        except Exception as ex:
                            list_missing_values.append(keys)
                if es_response["count"] == db_response["count"]:
                    LOG.info("This ou - {} is matching".format(ou_id))
                else:
                    list_not_match.append(ou_id)
                assert es_response == db_response, "missing matching the response for the ou id {}".format(ou_id)
            except Exception as ex:
                LOG.info("Not executed the ou {} with error - {}".format(ou_id, ex))
                not_executed_list.append(ou_id)
        assert len(list_not_match) == 0, "Not matching Ou list - {}".format(list_not_match)
        assert len(not_executed_list) == 0, "OU is not executed- list is {}".format(not_executed_list)
