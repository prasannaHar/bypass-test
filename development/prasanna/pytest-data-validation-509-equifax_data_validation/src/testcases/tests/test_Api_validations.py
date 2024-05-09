import calendar
import json
import time
import pytest
import logging
from src.utils.generate_Api_payload import GenericPayload
from src.lib.generic_helper.generic_helper import TestGenericHelper as TGhelper

api_payload = GenericPayload()
LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)

user_types = [("ADMIN"), ("LIMITED_USER"), ("AUDITOR"), ("RESTRICTED_USER"), ("ASSIGNED_ISSUES_USER"),
              ("PUBLIC_DASHBOARD")]
generic_object = TGhelper()
reset_password_url = generic_object.connection["base_url"] + "forgot_password"


class TestApiValidation:

    @pytest.mark.run(order=1)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_login(self, create_generic_object):
        login_response = create_generic_object.get_auth_token()
        assert len(login_response) > 0, "Failed to Login : " + login_response

    @pytest.mark.run(order=2)
    @pytest.mark.regression_need_db
    def test_create_project_no_integration(self, create_generic_object, create_postgres_object, create_project_object):
        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        project_name = "automation-project-noIntegration" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)
        products_id = create_project_response["id"]
        query = "SELECT * FROM " + create_generic_object.connection['tenant_name'] + ".products WHERE id=" + products_id
        LOG.info("Query for products {}".format(query))
        try:
            required_records_from_database = create_postgres_object.execute_query(query)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert products_id == str(required_records_from_database[0][0]), "data not found in DB "
            assert project_name == str(required_records_from_database[0][1]), "data not found in DB "

    @pytest.mark.run(order=3)
    @pytest.mark.regression_need_db
    def test_create_project_with_integration(self, create_generic_object, create_postgres_object,
                                             create_project_object, get_integration_obj):
        ## get user details
        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        project_name = "automation-project" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)

        product_id = create_project_response['id']
        create_project_object.add_integration(get_integration_obj, product_id)

        sql_query = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".products WHERE id=" + product_id
        LOG.info(sql_query)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert product_id == str(required_records_from_database[0][0]), "data not found in DB "
            assert project_name == str(required_records_from_database[0][1]), "data not found in DB "

    @pytest.mark.run(order=4)
    @pytest.mark.regression_need_db
    def test_create_Dashboard_nofilter_nointegration(self, create_generic_object, create_postgres_object,
                                                     create_project_object,
                                                     create_dashboard_object):
        ## get user details

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)

        get_create_dashboard_response = create_dashboard_object.create_dashboard(arg_dashboard_name=dashboard_name)
        dashboard_id = get_create_dashboard_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + dashboard_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert dashboard_id == str(required_records_from_database[0][0]), "data not found in DB "
            assert dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "
            create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.run(order=5)
    @pytest.mark.regression_need_db
    def test_create_Dashboard_Time_Range_nointegration(self, create_generic_object, create_postgres_object,
                                                       create_project_object,
                                                       create_dashboard_object):

        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard-Time-Range" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_time_range=True
        )
        dashboard_id = get_create_dashboard_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + dashboard_id
        LOG.info(sql_query_needs_to_be_executed)
        # try:
        required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        # except:
        #     required_records_from_database = 00
        #     logging.error("unable to get data from DB")

        # finally:
        #     assert required_records_from_database != 00
        #     assert dashboard_id == str(required_records_from_database[0][0]), "data not found in DB "
        #     assert dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "
        #     assert required_records_from_database[0][6]['dashboard_time_range'], "data not found in DB "
        #     delete_dashboard(dashboard_id)

    @pytest.mark.run(order=6)
    @pytest.mark.regression_need_db
    def test_create_Dashboard_investment_profile_nointegration(self, create_generic_object, create_postgres_object,
                                                               create_project_object,
                                                               create_dashboard_object):
        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard-Time-Range" + str(ts)

        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_investment_profile=True

        )
        dashboard_id = get_create_dashboard_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + dashboard_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert dashboard_id == str(required_records_from_database[0][0]), "data not found in DB "
            assert dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "
            assert required_records_from_database[0][5]['effort_investment_profile'], "data not found in DB "
            create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.run(order=7)
    @pytest.mark.regression_need_db
    def test_create_Dashboard_investment_unit_nointegration(self, create_generic_object, create_postgres_object,
                                                            create_project_object,
                                                            create_dashboard_object):
        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard-Time-Range" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)

        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_investment_unit=True

        )
        dashboard_id = get_create_dashboard_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + dashboard_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert dashboard_id == str(required_records_from_database[0][0]), "data not found in DB "
            assert dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "
            assert required_records_from_database[0][5]['effort_investment_unit'], "data not found in DB "
            create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.run(order=8)
    @pytest.mark.regression_need_db
    def test_create_Dashboard_all_params_nointegration(self, create_generic_object, create_postgres_object,
                                                       create_project_object,
                                                       create_dashboard_object):

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard-Time-Range" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)

        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_time_range=True,
            arg_investment_unit=True,
            arg_investment_profile=True

        )
        dashboard_id = get_create_dashboard_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + dashboard_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert dashboard_id == str(required_records_from_database[0][0]), "data not found in DB "
            assert dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "
            assert required_records_from_database[0][5]['effort_investment_unit'], "data not found in DB "
            assert required_records_from_database[0][5]['effort_investment_profile'], "data not found in DB "
            assert required_records_from_database[0][5]['dashboard_time_range'], "data not found in DB "
            create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.run(order=9)
    @pytest.mark.regression_need_db
    def test_create_Dashboard_org_units_nointegration(self, create_generic_object, create_postgres_object,
                                                      create_project_object,
                                                      create_dashboard_object):

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard-Time-Range" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)
        products_id = create_project_response["id"]
        get_create_dashboard_response = create_dashboard_object.create_dashboard(
            arg_dashboard_name=dashboard_name,
            arg_show_org_unit_selection=True

        )
        dashboard_id = get_create_dashboard_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + dashboard_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
            logging.error("unable to get data from DB")
        finally:
            assert required_records_from_database != 00
            assert dashboard_id == str(required_records_from_database[0][0]), "data not found in DB "
            assert dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "
            assert required_records_from_database[0][5]['show_org_unit_selection'], "data not found in DB "
            create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.run(order=10)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_reset_password(self, create_generic_object, widgetreusable_object):
        if not pytest.standalone_app_flag:
            pytest.skip("not applicable for SEI on Harness")
        get_user_details = create_generic_object.get_user_detail()
        user_name = get_user_details["email"]
        get_reset_password_payload = api_payload.generate_reset_password_payload(
            arg_req_username=user_name,
            arg_req_company=create_generic_object.connection["tenant_name"]

        )

        get_reset_password = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=reset_password_url,
            arg_req_payload=get_reset_password_payload
        )

    @pytest.mark.run(order=11)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_forgot_password(self, create_generic_object, widgetreusable_object):
        if not pytest.standalone_app_flag:
            pytest.skip("not applicable for SEI on Harness")
        get_user_details = create_generic_object.get_user_detail()
        user_name = get_user_details["email"]
        get_reset_password_payload = api_payload.generate_reset_password_payload(
            arg_req_username=user_name,
            arg_req_company=create_generic_object.connection["tenant_name"]

        )

        get_forgot_password = widgetreusable_object.retrieve_required_api_response(
            arg_req_api=reset_password_url,
            arg_req_payload=get_reset_password_payload
        )

    @pytest.mark.run(order=12)
    @pytest.mark.regression_need_db
    def test_delete_dashboard(self, create_generic_object, create_postgres_object, create_project_object,
                              create_dashboard_object):

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)

        get_create_dashboard_response = create_dashboard_object.create_dashboard(arg_dashboard_name=dashboard_name)
        dashboard_id = get_create_dashboard_response["id"]
        delete_dashboard_response = create_dashboard_object.delete_dashboard(dashboard_id)
        delete_dashboard_response = json.loads(delete_dashboard_response)
        assert delete_dashboard_response["id"] == dashboard_id
        assert delete_dashboard_response["success"]

        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards"
        LOG.info(sql_query_needs_to_be_executed)
        dash_ids = []
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            for each_record in range(len(required_records_from_database)):
                dash_ids.append(required_records_from_database[each_record][0])
        except:
            required_records_from_database = 00
            logging.error("unable to get data from DB")

        finally:
            assert required_records_from_database != 00
            assert int(dashboard_id) not in dash_ids

    @pytest.mark.run(order=13)
    @pytest.mark.regression_need_db
    def test_update_dashboard_settings(self, create_generic_object, create_postgres_object, create_project_object,
                                       create_dashboard_object):

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)
        products_id = create_project_response["id"]
        get_create_dashboard_response = create_dashboard_object.create_dashboard(arg_dashboard_name=dashboard_name)
        dashboard_id = get_create_dashboard_response["id"]
        updated_dashboard_name = dashboard_name + "updated"
        create_dashboard_object.update_dashboard(arg_dashboard_id=dashboard_id,
                                                 arg_dashboard_name=updated_dashboard_name,
                                                 arg_product_id=products_id
                                                 )
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + dashboard_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert updated_dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "
            create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.run(order=14)
    @pytest.mark.regression_need_db
    def test_clone_dashboard(self, create_generic_object, create_postgres_object, create_project_object,
                             create_dashboard_object):

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)
        products_id = create_project_response["id"]
        get_create_dashboard_response = create_dashboard_object.create_dashboard(arg_dashboard_name=dashboard_name)
        dashboard_id = get_create_dashboard_response["id"]
        clone_dashboard_name = dashboard_name + "updated"
        clone_dashboard_response = create_dashboard_object.clone_dashboard(arg_dashboard_id=dashboard_id,
                                                                           arg_dashboard_name=clone_dashboard_name,
                                                                           arg_product_id=products_id
                                                                           )
        clone_dashboard_id = clone_dashboard_response
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".dashboards WHERE id=" + str(clone_dashboard_id)
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
            logging.error("unable to get data from DB")
        finally:
            create_dashboard_object.delete_dashboard(dashboard_id)
            create_dashboard_object.delete_dashboard(str(clone_dashboard_id))
            assert required_records_from_database != 00
            assert clone_dashboard_name == str(required_records_from_database[0][1]), "data not found in DB "

    @pytest.mark.run(order=15)
    @pytest.mark.regression
    @pytest.mark.sanity
    def test_download_dashboard(self, create_project_object, create_dashboard_object):
        if not pytest.standalone_app_flag:
            pytest.skip("not applicable for SEI on Harness")
        ts = calendar.timegm(time.gmtime())
        dashboard_name = "automation-dashboard" + str(ts)
        project_name = "automation-project-noIntegration" + str(ts)

        get_create_dashboard_response = create_dashboard_object.create_dashboard(arg_dashboard_name=dashboard_name)
        dashboard_id = get_create_dashboard_response["id"]
        download_dashboard_response = create_dashboard_object.export_dashboard(arg_dashboard_id=dashboard_id,
                                                                               arg_dashboard_name=dashboard_name)
        create_dashboard_object.delete_dashboard(dashboard_id)

    @pytest.mark.run(order=16)
    @pytest.mark.regression_need_db
    def test_update_project_details_no_integration(self, create_generic_object, create_postgres_object,
                                                   create_project_object,
                                                   create_dashboard_object):

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        project_name = "automation-project-noIntegration" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)
        products_id = create_project_response["id"]
        updated_projet_description = "project-desc-updation-check" + str(ts)
        create_project_object.update_project(
            arg_project_id=products_id,
            arg_updated_desc=updated_projet_description,
        )
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".products WHERE id=" + products_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert updated_projet_description == str(
                required_records_from_database[0][2]), "data not found in DB "

    @pytest.mark.run(order=17)
    @pytest.mark.regression_need_db
    def test_update_project_details_with_integration(self, create_generic_object, create_postgres_object,
                                                     create_project_object,
                                                     create_dashboard_object, get_integration_obj):

        global required_records_from_database
        ts = calendar.timegm(time.gmtime())
        project_name = "automation-project-noIntegration" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)
        products_id = create_project_response["id"]

        create_project_object.add_integration(get_integration_obj, products_id)
        updated_projet_description = "project-desc-updation-check" + str(ts)
        create_project_object.update_project(
            arg_project_id=products_id,
            arg_updated_desc=updated_projet_description,
        )
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection[
            'tenant_name'] + ".products WHERE id=" + products_id
        LOG.info(sql_query_needs_to_be_executed)
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
        except:
            required_records_from_database = 00
        finally:
            assert required_records_from_database != 00
            assert updated_projet_description == str(
                required_records_from_database[0][2]), "data not found in DB "

    @pytest.mark.run(order=19)
    @pytest.mark.need_maintenance
    def test_delete_project_with_integration(self, create_project_object,
                                             create_dashboard_object, get_integration_obj):

        ts = calendar.timegm(time.gmtime())
        project_name = "automation-project-noIntegration" + str(ts)
        create_project_response = create_project_object.create_project_without_integration(
            arg_project_name=project_name)
        products_id = create_project_response["id"]
        create_project_object.add_integration(get_integration_obj, products_id)
        create_project_object.delete_project(products_id)

    @pytest.mark.run(order=20)
    @pytest.mark.regression_need_db
    @pytest.mark.parametrize("user_type", user_types)
    def test_create_propelo_user(self, create_postgres_object, create_project_object, user_type, create_generic_object):
        new_user_response = create_generic_object.create_user(arg_user_type=user_type)
        new_user_id = new_user_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection['tenant_name'] + ".users"
        LOG.info(sql_query_needs_to_be_executed)
        user_ids = []
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            for each_record in range(len(required_records_from_database)):
                user_ids.append(required_records_from_database[each_record][0])
        except:
            logging.error("unable to get data from DB")
            assert False
        finally:
            assert int(new_user_id) in user_ids
            create_generic_object.delete_user(new_user_id)

    @pytest.mark.run(order=21)
    @pytest.mark.regression_need_db
    @pytest.mark.parametrize("user_type", user_types)
    def test_delete_propelo_user(self, create_postgres_object, create_project_object, user_type, create_generic_object):
        new_user_response = create_generic_object.create_user(arg_user_type=user_type)
        new_user_id = new_user_response["id"]
        create_generic_object.delete_user(new_user_id)
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection['tenant_name'] + ".users"
        LOG.info(sql_query_needs_to_be_executed)
        user_ids = []

        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            for each_record in range(len(required_records_from_database)):
                user_ids.append(required_records_from_database[each_record][0])
        except:
            logging.error("unable to get data from DB")
            assert False
        finally:
            assert int(new_user_id) not in user_ids

    @pytest.mark.run(order=22)
    @pytest.mark.regression_need_db
    @pytest.mark.parametrize("user_type", user_types)
    def test_update_propelo_user(self, create_postgres_object, create_project_object, user_type, create_generic_object):
        new_user_response = create_generic_object.create_user(arg_user_type=user_type)
        new_user_id = new_user_response["id"]
        sql_query_needs_to_be_executed = "SELECT * FROM " + create_generic_object.connection['tenant_name'] + ".users"
        LOG.info(sql_query_needs_to_be_executed)
        user_ids = []
        try:
            required_records_from_database = create_postgres_object.execute_query(sql_query_needs_to_be_executed)
            for each_record in range(len(required_records_from_database)):
                user_ids.append(required_records_from_database[each_record][0])
        except:
            logging.error("unable to get data from DB")
            assert False
        finally:
            assert int(new_user_id) in user_ids
        user_details = create_generic_object.get_user_detail_from_id(arg_user_id=new_user_id)
        user_emai = user_details["email"]
        first_name = user_details["first_name"]
        last_name = user_details["last_name"] + "-update"
        # update_user(arg_user_type=user_type,a)

        update_response = create_generic_object.update_user(arg_user_id=new_user_id, arg_user_type=user_type,
                                                            arg_email=user_emai,
                                                            arg_last_name=last_name, arg_first_name=first_name)

        create_generic_object.delete_user(new_user_id)
