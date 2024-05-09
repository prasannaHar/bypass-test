import logging
import psycopg2
import os
import pandas as pd

LOG_FORMAT = "%(asctime)s %(levelname)s : %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOG = logging.getLogger(__name__)


class TestPostgresHelper:
    def __init__(self, generic_helper):
        self.generic = generic_helper
        self.query_info = self.generic.get_api_info()
        self.cursor, self.connection = self.create_postgres_connection()

    def close_postgres_cconnection(self):
        LOG.info("Closing the connection with postgres")
        self.cursor.close()
        self.connection.close()
        LOG.info("Connection closed successfully")

    def create_postgres_connection(self, user=None, password=None, host=None, database=None, port=None):
        """Create Postgress connection. output is cursor, connection"""

        count = self.query_info["postgres_validation"]["retry_count"]

        if not user:
            user = os.getenv("DBUSER")
        if not password:
            password = os.getenv("DB_PASSWORD")
        if not host:
            host = os.getenv("HOST")
        if not database:
            database = os.getenv("DATABASE")
        if not port:
            port = self.query_info["postgres_validation"]["db_port"]

        sslrootcert = "cred/ca.pem"
        sslcert = "cred/cert.pem"
        sslkey = "cred/key.pem"

        while int(count) > 0:
            LOG.info(
                "waiting for ...retry count... {}".format(count)
            )
            # LOG.info("DB parameters {} {} {} {} {}".format(user, password, host, database, port))
            try:
                LOG.info("Connecting to PostgreSQL...")
                connection = psycopg2.connect(
                    user=user,
                    password=password,
                    host=host,
                    port=port,
                    database=database,
                    sslmode="verify-ca",
                    sslrootcert=sslrootcert,
                    sslcert=sslcert,
                    sslkey=sslkey
                )
                cursor = connection.cursor()
                LOG.info("Connected to PostgreSQL...")
                return cursor, connection
            except (Exception, psycopg2.DatabaseError) as error:
                LOG.error(
                    "Error while connecting to PostgreSQL {} ...retrying...".format(
                        error
                    )
                )
                LOG.info(
                    "waiting for ...retry count... {}".format(count
                                                              )
                )
                count -= 1

        LOG.error("Unable to connect to PostgreSQL")
        return

    def execute_query(self, query, df_flag=None):
        """Execute the query of with cursor, connection, and query"""
        if df_flag:

            if pd.read_sql_query(query, self.connection).empty:
                issue_data=pd.DataFrame()
                return issue_data
            else:

                issue_data = pd.read_sql_query(query, self.connection)

            # return issue_data_df
        else:
            try:
                self.cursor.execute(query)
                self.connection.commit()
                issue_data = self.cursor.fetchall()

            except (psycopg2.InterfaceError, psycopg2.OperationalError) as error:
                LOG.error("Error in executing query: {}".format(error))
                return

        return issue_data

    def postgres_database_retrieve_table_data(self, arg_table_name, arg_req_columns=None,
                                              arg_condition_columns=None, df_flag=None):
        """this table return the required sql query results from postgres table

        Args:
            arg_table_name (string): database table name
            arg_req_columns (list): required column names if keep the list as empty if all the column details are required
            arg_condition_columns (list): required condition columns needs to be updated
            arg_condition_column_values (list): required condition column values needs to be updated

        Returns:
            List: multi-dimensional list with  the sql query result
        """
        generate_sql_query = "select "
        if arg_req_columns:
            for eachColumn in arg_req_columns:
                generate_sql_query = generate_sql_query + eachColumn + ", "
            generate_sql_query = generate_sql_query[:(len(generate_sql_query) - 2)] + " from " + arg_table_name + " "
        else:
            generate_sql_query = generate_sql_query + " * from " + arg_table_name + " "
        if arg_condition_columns:
            generate_sql_query = generate_sql_query + " where "
            for eachrecord in arg_condition_columns:
                data_type, column_name, column_val = eachrecord.split(":")
                generate_sql_query = generate_sql_query + column_name + "="
                if data_type == "str":
                    generate_sql_query = generate_sql_query + "'" + str(column_val) + "' "
                elif data_type == "num":
                    generate_sql_query = generate_sql_query + str(column_val) + "  "
                generate_sql_query = generate_sql_query + " and "
            generate_sql_query = generate_sql_query[:(len(generate_sql_query) - 4)]
        LOG.info("sql query generated - {}".format(generate_sql_query))
        result = self.execute_query(generate_sql_query, df_flag=df_flag)
        return result

    def postgres_retrieve_table_column_names(self, arg_req_table_name):
        """this table return required database column names

        Args:
            arg_req_table_name (string): database table name

        Returns:
            List: data base column names
        """
        generate_required_sql_query = """SELECT
            column_name,
            data_type
        FROM
            information_schema.columns
        WHERE
            table_name = '""" + arg_req_table_name + "'"
        result = self.execute_query(generate_required_sql_query)
        res_column_names = []
        for eachRow in result:
            res_column_names.append(eachRow[0])
        return res_column_names

    def postgres_database_retrieve_dynamic_column_values(self, arg_required_tenant_name, arg_required_integration_id,
                                                         arg_required_table_name, arg_required_column_name):
        """this function return the dynamic filter values based on the input arguments

        Args:
            arg_required_column_name (string): required column name
            arg_required_tenant_name (string): required tenant name
            arg_required_table_name (string): required database table name

        Returns:
            list: dynamic column values can be used
        """
        generate_sql_query = "select distinct " + arg_required_column_name + ", count(*) from " + arg_required_tenant_name + "." + arg_required_table_name + " where integration_id= [" + ','.join(
            arg_required_integration_id) + "] and ingested_at in (select distinct ingested_at from " + arg_required_tenant_name + "." + arg_required_table_name + " order by ingested_at desc limit 1) group by " + arg_required_column_name + " order by count(*) desc limit 3"

        result = self.execute_query(generate_sql_query)
        required_filter_values = []

        for eachrow in result:
            required_filter_values.append(eachrow[0])
        if "Former user" in required_filter_values:
            required_filter_values.remove("Former user")
        if "_UNASSIGNED_" in required_filter_values:
            required_filter_values.remove("_UNASSIGNED_")
        return required_filter_values
