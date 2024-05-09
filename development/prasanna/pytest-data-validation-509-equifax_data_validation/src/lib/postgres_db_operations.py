import psycopg2

from psycopg2 import connect

## refernce links - 

# https://stackoverflow.com/questions/56332906/where-to-put-ssl-certificates-when-trying-to-connect-to-a-remote-database-using

# https://www.tutorialspoint.com/python_data_access/python_postgresql_select_data.htm

# https://www.postgresqltutorial.com/postgresql-row_number/ -- group by and order by related information

# https://learnsql.com/blog/how-to-find-duplicate-values-in-sql/ - information related to postgres count function 

# https://www.postgresqltutorial.com/compare-two-tables-in-postgresql/ - comparing the 2 sql queries



## prerequiste setup required - 

    # brew install postgresql
    # pip3 install psycopg2

    # reference - https://stackoverflow.com/questions/11618898/pg-config-executable-not-found

import os

database_connection_string = os.getenv('database_connection_string')




def postgres_database_execute_select_query(arg_req_query_needs_to_be_executed):

    from psycopg2 import connect
    conn = connect(database_connection_string)

    cursor = conn.cursor()


    #Setting auto commit false
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(arg_req_query_needs_to_be_executed)

    #Fetching 1st row from the table
    # result = cursor.fetchone()
    # print(result)

    #Fetching 1st row from the table
    result = cursor.fetchall()
    # print(result)

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()


    return result




def postgres_database_retrieve_table_data(arg_table_name, arg_req_columns, arg_condition_columns, arg_condition_column_values):
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
    

    if(len(arg_req_columns)>0):


        for eachColumn in arg_req_columns:

            generate_sql_query = generate_sql_query + eachColumn + ", "


        generate_sql_query = generate_sql_query[:(len(generate_sql_query)-2)] + " from " + arg_table_name + " "

    else:

        generate_sql_query = generate_sql_query + " * from " + arg_table_name + " "


    if(len(arg_condition_columns)>0):

        generate_sql_query = generate_sql_query + " where "

        for eachIndx in range(len(arg_condition_columns)):

            generate_sql_query = generate_sql_query + arg_condition_columns[eachIndx] + "='" + arg_condition_column_values[eachIndx] + "' and "

        generate_sql_query = generate_sql_query[:(len(generate_sql_query)-4)]


    print("generate_sql_query", generate_sql_query)


    #establishing the connection
    conn = connect(database_connection_string)

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()


    #Setting auto commit false
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(generate_sql_query)

    #Fetching 1st row from the table
    # result = cursor.fetchone()
    # print(result)

    #Fetching 1st row from the table
    result = cursor.fetchall()
    # print(result)

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

    return result







def postgres_database_retrieve_number_of_records(arg_table_name, arg_condition_columns, arg_condition_column_values):
    """this table return the required sql query result count from postgres table

    Args:
        arg_table_name (string): database table name
        arg_condition_columns (list): required condition columns needs to be updated
        arg_condition_column_values (list): required condition column values needs to be updated

    Returns:
        int: number records 
    """


    generate_sql_query = "select count(*) from " + arg_table_name + " " 
    

    if(len(arg_condition_columns)>0):

        generate_sql_query = generate_sql_query + " where "

        for eachIndx in range(len(arg_condition_columns)):

            generate_sql_query = generate_sql_query + arg_condition_columns[eachIndx] + "='" + arg_condition_column_values[eachIndx] + "' and "

        generate_sql_query = generate_sql_query[:(len(generate_sql_query)-4)]


    print("generate_sql_query", generate_sql_query)


    #establishing the connection
    conn = connect(database_connection_string)

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()


    #Setting auto commit false
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(generate_sql_query)

    #Fetching 1st row from the table
    # result = cursor.fetchone()
    # print(result)

    #Fetching 1st row from the table
    result = cursor.fetchall()
    # print(result)

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

    return result[0][0]




def postgres_retrieve_table_column_names(arg_req_table_name):
    """this table return required database column names

    Args:
        arg_req_table_name (string): database table name

    Returns:
        List: data base column names
    """



    #establishing the connection
    conn = connect(database_connection_string)

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()


    #Setting auto commit false
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    generate_required_sql_query = """SELECT
        column_name,
        data_type
    FROM
        information_schema.columns
    WHERE
        table_name = '""" + arg_req_table_name + "'"


    #Retrieving data
    cursor.execute(generate_required_sql_query)

    num_fields = len(cursor.description)
    
    #Fetching 1st row from the table
    result = cursor.fetchall()
    # print(result)

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()    


    res_column_names = []

    for eachRow in result:

        res_column_names.append(eachRow[0])


    # print(res_column_names)


    return res_column_names





def postgres_database_retrieve_dynamic_column_values(arg_required_tenant_name, arg_required_integration_id, arg_required_table_name, arg_required_column_name ):
    """this function return the dynamic filter values based on the input arguments

    Args:
        arg_required_column_name (string): required column name
        arg_required_tenant_name (string): required tenant name
        arg_required_table_name (string): required database table name

    Returns:
        list: dynamic column values can be used
    """


    generate_sql_query = "select distinct " + arg_required_column_name + ", count(*) from " + arg_required_tenant_name + "." + arg_required_table_name + " where integration_id=" + arg_required_integration_id +" and ingested_at in (select distinct ingested_at from " + arg_required_tenant_name +  "." + arg_required_table_name + " order by ingested_at desc limit 1) group by " + arg_required_column_name + " order by count(*) desc limit 3"

    print(generate_sql_query)


    #establishing the connection
    conn = connect(database_connection_string)

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()


    #Setting auto commit false
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(generate_sql_query)

    #Fetching 1st row from the table
    # result = cursor.fetchone()
    # print(result)

    #Fetching 1st row from the table
    result = cursor.fetchall()
    # print(result)

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

    required_filter_values = []

    for eachrow in result:

        required_filter_values.append(eachrow[0])

    if("Former user" in required_filter_values):

        required_filter_values.remove("Former user")

    if("_UNASSIGNED_" in required_filter_values):

        required_filter_values.remove("_UNASSIGNED_")

        

    return required_filter_values        





# # usage -- 

# resultt = postgres_database_retrieve_table_data(
#     arg_table_name="foo.jira_issues", 
#     arg_req_columns=[], 
#     arg_condition_columns=["key"], 
#     arg_condition_column_values=["LFE-2502"]
#     )


# print(resultt)


# postgres_retrieve_table_column_names('jira_issues')


