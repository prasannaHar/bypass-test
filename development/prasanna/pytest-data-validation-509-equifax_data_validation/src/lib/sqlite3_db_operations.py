

import sqlite3
import os


def sqlite3_database_creation(arg_db_name):
    """this function will be responsible for creating the database file

    Args:
        arg_db_name (text): database file name needs to be created


    Returns:
        Boolean: True by default
    """
    if os.path.exists(arg_db_name):
        os.remove(arg_db_name)

    conn = sqlite3.connect(arg_db_name)

    conn.close()

    return True



def sqlite3_database_create_table(arg_db_file_name, arg_table_name, arg_column_names):
    """this function will be responsible for creating the database table 

    Args:
        arg_table_name (text): table_name needs to be created
        arg_column_names (text): required table column names

    Returns:
        Boolean: True
    """

    conn = sqlite3.connect(arg_db_file_name)

    generate_table_query = "CREATE TABLE " + arg_table_name + " ("


    for each_column in arg_column_names:

        generate_table_query = generate_table_query + each_column + " TEXT, "


    generate_table_query = generate_table_query[:(len(generate_table_query)-2)]

    generate_table_query = generate_table_query + ");"

    conn.execute(generate_table_query)

    conn.commit()

    conn.close()

    return True






def sqlite3_database_insert_data_into_table(arg_db_file_name, arg_table_name, arg_records_needs_to_be_inserted):
    """this function will be responsible for inserting the records into the database

    Args:
        arg_db_file_name (string): required database file name needs to be connected
        arg_table_name (string): table name needs to be inserted
        arg_records_needs_to_be_inserted (2D-list): data needs to be inserted

    Returns:
        Boolean: True
    """
    conn = sqlite3.connect(arg_db_file_name)

    generate_insert_query =  "INSERT INTO " + arg_table_name + " VALUES ("


    for each_index in range(len(arg_records_needs_to_be_inserted[0])):

        generate_insert_query = generate_insert_query + " ?, "


    generate_insert_query = generate_insert_query[:(len(generate_insert_query)-2)]

    generate_insert_query = generate_insert_query + ");"

    conn.executemany(generate_insert_query, arg_records_needs_to_be_inserted)

    conn.commit()


    conn.close()

    return True



def sqlite3_database_retrieve_table_data(arg_db_file_name, arg_table_name):
    """this function will be responsible for retrieving the table data

    Args:
        arg_db_file_name (string): required database file name
        arg_table_name (string): database table name to be used

    Returns:
        List: 2D list with complete data related to the required table
    """

    conn = sqlite3.connect(arg_db_file_name)

    cursor = conn.execute("SELECT * from " + arg_table_name)

    rows = cursor.fetchall()

    conn.close()


    return rows



def sqlite3_database_update_query(arg_db_file_name, arg_table_name, column_names_to_be_updated, column_values_to_be_updated, condition_columns, condition_column_values):
    """this function will be responsible for executing the update queries based on the input arguments

    Args:
        arg_db_file_name (string): required database file name
        arg_table_name (string): table name to be used
        column_names_to_be_updated (List): required column names to be updated
        column_values_to_be_updated (list): column values needs to be updated
        condition_columns (List): condtions columns to be applied
        condition_column_values (List): condition column values to be used

    Returns:
        Boolean: True by default
    """


    conn = sqlite3.connect(arg_db_file_name)

    
    generate_sql_query = "UPDATE " + arg_table_name + " set " 
    
    for eachIndx in range(len(column_names_to_be_updated)):

        generate_sql_query = generate_sql_query + column_names_to_be_updated[eachIndx] + "='" + column_values_to_be_updated[eachIndx] + "' , "


    generate_sql_query = generate_sql_query[:(len(generate_sql_query)-2)]


    if(len(condition_columns)>0):

        generate_sql_query = generate_sql_query + " where "

        for eachIndx in range(len(condition_columns)):

            generate_sql_query = generate_sql_query + condition_columns[eachIndx] + "='" + condition_column_values[eachIndx] + "' and "

        generate_sql_query = generate_sql_query[:(len(generate_sql_query)-4)]


    print("generate_sql_query", generate_sql_query)

    cursor = conn.execute(generate_sql_query)

    conn.commit()

    conn.close()


    return True




# ## usage examples 

# sqlite3_database_creation("try.db")

# sqlite3_database_create_table("try.db", "test", ["column1", "column2", "column3"])

# sqlite3_database_insert_data_into_table("try.db", "test", [["a1", "b1", "c1"], ["a2", "b2", "c2"], ["a3", "b3", "c3"]])

# data = sqlite3_database_retrieve_table_data("try.db", "test")

# print(data)




