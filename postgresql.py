import psycopg2
import psycopg2.extras as extras
import pandas as pd


def connect_to_postgres(conn):
    """ This fonction take user, password, host, port and database name 
    and use psycopG2 lib to create a connection to a postgresql database"""
    conn = psycopg2.connect(
        user=conn['user'],
        password=conn['password'],
        host=conn['host'],
        port=conn['port'],
        database=conn['database'])
    return (conn)


def insert_value(conn, df, table):
    """Function that take in argument a conn, a dataframe and a table
    we split the df into tuple, and then we use this tuple in a Insert sql request, with try except close, 
    we try to insert values, if not working we return the error and rollback the connexion"""
    tuples = [tuple(x) for x in df.to_numpy()]
    query = "INSERT INTO %s VALUES %%s" % (table)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


def select_to_df(conn, table):
    """This function take in argument connection parameter, and a table. Use connect_to_postgres function created before
    then put in a dataframe the result of a Select * from the table in argument then return this dataframe"""
    conn_str = connect_to_postgres(conn)
    df = pd.read_sql_query("SELECT * FROM "+table, conn_str)
    conn_str.close()
    return(df)

def postgres_mostRecentDate(conn, table):
    """This function is similar to select_to_df take the same argument, the difference is here 
    we store the most recent date of a table, put in argument"""
    conn_str = connect_to_postgres(conn)
    df = pd.read_sql_query('select Max("mostRecentDate") from '+table, conn_str)
    conn_str.close()
    return(df)


def insert_update_value(conn, df, table, pk):
    """This function is pretty similar to insert_value, the only difference is, here if the insert 
    return a duplicate primary key error, we update values in the table"""
    cursor = conn.cursor()
    for i,row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
        values = tuple(row)
        try:
            cursor.execute(insert_query, values)
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            update_query = f'UPDATE {table} SET "objectInfos_creation_date" = %s , "objectInfos_lastUpdate_date" = %s, "mostRecentDate" = %s where "{pk}" = %s'
            creation_date = row['objectInfos-creation-date'].strftime("%Y-%m-%d %H:%M:%S.%f")
            last_update_date = row['objectInfos-lastUpdate-date'].strftime("%Y-%m-%d %H:%M:%S.%f")
            mostrecentdate = row['mostRecentDate'].strftime("%Y-%m-%d %H:%M:%S.%f")
            update_values = (creation_date,last_update_date ,mostrecentdate, row['_id'])
            cursor.execute(update_query, update_values)
            conn.commit()
    print("the dataframe is inserted")
    cursor.close()