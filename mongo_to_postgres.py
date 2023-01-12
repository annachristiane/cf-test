import libs.postgresql as postgresql
import warnings
import pandas as pd
import datetime
import libs.mongo as mongo
import libs.postgresql as postgresql


warnings.simplefilter("ignore", UserWarning)


def columns_to_list(table, conn_postgresql):
    """
    Function that takes as arguments a connection and a table and will simply put all the columns of the table in a list.
    """
    conn = postgresql.connect_to_postgres(conn_postgresql)
    cursor = conn.cursor()
    table_name = table
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
    cursor.execute(query)
    column_names = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return (column_names)


def liste_to_fieldnames(liste):
    """Function that takes as arguments the list containing our columns from our select 
    and put them in the format expected by our postgres table, before that we remove first column (_id) 
    and last(mostRecentDate), cause they are integrated later"""
    liste.pop(0)
    liste.pop(-1)
    for i in range(len(liste)):
        liste[i] = liste[i].replace("_", "-")
        liste[i] = liste[i].replace("OID", "_id")
    return (liste)


def creation_insert_request_mongo(fieldnames, start, end):
    """this function takes as argument fieldname, a start date and an end date, 
    it will create a query mongo DB composed of two parts, the match which allows to filter on the date 
    and the project part which allows to get the data of the columns which interest us. It return the request in string format """
    pipeline = []
    match_stage = {
        "$match": {
            "objectInfos.creation.date": {
                "$gte": start,
                "$lt": end
            }
        }
    }
    pipeline.append(match_stage)
    project_stage = {
        "$project": {}
    }
    for fieldname in fieldnames:
        path = fieldname.split("-")
        projection = "$" + ".".join(path)
        project_stage["$project"][fieldname] = projection
    pipeline.append(project_stage)
    return (pipeline)


"""Retrieves the highest date, between 2 dates """


def compare_values(row): return max(
    row['objectInfos-creation-date'], row['objectInfos-lastUpdate-date'])


def delta_update(fieldnames, table, conn_mongo, conn):
    """this function is similar to creation_insert_request_mongo, it generate a mongoDB request, that take data between 
    max most recent date and date of the execution of the script, 
    it put data in a dataframe and generate the new mostrecent date"""
    client = mongo.connect_to_mongo(conn_mongo)
    db = client[conn_mongo['database']]
    collection = db[conn_mongo['collection']]
    dateExec = datetime.datetime.now()
    mostRecentDate = postgresql.postgres_mostRecentDate(
        conn, table).at[0, 'max']
    pipeline = []
    match_stage = {
        "$match": {
            "$or": [
                {"objectInfos.creation.date": {
                    "$gte": mostRecentDate, "$lte": dateExec}},
                {"objectInfos.lastUpdate.date": {
                    "$gte": mostRecentDate, "$lte": dateExec}}
            ]
        }
    }
    pipeline.append(match_stage)
    project_stage = {
        "$project": {}
    }
    for fieldname in fieldnames:
        path = fieldname.split("-")
        projection = "$" + ".".join(path)
        project_stage["$project"][fieldname] = projection
    pipeline.append(project_stage)
    results = collection.aggregate(pipeline)
    df = pd.DataFrame.from_records(results)
    df['mostRecentDate'] = df.apply(compare_values, axis=1)
    df['mostRecentDate'] = pd.to_datetime(df['mostRecentDate'])
    return (df)


def insert_update_value(conn, df, table, table_schema, pk):
    """Retrieve the columns of the table, then Iterate over the rows in the DataFrame, 
    Create the insert query on conflict do update query"""
    cursor = conn.cursor()

    cursor.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'")
    columns = [col[0] for col in cursor.fetchall()]
    columns2 = [f'"{x}"' for x in columns]
    df.columns = df.columns.str.replace('-', '_')
    df = df.rename(columns={'_id': columns[0]})
    for i, row in df.iterrows():
        query = query = f"INSERT INTO {table_schema} (" + ", ".join(columns2) + ") VALUES(" + ", ".join(["%s"] * len(
            columns2)) + ") ON CONFLICT(" + pk + ") DO UPDATE SET " + ", ".join([f"{col} = EXCLUDED.{col}" for col in columns2[1:]])
        print(query)
        cursor.execute(query, tuple(row[col] for col in columns))
        conn.commit()

    cursor.close()
    conn.close()
