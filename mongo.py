import pymongo
import pandas as pd


def connect_to_mongo(f_conn):
    """Function that take in argument id, password, server, port, database name and collection name 
    and create a mongo connection"""
    
    mongo_uri = "mongodb://"+f_conn['id']+":"+f_conn['pwd']+f_conn['server'] + \
        "/admin?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE&readPreference=secondary&readPreferenceTags=nodeType:ANALYTICS"
    client = pymongo.MongoClient(mongo_uri, int(f_conn['port']))
    return client


def mongo_to_df(conn, query):
    """This function take in argument parameter of a connection and a query mongo_df 
    and put the result and this query in a dataframe"""
    client = connect_to_mongo(conn)
    db = client[conn['database']]
    collection = db[conn['collection']]
    results = collection.aggregate(query)
    df = pd.DataFrame.from_records(results)
    return (df)
