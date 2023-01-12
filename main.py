import datetime
from mongo_to_postgres import *
import libs.postgresql as postgresql
import libs.mongo as mongo
from libs.types import isObject
# import yaml
import pandas as pd
import config
import os

"""_____________________COMMON PART (Only connection and fielnames creation)_____________________"""

conn_postgresql = postgresql.connect_to_postgres(config.postgresql)
list = columns_to_list(config.table_name, config.postgresql)

# Nos colonnes une fois transformé deviennent nos fieldnames
fieldnames = list_to_fieldnames(list)

"""___________________________________INSERT PART__________________________________"""
# Création d'une query mongo DB pour filter nos lignes (ici en fonction de la date)
start = datetime.datetime(2022, 12, 1, 0, 0, 0, 1)
end = datetime.datetime(2023, 1, 5, 0, 0, 0, 0)

# on genere une query mongo DB qui recupere les données entre 2 date et les colonnes qui sont presentes dans fieldnames
# creation_insert_request_mongo??
query = creation_request_mongo(fieldnames, start, end)

df = mongo.mongo_to_df(config.mongo, query)


df['objectInfos-creation-date'] = pd.to_datetime(
    df['objectInfos-creation-date'])
df['objectInfos-lastUpdate-date'] = pd.to_datetime(
    df['objectInfos-lastUpdate-date'])

# date_cols = ['objectInfos-creation-date', 'objectInfos-lastUpdate-date']
# df[date_cols] = df[date_cols].apply(pd.to_datetime, errors='coerce')
# df['mostRecentDate'] = get_max(date_cols)

# On applique la lambda functions pour recupérer dans une nouvelle colonne de DF la date la plus élevée
df['mostRecentDate'] = df.apply(compare_values, axis=1)
df['mostRecentDate'] = pd.to_datetime(df['mostRecentDate'])

for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].astype(str)

# for col in df.columns:
#     if isObject(col):
#         df[col] = df[col].astype(str)

postgresql.insert_value(conn_postgresql, df, config.table_schema)

"""____________________________________DELTA PART_____________________________________"""
df3 = delta_update(fieldnames, config.table_schema,
                   config.mongo, config.postgresql)
df3['objectInfos-creation-date'] = pd.to_datetime(
    df3['objectInfos-creation-date'])
df3['objectInfos-lastUpdate-date'] = pd.to_datetime(
    df3['objectInfos-lastUpdate-date'])
fieldnames = ['_id']+fieldnames
fieldnames.append('mostRecentDate')
df3 = df3.reindex(columns=fieldnames)
default_timestamp = pd.Timestamp('2000-01-01 00:00:00.000')
df3 = df3.fillna(default_timestamp)

for col in df3.columns:
    if df3[col].dtype == 'object':
        df3[col] = df3[col].astype(str)

insert_update_value(conn_postgresql, df3, config.table_name,
                    config.table_schema, 'promiseOrderOID')
