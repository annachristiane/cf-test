import datetime
from mongo_to_postgres import *
import postgresql
import mongo
import yaml
import pandas as pd
import os

"""_____________________COMMON PART (Only connection and fielnames creation)_____________________"""
with open('config.yml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

conn_mongo = config['conn_mongo']
conn_postgresql = config['conn_postgresql']
table_schema = config['table_schema']
table_name = config['table_name']

conn = postgresql.connect_to_postgres(conn_postgresql)
liste = columns_to_list(table_name, conn_postgresql)

# Nos colonnes une fois transformé deviennent nos fieldnames
fieldnames = liste_to_fieldnames(liste)



"""___________________________________INSERT PART__________________________________"""
# Création d'une query mongo DB pour filter nos lignes (ici en fonction de la date)
start = datetime.datetime(2022, 12, 1, 0, 0, 0, 1)
end = datetime.datetime(2023, 1, 5, 0, 0, 0, 0)

# on genere une query mongo DB qui recupere les données entre 2 date et les colonnes qui sont presentes dans fieldnames
query = creation_request_mongo(fieldnames, start, end)

df = mongo.mongo_to_df(conn_mongo, query)


df['objectInfos-creation-date'] = pd.to_datetime(
    df['objectInfos-creation-date'])
df['objectInfos-lastUpdate-date'] = pd.to_datetime(
    df['objectInfos-lastUpdate-date'])


# On applique la lambda functions pour recupérer dans une nouvelle colonne de DF la date la plus élevée
df['mostRecentDate'] = df.apply(compare_values, axis=1)
df['mostRecentDate'] = pd.to_datetime(df['mostRecentDate'])

# on defini une date par défaut pour les date manquante (ici le 01/01/2000) ANCIEN
default_timestamp = pd.Timestamp('2000-01-01 00:00:00.000')

# Replace NaT values in the DataFrame with the default timestamp value
df = df.fillna(default_timestamp)

for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].astype(str)

postgresql.insert_value(conn, df, table_schema)

"""____________________________________DELTA PART_____________________________________"""
df3 = delta_update(fieldnames, table_schema,conn_mongo, conn_postgresql)
df3['objectInfos-creation-date']=pd.to_datetime(df3['objectInfos-creation-date'])
df3['objectInfos-lastUpdate-date']=pd.to_datetime(df3['objectInfos-lastUpdate-date'])
fieldnames = ['_id']+fieldnames
fieldnames.append('mostRecentDate')
df3 = df3.reindex(columns=fieldnames)
default_timestamp = pd.Timestamp('2000-01-01 00:00:00.000')
df3 = df3.fillna(default_timestamp)

for col in df3.columns:
    if df3[col].dtype == 'object':
        df3[col] = df3[col].astype(str)

insert_update_value(conn, df3, table_name,table_schema, 'promiseOrderOID')