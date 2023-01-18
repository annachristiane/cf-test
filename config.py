import libs.GCP.secretmanager as secretmanager
import os

postgresql_password = secretmanager.access_secret_version(
    os.getenv("project_id"), os.getenv("postgresql_password_secret"), 'latest')
mongo_password = secretmanager.access_secret_version(
    os.getenv("project_id"), os.getenv("mongo_password_secret"), 'latest')

postgresql = {
    "user": os.getenv("postgresql_username"),
    "password": postgresql_password,
    "host": os.getenv("postgresql_host"),  # 10.100.114.248
    "port": os.getenv("postgresql_port"),  # 5432
    "database": os.getenv("postgresql_database")  # warehouse
}

mongo = {
    "id": os.getenv("mongo_id"),  # datalec-warehouse-npd
    "pwd": mongo_password,
    # @sue-oms-prd-shard-00-03-pri.amewh.gcp.mongodb.net
    "server": os.getenv("mongo_server"),
    "port": os.getenv("mongo_port"),  # 27017
    "database": os.getenv("mongo_database"),  # sue-oms-order-api
    "collection": os.getenv("mongo_collection")  # promiseOrder
}

table_name = os.getenv("table_name")  # qal_omsorder_promiseorder

table_schema = os.getenv("table_schema")  # dev.qal_omsorder_promiseorder

mongo_uri = "mongodb://"+mongo['id']+":"+mongo['pwd']+mongo['server'] + \
    "/admin?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE&readPreference=secondary&readPreferenceTags=nodeType:ANALYTICS"

start_date = "2022/12/01 00:00:01"

end_date = "2023/01/05 00:00:00"
