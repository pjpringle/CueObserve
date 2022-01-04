from dbConnections import (
    BigQuery,
    Druid,
    Pinot,
    Redshift,
    Snowflake,
    Druid,
    MySQL,
    Postgres,
    MSSQL,
    ClickHouse,
)
from anomaly.serializers import ConnectionDetailSerializer

class Data:
    @staticmethod
    def runQueryOnConnection(connectionType, connectionParams, query, limit=True):
        dataframe = None
        params = connectionParams
        if connectionType == "BigQuery":            
            dataframe = BigQuery.fetchDataframe(params, query, limit=limit)
        elif connectionType == "Druid":            
            dataframe = Druid.fetchDataframe(params, query, limit=limit)
        elif connectionType == "Pinot":
            dataframe = Pinot.fetchDataframe(params, query, limit=limit)            
        elif connectionType == "MySQL":
            dataframe = MySQL.fetchDataframe(params, query, limit=limit)
        elif connectionType == "Postgres":
            dataframe = Postgres.fetchDataframe(params, query, limit=limit)
        elif connectionType == "MSSQL":
            dataframe = MSSQL.fetchDataframe(params, query, limit=limit)
        elif connectionType == "Redshift":
            dataframe = Redshift.fetchDataframe(params, query, limit=limit)
        elif connectionType == "Snowflake":
            dataframe = Snowflake.fetchDataframe(params, query, limit=limit)
        elif connectionType == "ClickHouse":            
            dataframe = ClickHouse.fetchDataframe(params, query, limit=limit)

        return dataframe

    @staticmethod
    def fetchDatasetDataframe(dataset):
        connectionParams = {}
        for val in dataset.connection.cpvc.all():
            connectionParams[val.connectionParam.name] = val.value
        datasetDf = Data.runQueryOnConnection(
            dataset.connection.connectionType.name,
            connectionParams,
            dataset.sql,
            False,
        )
        return datasetDf
