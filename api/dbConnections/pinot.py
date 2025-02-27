import json
import logging
import pandas as pd
from pinotdb import connect
from dbConnections.utils import limitSql

logger = logging.getLogger(__name__)

class Pinot:
    """
    Class to support functionalities on Apache Pinot connection
    """

    @staticmethod
    def checkConnection(params):
        """
        Check if connection can be established for Pinot
        """
        res = True
        try:
            host = params.get("host", "")
            port = params.get("port", 8099)
            conn = connect(host=host, port=port, path="/query/sql", scheme="http")
            conn.cursor()
            pd.read_sql("SELECT now() FROM ignoreMe", conn, chunksize=None)

        except Exception:
            logger.error("Can't connect to db with these credentials ")
            res = False
        return res

    @staticmethod
    def fetchDataframe(params: dict, sql: str, limit: bool = False):
        """
        Fetch dataframe for given sql
        """
        dataframe = None
        try:
            host = params.get("host", "")
            port = params.get("port", 8099)
            conn = connect(host=host, port=port, path="/query/sql", scheme="http")
            if limit:
                sql = limitSql(sql)
            chunksize = None
            dataframe = pd.read_sql(sql, conn, chunksize=chunksize)

        except Exception as ex:
            logger.error("Can't connect to db with these credentials %s", str(ex))

        return dataframe
