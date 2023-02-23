import pandas as pd
import psycopg2
from yaml import safe_load
import db_connect as dbc
from sqlalchemy import text


class DataExtractor:
    def read_rds_table(self, db_connector,table_name):
        with db_connector.init_db_engine().connect() as connection:
            df = pd.read_sql_query(text(f'''select * from {table_name}'''), connection)
            df = pd.DataFrame(df)
            
            #print(df)
            return df


dbconnector = dbc.DatabaseConnector()
db_extractor = DataExtractor()
db_extractor.read_rds_table(dbconnector,"legacy_users")
