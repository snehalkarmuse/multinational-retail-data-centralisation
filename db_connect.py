import pandas as pd
import psycopg2
from yaml import safe_load
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text


class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            #df = pd.json_normalize(safe_load(f))
            cd = safe_load(f)
        return cd
    
    def init_db_engine(self):
        cd = self.read_db_creds()
        cd['RDS_PORT'] = str(cd['RDS_PORT'])
        self.db_engine = create_engine(cd['RDS_DATABASE_TYPE']+"+"+cd['RDS_DBAPI']+"://"+cd['RDS_USER']+":"+cd['RDS_PASSWORD']+"@"+cd['RDS_HOST']+":"+cd['RDS_PORT']+"/"+cd['RDS_DATABASE']) 
        print(self.db_engine)
        return self.db_engine
    
    def list_db_tables(self):
        inspector = inspect(self.db_engine)
        with self.db_engine.connect() as connection:
            
            #query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public'"
            #result = connection.execute()
            #list_of_tables = result.fetchall()
            return inspector.get_table_names()
    
    def upload_to_db(self,df,table_name):
        print(table_name)
        df.to_sql(table_name,self.db_engine,schema='public',if_exists='append')


dbconnector = DatabaseConnector()
dbconnector.init_db_engine()
dbconnector.list_db_tables()

