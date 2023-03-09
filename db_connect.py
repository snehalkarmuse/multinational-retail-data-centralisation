import pandas as pd
import psycopg2
from yaml import safe_load
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text


class DatabaseConnector:
    '''Read database credentials to get from yaml file.'''
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            #df = pd.json_normalize(safe_load(f))
            cd = safe_load(f)
        return cd
    
    '''his method calls yaml file with read database method. creates engine.'''
    def init_db_engine(self):
        cd = self.read_db_creds()
        cd['RDS_PORT'] = str(cd['RDS_PORT'])
        self.db_engine = create_engine(cd['RDS_DATABASE_TYPE']+"+"+cd['RDS_DBAPI']+"://"+cd['RDS_USER']+":"+cd['RDS_PASSWORD']+"@"+cd['RDS_HOST']+":"+cd['RDS_PORT']+"/"+cd['RDS_DATABASE']) 
        #print(self.db_engine)
        return self.db_engine
    
    '''This method check the engine is ready and connects it. This methods shows number of tables are in database (database name is in yaml file). Returns list of table names.'''
    def list_db_tables(self):
        inspector = inspect(self.db_engine)
        with self.db_engine.connect() as connection:
            
            #query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public'"
            #result = connection.execute()
            #list_of_tables = result.fetchall()
            return inspector.get_table_names()
        
    '''This method takes table name and dataframe.Upload it where schema is public and we are replacing the data instead of appending it. Not adding index to records.'''
    def upload_to_db(self,df,table_name):
        df.to_sql(table_name,self.db_engine,schema='public',if_exists='replace',index = False)

'''created instance of the class DatabaseConnector. Initializing the engine. Lising tables in the database'''
dbconnector = DatabaseConnector()
dbconnector.init_db_engine()
table_names = dbconnector.list_db_tables()

