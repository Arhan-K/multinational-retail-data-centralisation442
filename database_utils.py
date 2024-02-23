import yaml
from sqlalchemy import create_engine, MetaData
import psycopg2

class DatabaseConnector:
    def __init__(self):
            pass
    
    def read_db_creds(self):
        '''
        Uses Python's PyYAML library to read the credentials from the db_creds.yaml file and
        returns a dictionary of the credentials
        '''
        file_path = "C:/Users/arhan/OneDrive/Documents/AI_Core/MRDC/db_creds.yaml"
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def read_personal_creds(self):
        '''
        Uses Python's PyYAML library to read the credentials from the personal_creds.yaml file and
        returns a dictionary of the credentials
        '''
        file_path = "C:/Users/arhan/OneDrive/Documents/AI_Core/MRDC/personal_creds.yaml"
        with open(file_path, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    
    def init_db_engine(self):
        '''
        Reads the credentials from the return of read_db_creds and initialises and returns an 
        sqlalchemy database engine.
        '''
        credentials = self.read_db_creds()
        from sqlalchemy import create_engine
        username = credentials['RDS_USER']
        password = credentials['RDS_PASSWORD']
        host = credentials['RDS_HOST']
        port = credentials['RDS_PORT']
        database = credentials['RDS_DATABASE']
        url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        db_engine = create_engine(url)
        db_engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        db_engine.connect()
        return db_engine

    def list_db_tables(self):
        '''
        Lists all the tables in the database
        '''
        engine = self.init_db_engine()
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table_names = metadata.tables.keys()
        table_list = ['legacy_store_details', 'legacy_users', 'orders_table']
        return table_list
    
    def upload_to_db(self, df, table_name):
        '''
        Uploads the dataframe to the correct table using sqlalchemy

        Parameters:
        df: pandas dataframe
            The dataframe to be uploaded to the different table
        table_name: str
            The name of the table to which the dataframe must be uploaded
        '''
        engine = self.init_db_engine()
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table_names = metadata.tables.keys()
        personal_credentials = self.read_personal_creds()
        from sqlalchemy import create_engine
        username = personal_credentials['USER']
        password = personal_credentials['PASSWORD']
        host = personal_credentials['HOST']
        port = personal_credentials['PORT']
        database = personal_credentials['DATABASE']
        url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        df_engine = create_engine(url)
        #db_engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        #db_engine.connect()
        #print("upload")
        df.to_sql(table_name, df_engine, index = False, if_exists = 'replace')