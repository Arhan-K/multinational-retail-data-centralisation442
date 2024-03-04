# Project Description
This project entailed extracting, cleaning, processing, and analysing retail data for a Multinational Company selling various goods in different locations around the globe. The data was provided in a form that was spread across different sources and formats, which rendered a detailed study and analysis of the data difficult. The objective of this project was to compile all the sales data from various sources into a coherent database so that it can be analysed easily in order to extract crucial insights to inform business decisions.
This project gave me a great introduction to the field of data science and data analysis, by providing me with a simulated experience of analysing data to extract insights for retail purposes.

# File Structure:
The code for this project is organised into 3 python files which perform various stages of the process. Apart from this, there is a PostgreSQL file containing different queries to obtain different aspects of the data, and 2 yaml files containing login credentials for the database engine and for personal login credentials. The files are described below:
* data_extraction.py - This file uses the class 'DataExtractor' containing methods that extract data from various types of data sources, namely, CSV files, Web APIs, and an Amazon Web Services EC2 S3 bucket.
* database_utils.py - This file connects to the database and uploads data to a new database by use of the DatabaseConnector class.
* data_cleaning.py - This class uses the 'DataCleaning' class containing methods to analyse and clean data from each of the data sources, getting rid of erroneous values and correcting errors with formatting.

The files in the project can be executed by running the following command:
```
python {filename.py}
```


# Technologies Used:
* Python (including the libraries Pandas, NumPy)
* PostgreSQL
* AWS
* SQLalchemy
* psycopg2
* tabula-py
* YAML



# Data Sources:
The retail data was organised  into various different sources in different formats which were then compiled and consolidated into a common database. The data sources used were:

## AWS RDS Database
The data tables orders_table containing historical sales data, and dim_users containing user data were extracted from an AWS RDS database using methods defined in the DataExtractor and DatabaseConnector classes.

## AWS EC2 S3 Bucket
The data tables dim_products and dim_date_times were extracted from a CSV file stored in an AWS EC2 S3 Bucket using methods defined in the DataExtractor class, with the help of the boto3 library. The data from the bucket was then converted into a pandas DataFrame in order to be studied and cleaned.

## AWS PDF Link
The table dim_card_details was extracted from a PDF file stored in an AWS S3 bucket, utilising the library tabula. The data was then converted into a pandas DatFrame for data cleaning and analysis.

## RESTful API
The dim_store_details table was extracted in the form of a JSON file from a Web API endpoint using HTTP GET requests.

# Connecting to the Database and Extracting Data:
The database_utils.py file contains the DatabaseConnector() class with the method read_db_creds() which loads the login credentials used to connect to the engine and obtain the names of the tables that need to be extracted. These credentials are then input into the engine to establish a secure connection by use of the init_db_engine() method.
```
def init_db_engine(self):
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
```

The method list_db_tables() obtains the names of the tables that will be extracted for  this project.
```
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
```

The upload_to_db() method then uploads the selected tables to the PgAdmin4 database.
```
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
```

The DataExtractor() class in the script data_extraction.py contains the method reads_rds_table() which reads and returns the data from the tables provided in the AWS RDS bucket, namely orders_table and dim_users in the form of a pandas DataFrame.
```
def reads_rds_table(self):
        dc = database_utils.DatabaseConnector()
        rds_engine = dc.init_db_engine()
        table_list = dc.list_db_tables
        store_details_query = "SELECT * FROM legacy_store_details;"
        user_query = "SELECT * FROM legacy_users;"
        orders_query = "SELECT * FROM orders_table;"
        store_df = pd.read_sql(store_details_query, rds_engine)
        user_df = pd.read_sql(user_query, rds_engine)
        orders_df = pd.read_sql(orders_query, rds_engine)
        return [user_df, store_df, orders_df]
```

Similarly, the retrieve_pdf_data(), extract_from_s3(), and extract_from_json() methods in the same class return pandas DataFrames for the data stored as a pdf link and in an s3 bucket, and json link, respectively. This returns the tables.
```
def retrieve_pdf_data(self, pdf_link):  
        tabular_data = tabula.read_pdf(pdf_link, pages = 'all', multiple_tables = True)
        df = pd.concat(tabular_data, ignore_index=True)
        return df
```

```
def extract_from_s3(self, s3_address):
        access_key = 'AKIAUW2XISLH2YEKUQ4L'
        secret_key = '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'        
        bucket_name = "data-handling-public"
        object_key = "products.csv"
        s3 = boto3.client('s3')#, access_key, secret_key)
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        df.to_csv('s3_csv.csv', index = False)
        return df
```


```
def extract_from_json(self, json_link):        
        df = pd.read_json(json_link)
        return df
```


The method list_number_of_stores() returns the number of stores from the API link, which is then used by the retrieve_stores_data() method to return a DataFrame for the stores_data table. 
```
def list_number_of_stores(self,store_number_endpoint, header_dict):
        response = requests.get(store_number_endpoint, headers = header_dict)
        if response.status_code == 200:
            data = response.json()
            number_extracted_stores = data['number_stores']
            return number_extracted_stores  
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
```

```
def retrieve_stores_data(self, retrieve_store_endpoint, header_dict, number_stores):
        store_data_list = []
        for i in range(0, number_stores):
            response = requests.get(f"{retrieve_store_endpoint}{i}", headers = header_dict)
            if response.status_code == 200:
                data = response.json()
                store_data_list.append(data)
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        store_data_df = pd.DataFrame(store_data_list)
        return store_data_df
```

# Data Cleaning
The data_cleaning.py file contains the DataCleaning() class which contains methods for cleaning the dataframes obtained from the DataExtractor class. A separate method is used for each table, which cleans specific erroneous values pertaining to them and returns a cleaned dataframe. 
* The clean_user_data() and clean_card_data() methods remove incorrect and NULL values, and errors with dates. 
* The clean_store_data() method corrects spelling mistakes and imprecise values for the country_code and continent, and removes the latitude column. 
* The clean_products_data() method applies the appropriate conversion formulae to convert all values for the weights into kg and adds missing country_code values based on the locality of the stores. 
* The clean_orders_data() column gets rids of the first_name and last_name columns, as well as the erroneous 1 column. 
* The clean_dates_data() method removes erroneous values in the 'year' column.

The cleaned dataframes are then uploaded to the database with the upload_to_db() method used by creating an instance of the DatabaseConnector() class in the database_utils.py script.

# Querying the Data
The orders_table_alter.sql file contains lists of different queries that should be run in order to return the required results from the database. The function of each block of queries is mentioned above the queries in the doscstring.
