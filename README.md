# Project Description
This project entailed extracting, cleaning, processing, and analysing retail data for a Multinational Company selling various goods in different locations around the globe. The data was provided in a form that was spread across different sources and formats, which rendered a detailed study and analysis of the data difficult. The objective of this project was to compile all the sales data from various sources into a coherent database so that it can be analysed easily in order to extract crucial insights to inform business decisions.
This project gave me a great introduction to the field of data science and data analysis, by providing me with a simulated experience of analysing data to extract insights for retail purposes.

# File Structure:
The code for this project is organised into 3 python files which perform various stages of the process:
* data_extraction.py - This file uses the class 'DataExtractor' containing methods that extract data from various types of data sources, namely, CSV files, Web APIs, and an Amazon Web Services EC2 S3 bucket.
* database_utils.py - This file connects to the database and uploads data to a new database by use of the DatabaseConnector class.
* data_cleaning.py - This class uses the 'DataCleaning' class containing methods to analyse and clean data from each of the data sources, getting rid of erroneous values and correcting errors with formatting.

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

# Technologies Used:
* Python (including the libraries Pandas, NumPy)
* PostgreSQL
* AWS
* SQLalchemy
* psycopg2
* tabula-py
* YAML

The files in the project can be executed by running the following command:

python {filename.py}
'''

# Connecting to the Database and Extracting Data:
The database_utils.py file contains the DatabaseConnector() class with the method read_db_creds() which loads the login credentials used to connect to the engine and obtain the names of the tables that need to be extracted. These credentials are then input into the engine to establish a secure connection by use of the init_db_engine() method.

The method list_db_tables() obtains the names of the tables that will be extracted for  this project.

The upload_to_db() method then uploads the selected tables to the PgAdmin4 database.

The DataExtractor() class in the script data_extraction.py contains the method reads_rds_table() which reads and returns the data from the tables provided in the AWS RDS bucket, namely orders_table and dim_users in the form of a pandas DataFrame.

Similarly, the retrieve_pdf_data(), extract_from_s3(), and extract_from_json() methods in the same class return pandas DataFrames for the data stored as a pdf link and in an s3 bucket, and json link, respectively. This returns the tables.

The method list_number_of_stores() returns the number of stores from the API link, which is then used by the retrieve_stores_data() method to return a DataFrame for the stores_data table. <br>

def list_number_of_stores(self,store_number_endpoint, header_dict):
        response = requests.get(store_number_endpoint, headers = header_dict)
        if response.status_code == 200:
            data = response.json()
            number_extracted_stores = data['number_stores']
            return number_extracted_stores  
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
  
