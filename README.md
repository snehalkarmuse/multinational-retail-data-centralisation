![alt text](https://docs.toonboom.com/help/harmony-12/paint/Content/Resources/Images/HAR/Stage/Network/Animation-network.jpg)
# multinational-retail-data-centralisation
- Created three classes, DataExtractor, DataConnector, DataCleaning.
- Imported yaml module.
- Created read_db_creds method in dataconnector, to read yaml file called db_creds.yaml. 
- Created connection to Postgresql.
- Created the method called init_db_engine. To initilize the engine(SQLalchemy).
- This method shows list of tables in the database. 
- Getting data from various sources.
- Created method called read_db_table in dataextractor class, which reads the table and convert that into dataframe.
- To get data from tabula, Passed the link. Read the data and converted into dataframe.
- To get data from API, supplied api link and authentication key to get data. 
- Here we call json method on response and converted it into dataframe. This method gives number of stores are in the database.
- Retrieve_store_data method created for taking the data from the API. Which is interface which takes request and gives response. 
  To get the data, needs url and header info or authentication key. Here we call json method on response and converted it into dataframe
  This method takes the specific store number and converts store details into dataframe.
- add_data_to_dataframe method created for taking the data from the API. Which is interface which takes request and gives response. 
  This method is taking all store in for loop and retrives the details. appending it in one dataframe which created above.
- extract_from_s3 methods takes data from the AWS S3. which is data pool where files are stored. For this created user on AWS s3. Important information is 
  username, password, region (selected US) which suppourts all data. After that downloaded awscli, aws configure and boto. imported boto in program.
  The link gets the file on the aws s3. Created s3 object. convered json response into dataframe. For product data.
- extract_from_json_file method extract data from json file.
- cleaned data using Pandas. Upload it into appropriate table.
