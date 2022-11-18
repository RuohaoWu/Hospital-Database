# Hospital-Database

We want to build a hospital database to keep track of hostpital info and other bed and quality information

1. User should first create three tables in a sql environemnt by running schema.sql with PostgreSQL

2. Credentials for the connection to PostgreSQL are controlled in the credencials.py module

3. The models load_hhs_functions.py and load_quality_functions.py contain functions for the driver code

4. Users should load hospital bed information by running the load-hhs.py module
	- eg: load-hhs.py 2022-01-04-hhs.data.csv
	- The function takes the csv file path as the only command line arguement
	- hospital and hospital_weekly tables are updated
		- hospital will only be added if not already in database
	- Rows unable to insert will be written to failed_insert_hhs.csv and printed on screen
	- Metrics for rows inserted, hospitals added, and rows failed will be printed when completed

5. Users should then load hospital quality module by running the load-quality.py module
	- eg: load-quality 2021-07-01 Hospital_General_Information-2021-07.csv
	- The function takes the take of the file as the first command line arguement
	- The function take the csv file path as the second command line arguement
	- hospital and hospital_quality tables are updated
		- hospital will only be added if not already in database
	- Rows unable to insert will be written to failed_insert_HGI_Quality.csv and printed on screen
	- Metrics for rows inserted, hospitals added, and rows failed will be printed on screen

6. Check in PostgreSQL that the data successfully loaded
