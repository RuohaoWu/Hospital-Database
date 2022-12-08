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
	- The function takes the csv file path as the second command line arguement
	- hospital and hospital_quality tables are updated
		- hospital will only be added if not already in database
	- Rows unable to insert will be written to failed_insert_HGI_Quality.csv and printed on screen
	- Metrics for rows inserted, hospitals added, and rows failed will be printed on screen

***Note: Steps 4 and 5 can be executed in any order. Both steps will load new hospitals
***Note: There are checks in place to not allow a users to upload twice in one week

6. Check in PostgreSQL that the data successfully loaded

7. Create the Analytics Dashboard for the week
	- Run the following two commands in the command line

	papermill weekly-report.ipynb 2022-09-30-report.ipynb -p week 2022-09-30
	jupyter nbconvert --no-input --to html 2022-09-30-report.ipynb

	- The week values will need to be changed each week

	- The report will generate the following information

	1. A table summary of how many hospital beds records were uploaded
		- Compared to the most recent 4 week
		- The current week is in bold
	2. A table summary of the breakdown of beds used and available between children and adult as well as covid patients
		- Compared to the most recent 4 weeks
		- The current week is in bold
	3. A table summary of the breakdown in the percentage of beds used by hospital quality rating
		- The most recent quality rating for each hospital is used
		- Compared to the most recent 4 weeks
		- The highest quality rating is in bold
	4. A plot and summary table of the total number of beds used broken down into all cases and COVID cases
		- All time comparison
		- The current week is in bold
	5. A heatmap of COVID cases by state
		- The interaction map gets cutoff when coverted from html to pdf
		- The html version give the user a tooltip to view summary statistics when hovering over each state
		- Only include COVID cases from most recent week
	6. A table summary of the top 10 states with the largest COVID case increase from the week prior
	7. A table summary of the top 10 hospitals with the largest COVID case increase from the week prior
		- City and State of the Hospital is included

	- Final output is an html file
	- User will need to manually convert if they want to work with a pdf
