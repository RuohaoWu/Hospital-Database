# Hospital-Database

We want to build a hospital database to keep track of hostpital info and other information

1. User should first create three tables in a sql environemnt

2. User should have the hhs and quality csv file ready

3. User should first load hhs csv file by doing:
eg:python load-hhs.py 2022-01-04-hhs-data.csv

4. After loading hhs.csv file, load quality data given a time in command line argument
eg:python load-quality.py 2021-07-01 Hospital_General_Information-2021-07.csv

5. Check in sql that the data successfully loaded