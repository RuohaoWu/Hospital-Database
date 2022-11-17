"""Clean and Import HHS data to the SQL Hospital Database"""


# Import Necessary Package
import psycopg
import credencials
import pandas as pd
import sys
import numpy as np

conn = psycopg  .connect(
    host="sculptor.stat.cmu.edu", dbname=credencials.DB_USER,
    user=credencials.DB_USER, password=credencials.DB_PASSWORD
)

cur = conn.cursor()

# Read the file into python

data = pd.read_csv(sys.argv[1], usecols=["hospital_pk", "hospital_name" ,"city",  "state", "address", "zip",
                                        "fips_code", "geocoded_hospital_address", "collection_week", 
                                        "all_adult_hospital_beds_7_day_avg", "all_pediatric_inpatient_beds_7_day_avg",
                                        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
                                        "all_pediatric_inpatient_bed_occupied_7_day_avg",
                                        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
                                        "all_pediatric_inpatient_bed_occupied_7_day_avg",
                                        "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg",
                                        "inpatient_beds_used_covid_7_day_avg",
                                        "staffed_icu_adult_patients_confirmed_covid_7_day_avg"],
                                dtype = {"hospital_pk":str})


data.fillna('NULL')

### Set Data Types                                     
### Replace -99999 values to NaN
### Error with NaN values


# Get Hospital_pk that are already in data set
d = pd.read_sql_query("SELECT hospital_pk from hospital;", conn)
new_hospital = 0
updated_hospital = 0

# Ensure multiple queries run automically (either all succeed or all fail)
with conn.transaction():
    for indx, row in data.iterrows():
        # Reset Variables
        update = False
        insert = False
        # Scan through every row in the csv and make it as tuple
        hospital_pk = row["hospital_pk"]
        print(hospital_pk)
        name = row["hospital_name"]
        city = row["city"]
        state = row["state"]
        address = row["address"]
        zip = row["zip"]
        fips = row["fips_code"]
        point = str(row["geocoded_hospital_address"])
        # Seperate point into latitude and longitude
        x = point.find("(")
        y = point.find(")")
        if x == -1 or y == -1:
            longitude = np.NaN
            latitude = np.NaN
        else:
            point = point[x+1:y]
            location = point.split(" ")
            longitude = location[0]
            latitude = location[1]
        date = row.collection_week
        # If hopital_beds < 0, then we convert it into NA
        adult_bed_avail = row.all_adult_hospital_beds_7_day_avg
        child_bed_avail = row.all_pediatric_inpatient_beds_7_day_avg
        adult_bed_used = \
            row.all_adult_hospital_inpatient_bed_occupied_7_day_coverage
        child_bed_used = row.all_pediatric_inpatient_bed_occupied_7_day_avg
        all_icu_bed_avail = row.total_icu_beds_7_day_avg
        all_icu_bed_used = row.icu_beds_used_7_day_avg
        all_COVID_patient = row.inpatient_beds_used_covid_7_day_avg
        adult_icu_COVID_patient = \
            row.staffed_icu_adult_patients_confirmed_covid_7_day_avg
        try:
            # Here we use try-exception if there is an exception,
            # check if the hospital is in the database
            with conn.transaction():
                # Make a savepoint
                if (d["hospital_pk"] == hospital_pk).any():
            
                    cur.execute("update hospital set fips = %s, longitude = %s, latitude = %s\
                        where hospital_pk = %s ", (fips, longitude, latitude, hospital_pk))
                    
                    
                    update = True
                else:

                    cur.execute("insert into hospital(hospital_pk, hospital_name, \
                                city, state, address, zip, fips, longitude, latitude) "
                                "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (hospital_pk, name, city, state, address, zip,
                                fips, longitude, latitude))

                    insert = True

                
                cur.execute("insert into hospital_weekly(hospital_pk, date, \
                    adult_bed_avail, child_bed_avail, adult_bed_used, \
                        child_bed_used, all_icu_bed_avail, all_icu_bed_used, \
                            all_COVID_patient, adult_icu_COVID_patient) "
                            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (hospital_pk, date, adult_bed_avail,
                             child_bed_avail, adult_bed_used, child_bed_used,
                             all_icu_bed_avail, all_icu_bed_used,
                             all_COVID_patient, adult_icu_COVID_patient))

        except Exception as e :
            print(e)
            #data[index:]
        
        else:
            print("Yes")
            if update == True:
                new_hospital += 1
            else:
                updated_hospital += 1

            

conn.commit()
conn.close()

print(new_hospital, updated_hospital)