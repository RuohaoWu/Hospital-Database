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
                                dtype = {
                                    "hospital_pk":str, 
                                "hospital_name":str, "city":str, "state":str,
                                "address":str, "zip":str, "geocoded_hospital_address":str, "collection_week": str, 
                                        "all_adult_hospital_beds_7_day_avg":float, "all_pediatric_inpatient_beds_7_day_avg":float,
                                        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage":float,
                                        "all_pediatric_inpatient_bed_occupied_7_day_avg":float,
                                        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage":float,
                                        "all_pediatric_inpatient_bed_occupied_7_day_avg":float,
                                        "total_icu_beds_7_day_avg":float, "icu_beds_used_7_day_avg":float,
                                        "inpatient_beds_used_covid_7_day_avg":float,
                                        "staffed_icu_adult_patients_confirmed_covid_7_day_avg":float})


data = data.replace(-999999, None)
data = data.replace('NULL', None)
data = data.replace('', None)
data = data.replace('NaN', None)
data = data.replace(np.NaN, None)

exception_df = pd.DataFrame(columns=["Exception"])

# Get Hospital_pk that are already in data set
d = pd.read_sql_query("SELECT hospital_pk from hospital;", conn)
new_hospital = 0
rows_updated = 0
rows_failed = 0

error_indx = []
error = []

# Ensure multiple queries run automically (either all succeed or all fail)
with conn.transaction():
    for indx, row in data.iterrows():
        # Reset Variables
        insert = False
        # Scan through every row in the csv and make it as tuple
        hospital_pk = row["hospital_pk"]
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
            longitude = None
            latitude = None
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
                        where hospital_pk = %s ", (fips, longitude, latitude,
                                                   hospital_pk))

                else:

                    cur.execute("insert into hospital(hospital_pk, hospital_name, \
                                city, state, address, zip, fips, longitude, \
                                    latitude) "
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

        except Exception as e:
            error_indx.append(indx)
            error.append(str(e))
            rows_failed += 1

            message = "Row Number: " + str(indx) + " failed to insert."
            print(message)
            print(str(e))

        else:
            if insert is True:
                new_hospital += 1

            rows_updated += 1 


conn.commit()
conn.close()

error_df = data.loc[data.index[error_indx]]

error_df["Exception"] = error

error_df.to_csv("failed_insert_hhs.csv", index=False)

# Summary
summary1 = "Rows added to hospital_weekly: " + str(rows_updated)
summary2 = "New hospitals found: " + str(new_hospital)
summary3 = "Rows that failed to insert " + str(rows_failed)

print(summary1)
print(summary2)
print(summary3)
