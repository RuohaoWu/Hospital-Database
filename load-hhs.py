# This python file will import the data into the sql table


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
data = pd.read_csv(sys.argv[1])

# Ensure multiple queries run atomically (either all succeed or all fail)
with conn.transaction():
    for row in data.itertuples():
        # Scan through every row in the csv and make it as tuple
        hospital_pk = row.hospital_pk
        name = row.hospital_name
        city = row.city
        state = row.state
        address = row.address
        zip = row.zip
        fips = row.fips_code
        point = str(row.geocoded_hospital_address)
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
        try:
            # Here we use try-exception if there is an exception,
            # we catch it and do other query
            with conn.transaction():
                # Make a savepoint
                cur.execute("insert into hospital(hospital_pk, hospital_name, \
                city, state, address, zip, fips, longitude, latitude) "
                            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (hospital_pk, name, city, state, address, zip,
                             fips, longitude, latitude))
        except psycopg.errors.UniqueViolation:
            # Here we need to catch exception and do something
            pass
        # Below is for hospital_weekly
        date = row.collection_week
        # If hopital_beds < 0, then we convert it into NA
        adult_bed_avail = row.all_adult_hospital_beds_7_day_avg
        if adult_bed_avail < 0:
            adult_bed_avail = np.NaN
        child_bed_avail = row.all_pediatric_inpatient_beds_7_day_avg
        if child_bed_avail < 0:
            child_bed_avail = np.NaN
        adult_bed_used = \
            row.all_adult_hospital_inpatient_bed_occupied_7_day_coverage
        if adult_bed_used < 0:
            adult_bed_used = np.NaN
        child_bed_used = row.all_pediatric_inpatient_bed_occupied_7_day_avg
        if child_bed_used < 0:
            child_bed_used = np.NaN
        all_icu_bed_avail = row.total_icu_beds_7_day_avg
        if all_icu_bed_avail < 0:
            all_icu_bed_avail = np.NaN
        all_icu_bed_used = row.icu_beds_used_7_day_avg
        if all_icu_bed_used < 0:
            all_icu_bed_used = np.NaN
        all_COVID_patient = row.inpatient_beds_used_covid_7_day_avg
        if all_COVID_patient < 0:
            all_COVID_patient = np.NaN
        adult_icu_COVID_patient = \
            row.staffed_icu_adult_patients_confirmed_covid_7_day_avg
        if adult_icu_COVID_patient < 0:
            adult_icu_COVID_patient = np.NaN
        try:
            with conn.transaction():
                # Insert data into our hospital_weekly table
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
            # Catch exception and do some operation when we catch it
            print("Exception is", e)


conn.commit()  # Commit what we did above if it runs
conn.close()  # Close the cursor
