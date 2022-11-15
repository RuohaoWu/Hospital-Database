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

data = pd.read_csv(sys.argv[1])

with conn.transaction():
    for row in data.itertuples():
        hospital_pk = row.hospital_pk
        name = row.hospital_name
        city = row.city
        state = row.state
        address = row.address
        zip = row.zip
        fips = row.fips_code
        point = str(row.geocoded_hospital_address)
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
            with conn.transaction():
                cur.execute("insert into hospital(hospital_pk, hospital_name, city, state, address, zip, fips, longitude, latitude) "
                            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (hospital_pk, name, city, state, address, zip, fips, longitude, latitude))
        except Exception:
            pass
        # Below is for hospital_weekly
        date = row.collection_week
        adult_bed_avail = row.all_adult_hospital_beds_7_day_avg
        child_bed_avail = row.all_pediatric_inpatient_beds_7_day_avg
        adult_bed_used = row.all_adult_hospital_inpatient_bed_occupied_7_day_coverage
        child_bed_used = row.all_pediatric_inpatient_bed_occupied_7_day_avg
        all_icu_bed_avail = row.total_icu_beds_7_day_avg
        all_icu_bed_used = row.icu_beds_used_7_day_avg
        all_COVID_patient = row.inpatient_beds_used_covid_7_day_avg
        adult_icu_COVID_patient = row.staffed_icu_adult_patients_confirmed_covid_7_day_avg
        try:
            with conn.transaction():
                cur.execute("insert into hospital_weekly(hospital_pk, date, adult_bed_avail, child_bed_avail, adult_bed_used, child_bed_used, all_icu_bed_avail, all_icu_bed_used, all_COVID_patient, adult_icu_COVID_patient) "
                            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (hospital_pk, date, adult_bed_avail, child_bed_avail, adult_bed_used, child_bed_used, all_icu_bed_avail, all_icu_bed_used, all_COVID_patient, adult_icu_COVID_patient))
        except Exception:
            print("Error")
            break


conn.commit()
conn.close()
