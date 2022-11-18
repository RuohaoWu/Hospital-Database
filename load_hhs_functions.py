"""Function module for load-hhspy"""


# Import Necessary Package
import psycopg
import credencials
import pandas as pd
import sys
import numpy as np


def read_data(datafile):
    """Read and and prepare hospital data"""

    # Connect to the sql server
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=credencials.DB_USER,
        user=credencials.DB_USER, password=credencials.DB_PASSWORD
    )

    cur = conn.cursor()

    # Read the file into python
    # Select only the columns we need and specify the data type of the columns
    # It will make the script read the data faster
    # and we have control of the data type

    data = pd.read_csv(datafile,
                    usecols=[
                        "hospital_pk", "hospital_name",
                        "city",  "state", "address", "zip",
                        "fips_code",
                        "geocoded_hospital_address",
                        "collection_week",
                        "all_adult_hospital_beds_7_day_avg",
                        "all_pediatric_inpatient_beds_7_day_avg",
                        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
                        "all_pediatric_inpatient_bed_occupied_7_day_avg",
                        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
                        "all_pediatric_inpatient_bed_occupied_7_day_avg",
                        "total_icu_beds_7_day_avg",
                        "icu_beds_used_7_day_avg",
                        "inpatient_beds_used_covid_7_day_avg",
                        "staffed_icu_adult_patients_confirmed_covid_7_day_avg"],
                    dtype={"hospital_pk": str, "hospital_name": str,
                            "city": str, "state": str,
                            "address": str, "zip": str,
                            "geocoded_hospital_address": str,
                            "collection_week": str,
                            "all_adult_hospital_beds_7_day_avg": float,
                            "all_pediatric_inpatient_beds_7_day_avg": float,
                            "all_adult_hospital_inpatient_bed_\
                            occupied_7_day_coverage": float,
                            "all_pediatric_inpatient_bed_occupied_7_\
                            day_avg": float,
                            "all_adult_hospital_inpatient_bed_occupied\
                            _7_day_coverage": float,
                            "all_pediatric_inpatient_bed_occupied_7_day\
                            _avg": float,
                            "total_icu_beds_7_day_avg": float,
                            "icu_beds_used_7_day_avg": float,
                            "inpatient_beds_used_covid_7_day_avg": float,
                            "staffed_icu_adult_patients_confirmed_\
                            covid_7_day_avg": float})

    # Converting -999999, NULL, empty string, NaN, and np.NaN value in the csv
    # Into NULL that SQL regongnized so that it wouldn't cause confusion
    data = data.replace(-999999, None)
    data = data.replace('NULL', None)
    data = data.replace('', None)
    data = data.replace('NaN', None)
    data = data.replace(np.NaN, None)

    # Get Hospital_pk that are already in data set
    d = pd.read_sql_query("SELECT hospital_pk from hospital;", conn)
    # Set some inital value for new hospital we get when we run the script
    # Getting the hopital we update
    # The amount of rows that fail to insert or update

    return data, d, conn, cur


def insert_data(data, d, conn, cur):
    """Insert data into hospital database"""

    new_hospital = 0
    rows_updated = 0
    rows_failed = 0

    # Create two lists to capture
    # The index of the row which doesn't insert or update successfully
    error_indx = []
    error = []

    # Ensure multiple queries run automically (either all succeed or all fail)
    with conn.transaction():
        for indx, row in data.iterrows():
            # Reset Boolean Variables such that we can use it to count
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
                        # If the hospital pk was already in the table,
                        # we update information we are missing from quality table
                        cur.execute("update hospital set fips = %s, longitude = %s, latitude = %s\
                            where hospital_pk = %s ", (fips, longitude, latitude,
                                                    hospital_pk))

                    else:
                        # If the hospital pk was not in the table,
                        # We should insert into the hospital table
                        cur.execute("insert into hospital(hospital_pk, hospital_name, \
                                    city, state, address, zip, fips, longitude, \
                                        latitude) "
                                    "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (hospital_pk, name, city, state, address, zip,
                                    fips, longitude, latitude))
                        # When the insert works, turn insert variable into true
                        insert = True
                    # Regardless of update and insert works,
                    # we should always insert information in hospital_weekly
                    cur.execute("insert into hospital_weekly(hospital_pk, date, \
                        adult_bed_avail, child_bed_avail, adult_bed_used, \
                            child_bed_used, all_icu_bed_avail, all_icu_bed_used, \
                                all_COVID_patient, adult_icu_COVID_patient) "
                                "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (hospital_pk, date, adult_bed_avail,
                                child_bed_avail, adult_bed_used, child_bed_used,
                                all_icu_bed_avail, all_icu_bed_used,
                                all_COVID_patient, adult_icu_COVID_patient))
            # If there is an exception,
            # we capture it and save the rows in a csv file
            except Exception as e:
                # Append the row index when the row fail to insert or update
                error_indx.append(indx)
                # Append the error information when there is an exception
                error.append(str(e))
                # The rows that fail to insert adds one more
                rows_failed += 1
                # Print out the index of the row fail to insert
                message = "Row Number: " + str(indx) + " failed to insert."
                print(message)
                print(str(e))
            # if insert change from false to true
            # It means we insert one more hospital into hospital table
            # If it does not, it means we update one more hospital
            else:
                if insert is True:
                    new_hospital += 1

                rows_updated += 1

    # Commit what we did above and close the sql connnection
    conn.commit()
    conn.close()
    # Make a subdataframe to obtain rows which fail to insert or update
    error_df = data.loc[data.index[error_indx]]
    # Add a column into error_df which is the exception we capture for
    # each row that fail to insert or update
    error_df["Exception"] = error
    # Turn the dataframe into a csv where user can see the information about the
    # failed insertation row and what error occurs
    error_df.to_csv("failed_insert_hhs.csv", index=False)

    return rows_updated, new_hospital, rows_failed


def display_results(rows_updated, new_hospital, rows_failed):
    """Display a summary of the updates to the database"""

    # Summary of  how many rows added to table hospital_weekly
    # How many new hospital it insert into hospital table
    # How many rows that fail to insert
    summary1 = "Rows added to hospital_weekly: " + str(rows_updated)
    summary2 = "New hospitals found: " + str(new_hospital)
    summary3 = "Rows that failed to insert " + str(rows_failed)

    print(summary1)
    print(summary2)
    print(summary3)
