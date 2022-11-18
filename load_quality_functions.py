"""Function module for load-quality.py"""

# Import Necessary Package
import psycopg
import credencials
import pandas as pd
import numpy as np


def read_data(datafile):
    """Read and and prepare hospital quality data"""

    # Connect to SQL server
    conn = psycopg  .connect(
        host="sculptor.stat.cmu.edu", dbname=credencials.DB_USER,
        user=credencials.DB_USER, password=credencials.DB_PASSWORD
    )

    cur = conn.cursor()
    # Read the file into python
    # Select only the columns we need and specify the data type of the columns
    # It will make the script read the data faster
    # and we have control of the data type

    quality_data = pd.read_csv(
        datafile, usecols=["Facility ID", "Facility Name", "Address",
                           "City", "State", "ZIP Code",
                           "County Name", "Hospital Type",
                           "Hospital Ownership",
                           "Emergency Services",
                           "Hospital overall rating"],
        dtype={"Facility ID": str,
               "Facility Name": str, "Address": str, "City": str,
               "State": str, "ZIP Code": str,
               "County Name": str, "Hospital Type": str,
               "Hospital Ownership": str,
               "Emergency Services": str, "Hospital overall rating": str})

    # Converting Not Available, NULL, empty string, NaN, and np.NaN value
    # in the csv into NULL that SQL regongnized so that
    # it wouldn't cause confusion
    quality_data = quality_data.replace("Not Available", None)
    quality_data = quality_data.replace('NULL', None)
    quality_data = quality_data.replace('', None)
    quality_data = quality_data.replace('NaN', None)
    quality_data = quality_data.replace(np.NaN, None)

    # Get Hospital_pk that are already in data set
    d = pd.read_sql_query("SELECT hospital_pk from hospital;", conn)

    # Set some inital value for new hospital we get when we run the script
    # Getting the hopital we update
    # The amount of rows that fail to insert or update
    return quality_data, d, conn, cur


def insert_data(quality_data, d, conn, cur, date_sys):
    """Insert data into hospitals database"""

    new_hospital = 0
    rows_updated = 0
    rows_failed = 0

    # Create two lists to capture
    # The index of the row which doesn't insert or update successfully
    error_indx = []
    error = []

    # Ensure multiple queries run atomically (either all succeed or all fail)
    with conn.transaction():
        for indx, row in quality_data.iterrows():
            # Reset Boolean Variables such that we can use it to count
            insert = False
            # Scan through every row in the csv and make it as tuple
            hospital_pk = row["Facility ID"]
            hospital_name = row["Facility Name"]
            address = row["Address"]
            city = row["City"]
            state = row["State"]
            zip = row["ZIP Code"]
            county = row["County Name"]
            type = row["Hospital Type"]
            ownership = row["Hospital Ownership"]
            emergency_avail = row["Emergency Services"]

            # Covert Yes and No into boolean variable
            # If it doesn't have, keep None
            if emergency_avail == 'Yes':
                emergency_avail = True
            elif emergency_avail == 'No':
                emergency_avail = False
            else:
                emergency_avail = None

            quality = row["Hospital overall rating"]
            # Covert all the quality into integer
            if isinstance(quality, str):
                quality = int(quality)

            # The date of the quality update by typing it in the command line
            date = date_sys

            try:
                # Here we use try-exception if there is an exception,
                # check if the hospital is in the database
                with conn.transaction():
                    # Make a savepoint
                    if (d["hospital_pk"] == hospital_pk).any():
                        # If the hospital pk was already in the table,
                        # we update information we are missing
                        # from quality table
                        cur.execute("update hospital set county = %s, type = %s, \
                            ownership = %s, emergency_avail = %s \
                                where hospital_pk = %s ", (county, type,
                                                           ownership,
                                                           emergency_avail,
                                                           hospital_pk))

                    else:
                        # If the hospital pk was not in the table,
                        # We should insert into the hospital table
                        cur.execute("insert into hospital (hospital_pk, hospital_name, \
                        city, county, state, address, zip, type, ownership, \
                            emergency_avail) "
                                    "values(%s, %s, %s, %s, %s, %s, %s, %s, \
                                        %s, %s)",
                                    (hospital_pk, hospital_name, city,
                                     county, state, address, zip, type,
                                     ownership, emergency_avail))
                        # When the insert works, turn insert variable into true
                        insert = True
                    # Regardless of update and insert works,
                    # we should always insert information in hospital_quality
                    cur.execute("insert into hospital_quality (hospital_pk, date, \
                    quality) " "values (%s, %s, %s)", (hospital_pk, date,
                                                       quality))
            # If there is an exception,
            # we capture it and save the rows in a csv file

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

    error_df = quality_data.loc[quality_data.index[error_indx]]

    error_df["Exception"] = error

    error_df.to_csv("failed_insert_HGI_Quality.csv", index=False)

    return rows_updated, new_hospital, rows_failed


def display_results(rows_updated, new_hospital, rows_failed):
    """Display a summary of the updates to the database"""

    # Summary
    summary1 = "Rows added to hospital_quality: " + str(rows_updated)
    summary2 = "New hospitals found: " + str(new_hospital)
    summary3 = "Rows that failed to insert " + str(rows_failed)

    print(summary1)
    print(summary2)
    print(summary3)
