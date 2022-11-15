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
quality_data = pd.read_csv(sys.argv[2])

# Ensure multiple queries run atomically (either all succeed or all fail)
with conn.transaction():
    for row in quality_data.itertuples():
        # Scan through every row in the csv and make it as tuple
        # row._1 means the first column of the csv file
        hospital_pk = row._1
        hospital_name = row._2
        address = row._3
        city = row._4
        state = row._5
        zip = row._6
        county = row._7
        type = row._9
        ownership = row._10
        emergency_avail = row._11
        # Covert Yes and No into boolean variable
        # If it doesn't have, keep null
        if emergency_avail == 'Yes':
            emergency_avail = True
        elif emergency_avail == 'No':
            emergency_avail = False
        else:
            emergency_avail = np.NaN
        quality = row._13
        # Convert String Not Available into NA
        if quality == "Not Available":
            quality = np.NaN
        # The date of the quality update by typing it in the command line
        date = sys.argv[1]
        try:
            # Here we use try-exception if there is an exception,
            # we catch it and do other query
            with conn.transaction():
                # Make a save point
                # Insert new hospitals that are in quality.csv
                # but not hhs.csv into hospital table
                # If it doesn't exist, insert into hospital
                # with hospital name, county, type, ownership...
                # If it already exist in hospital table
                # Update information only from quality csv about hospital
                cur.execute("insert into hospital (hospital_pk, hospital_name, \
                    city, county, state, address, zip, type, ownership, \
                        emergency_avail) "
                            "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (hospital_pk, hospital_name, city,
                             county, state, address, zip, type,
                             ownership, emergency_avail))
        except Exception:
            # If we catch the exception, we do other operation
            # If the hospital is already in the hospital table
            # Update more information from quality csv
            with conn.transaction():
                # Create savepoint
                try:
                    cur.execute("update hospital set county = %s, type = %s, \
                        ownership = %s, emergency_avail = %s \
                            where hospital_pk = %s ", (county, type, ownership,
                                                       emergency_avail,
                                                       hospital_pk))
                except Exception:
                    # If we catch exception, we do other operation
                    print("Error")
        try:
            # Insert data into hospital_quality table
            with conn.transaction():
                # Make a save point
                # Insert date quality rating for a hospital given a time
                cur.execute("insert into hospital_quality (hospital_pk, date, \
                    quality) "
                            "values (%s, %s, %s)", (hospital_pk, date,
                                                    quality))
        except Exception:
            # Need to do some operation when we catch an exception
            print("This is an error")
conn.commit()  # Commit what we did above if it runs
conn.close()  # Close the cursor
