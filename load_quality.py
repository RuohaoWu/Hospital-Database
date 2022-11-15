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

quality_data = pd.read_csv(sys.argv[2])

with conn.transaction():
    for row in quality_data.itertuples():
        hospital_pk = row._1
        hospital_name = row._2
        county = row._7
        type = row._9
        ownership = row._10
        emergency_avail = row._11
        if emergency_avail == 'Yes':
            emergency_avail = True
        elif emergency_avail == 'No':
            emergency_avail = False
        else:
            emergency_avail = np.NaN
        quality = row._13
        if quality == "Not Available":
            quality = np.NaN
        date = sys.argv[1]
        try:
            with conn.transaction():
                cur.execute("insert into hospital (hospital_pk, hospital_name, county, type, ownership, emergency_avail) "
                            "values(%s, %s, %s, %s, %s, %s)", (hospital_pk, hospital_name, county, type, ownership, emergency_avail))
        except Exception:
            with conn.transaction():
                try:
                    cur.execute("update hospital set county = %s, type = %s, ownership = %s, emergency_avail = %s where hospital_pk = %s ",
                    (county, type, ownership, emergency_avail, hospital_pk))
                except Exception:
                    print("Error")
        try:
            with conn.transaction():
                cur.execute("insert into hospital_quality (hospital_pk, date, quality) "
                            "values (%s, %s, %s)", (hospital_pk, date, quality))
        except Exception:
            print("This is an error")
conn.commit()
conn.close()
