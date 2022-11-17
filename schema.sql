/* 
Team Blackbirds
Sam Kalman
Harry Shao
Ruohao Wu
*/

/* The basic entities in our schema are basic information about hospitals,
weekly medical resources of the hospital, and the quality rating of
each hospital. */


/* This table contains the basic information for each hospital, table is
immutable. Nothing in this table should be updated until the hospital
relocates or changes the owner. */

/* Create a table named hospital */
CREATE TABLE hospital(
    hospital_pk VARCHAR(255) PRIMARY KEY,
    hospital_name VARCHAR(255) NOT NULL,
    city VARCHAR(100) Default NULL,
    county VARCHAR(50) Default NULL,
    state CHAR(2) Default NULL,
    address VARCHAR(100) Default NULL,
    zip VARCHAR(10) Default NULL,
    fips VARCHAR(20) Default NULL,
    longitude numeric Default NULL,
    latitude numeric Default NULL,
    type VARCHAR(50) Default NULL,
    ownership VARCHAR(50) Default NULL,
    emergency_avail boolean Default NULL
);


/* This table contains the weekly medical resources of the hospital, the
variables on this table will be counted and updated every week for each
hospital, recording the average number of hospital beds used, the average
number of ICU beds used, and so on. */

/* Create a table named hospital_weekly */
CREATE TABLE hospital_weekly(
    ID SERIAL PRIMARY KEY,
    hospital_pk VARCHAR(255),
    date DATE NOT NULL CHECK (date <= now()),
    adult_bed_avail numeric CHECK (adult_bed_avail >= 0),
    child_bed_avail numeric CHECK (child_bed_avail >= 0),
    adult_bed_used numeric CHECK (adult_bed_used >= 0),
    child_bed_used numeric CHECK (child_bed_used >= 0),
    all_ICU_bed_avail numeric CHECK (all_ICU_bed_avail >= 0),
    all_ICU_bed_used numeric CHECK (all_ICU_bed_used >= 0),
    all_COVID_patient numeric CHECK (all_COVID_patient >= 0),
    adult_ICU_COVID_patient numeric CHECK (adult_ICU_COVID_patient >= 0),
    FOREIGN KEY (hospital_pk)
        REFERENCES hospital (hospital_pk) MATCH FULL
 );

/* The third table contains the quality rating of each hospital, this table
is updated several times a year. */

/* Create a table named quality */
CREATE TABLE hospital_quality(
    ID SERIAL PRIMARY KEY,
    hospital_pk VARCHAR(255),
    quality numeric,
    date DATE NOT NULL,
    FOREIGN KEY (hospital_pk)
        REFERENCES hospital (hospital_pk) MATCH FULL
 );

 /* As comments show, we created all three tables based on the updating rate of
variables. Variables with the same update rate are placed in the same table. */

/* The table does not have any redundant variables, because we create the table
through the update rate of the variable, and the variables with the same update
rate are placed in the same table. The only variable that overlaps in the three
tables is 'hospital_pk', the 'hospital_pk' of the second and third tables are
the reference of the first table. Without 'hospital_pk', we cannot join these
three tables. Hence, although 'hospital_pk' is present in all three tables, it
is not redundant.
