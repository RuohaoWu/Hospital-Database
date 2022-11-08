CREATE TABLE hospital(
    hospital_pk VARCHAR(255) PRIMARY KEY, --hospital ID, unique, string 
    hospital_name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    county VARCHAR(50),
    state CHAR(2),
    address VARCHAR(100),
    zip VARCHAR(10),
    fips VARCHAR(20),
    lautitude numeric,
    longitude numeric,
    type VARCHAR(50),
    ownership VARCHAR(50),
    emergency_avail boolean
);

-- Create a table named hospital_weekly
CREATE TABLE hospital_weekly(
    ID SERIAL PRIMARY KEY,
    hospital_pk VARCHAR(255),
    date DATE NOT NULL CHECK (date <= now()),
    adult_bed_avail numeric,
    child_bed_avail numeric,
    adult_bed_used numeric,
    child_bed_used numeric,
    all_ICU_bed_avail numeric,
    all_ICU_bed_used numeric,
    all_COVID_patient numeric,
    adult_ICU_COVID_patient numeric,
    FOREIGN KEY (hospital_pk)
        REFERENCES hospital (hospital_pk) MATCH FULL
 );

-- Create a table named quality
CREATE TABLE hospital_quality(
    ID SERIAL PRIMARY KEY,
    hospital_pk VARCHAR(255),
    quality integer,
    date DATE NOT NULL,
    FOREIGN KEY (hospital_pk)
        REFERENCES hospital (hospital_pk) MATCH FULL
 );
