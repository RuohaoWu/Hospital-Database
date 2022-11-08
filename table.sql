--Create a table named hospital
create table hospital(
    hospital_pk text PRIMARY KEY, --hospital ID, unique, string 
    hospital_name text NOT NULL,
    city text,
    county text,
    state varchar(2),
    address text,
    zip text,
    fips text,
    lautitude float,
    longitude float,
    type text,
    ownership text,
    emergency_availability boolean default NULL);

-- Create a table named hospital_info
create table hospital_info(
    ID SERIAL PRIMARY KEY,
    hospital_pk text,
    date DATE check(date<CURRENT_DATE),
    adult_bed_available float,
    child_bed_available float,
    adult_bed_occupied float,
    child_bed_occupied float,
    ICU_bed_available float,
    ICU_bed_occupied float,
    COVID_patient float,
    ICU_COVID_patient float,
    FOREIGN KEY(hospital_pk)
    REFERENCES hospital(hospital_pk) MATCH FULL);

-- Create a table named quality
create table hospital_quality(
    ID SERIAL PRIMARY KEY,
    hospital_pk stirng,
    quality numeric,
    date DATE,
    FOREIGN KEY(hospital_pk)
    REFERENCES hospital(hospital_pk) MATCH FULL);