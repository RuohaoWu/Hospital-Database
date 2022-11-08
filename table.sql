--Create a table named hospital
--Contain basic information of hospitals, these informations will never change until hospital relocation.
create table hospital(
    hospital_pk text PRIMARY KEY, --Primary key of hospitals, contains number and characters.
    hospital_name text NOT NULL, --The name of hospital, contains number and characters, it can't be NULL.
    city text, --The city that hospital locates in, contains characters.
    county text, --The county that hospital locates in, contains characters.
    state varchar(2), --The state that hospital locates in, shows the two letter abbreviation.
    address text, --The address of the hospital, contains number and characters.
    zip text, --The zip code of the hospital, contains number and characters.
    fips text, --The fips code of the hospital, unique among counties, contain number and characters.
    latitude float, --The latitude of the hospital, contain decimal numbers.
    longitude float, --The longitude of the hospital, contain decimal numbers.
    type text, --The type of hospitals, contain characters.
    ownership text, --The owner of hospitals, contain characters.
    emergency_availability boolean default NULL); --Whether hospital has emergency, T for yes, F for no.

--Create a table named hospital_info
--Contain the hospital resource information, and it will update these information weekly.
create table hospital_info(
    ID SERIAL PRIMARY KEY, --Primary key of keeping track of each hospital beds info each week.
    hospital_pk text, --Primary key of hospitals, contains number and characters.
    date DATE check(date<CURRENT_DATE), --The update date of the data, contains date.
    adult_bed_available float, --7-day average total number of beds available for adult in each week.
    child_bed_available float, --7-day average total number of beds available for child in each week.
    adult_bed_occupied float, --7-day average total number of beds in use for adult in each week.
    child_bed_occupied float, --7-day average total number of beds in use for child in each week.
    ICU_bed_available float, --7-day average total number of beds available in ICU in each week.
    ICU_bed_occupied float, --7-day average total number of beds in use in ICU in each week.
    COVID_patient float, --Number of patients hospitalized who confirmed COVID.
    ICU_COVID_patient float, --Number of ICU adult patients who confirmed COVID.
    FOREIGN KEY(hospital_pk) --hospital_pk occurs in other table, it can't be null in this table.
    REFERENCES hospital(hospital_pk) MATCH FULL); 

--Create a table named quality
--Contain the quality rating for hospitals, and the table will update several times a year.
create table hospital_quality(
    ID SERIAL PRIMARY KEY, --Primary key of keeping track of each hospital quality rating.
    hospital_pk text, --Primary key of hospitals, contains number and characters.
    quality numeric, --Quality ratings for hospitals, updates several times a year, contain numbers.
    date DATE, --The date that updates the hospital quality rating.
    FOREIGN KEY(hospital_pk) --hospital_pk occurs in other table, it can't be null in this table.
    REFERENCES hospital(hospital_pk) MATCH FULL);