CREATE TABLE transducer._TPERSON AS 
	SELECT DISTINCT ssn, name, city_name FROM transducer._SPERSON;

CREATE TABLE transducer._PERSON_PHONE AS
SELECT DISTINCT ssn, phone FROM transducer._SPERSON;

CREATE TABLE transducer._PERSON_EMAIL AS 
SELECT DISTINCT ssn, email FROM transducer._SPERSON;

CREATE TABLE transducer._TCITY AS 
SELECT DISTINCT city_name, country FROM transducer._SCITY;

CREATE TABLE transducer._CITY_COORD AS 
SELECT DISTINCT city_name, country, coordinates FROM transducer._SCITY WHERE coordinates IS NOT NULL;





/*BASE CONSTRAINTS*/

ALTER TABLE transducer._TPERSON ADD PRIMARY KEY (ssn);

ALTER TABLE transducer._PERSON_PHONE ADD PRIMARY KEY (ssn,phone);

ALTER TABLE transducer._PERSON_EMAIL ADD PRIMARY KEY (ssn,email);

ALTER TABLE transducer._TCITY ADD PRIMARY KEY (city_name);

ALTER TABLE transducer._CITY_COORD ADD PRIMARY KEY (city_name);



ALTER TABLE transducer._PERSON_PHONE 
ADD FOREIGN KEY (ssn) REFERENCES transducer._TPERSON(ssn);

ALTER TABLE transducer._PERSON_EMAIL 
ADD FOREIGN KEY (ssn) REFERENCES transducer._TPERSON(ssn);

ALTER TABLE transducer._TPERSON
ADD FOREIGN KEY (city_name) REFERENCES transducer._TCITY(city_name);

ALTER TABLE transducer._CITY_COORD
ADD FOREIGN KEY (city_name) REFERENCES transducer._TCITY(city_name);