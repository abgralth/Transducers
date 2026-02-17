/* TESTING AREA */

SELECT * FROM transducer._SPERSON
SELECT * FROM transducer._SCITY
SELECT ssn, name, phone, email, city_name, country, coordinates 
FROM transducer._SPERSON
NATURAL LEFT OUTER JOIN transducer._SCITY

SELECT * FROM transducer._SPERSON_INSERT
SELECT * FROM transducer._SCITY_INSERT
SELECT ssn, name, phone, email, city_name, country, coordinates 
FROM transducer._SPERSON_INSERT
NATURAL LEFT OUTER JOIN transducer._SCITY_INSERT

SELECT * FROM transducer._SPERSON_INSERT_JOIN
SELECT * FROM transducer._SCITY_INSERT_JOIN
SELECT ssn, name, phone, email, city_name, country, coordinates 
FROM transducer._SPERSON_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._SCITY_INSERT_JOIN


SELECT * FROM transducer._TPERSON
SELECT * FROM transducer._PERSON_PHONE
SELECT * FROM transducer._PERSON_EMAIL
SELECT * FROM transducer._TCITY
SELECT * FROM transducer._CITY_COORD
SELECT ssn, name, phone, email, city_name, country, coordinates 
FROM transducer._TPERSON
NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
NATURAL LEFT OUTER JOIN transducer._TCITY
NATURAL LEFT OUTER JOIN transducer._CITY_COORD

SELECT * FROM transducer._TPERSON_INSERT
SELECT * FROM transducer._PERSON_PHONE_INSERT
SELECT * FROM transducer._PERSON_EMAIL_INSERT
SELECT * FROM transducer._TCITY_INSERT
SELECT * FROM transducer._CITY_COORD_INSERT
SELECT ssn, name, phone, email, city_name, country, coordinates 
FROM transducer._TPERSON_INSERT
NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT
NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT
NATURAL LEFT OUTER JOIN transducer._TCITY_INSERT
NATURAL LEFT OUTER JOIN transducer._CITY_COORD_INSERT

SELECT * FROM transducer._TPERSON_INSERT_JOIN
SELECT * FROM transducer._PERSON_PHONE_INSERT_JOIN
SELECT * FROM transducer._PERSON_EMAIL_INSERT_JOIN
SELECT * FROM transducer._TCITY_INSERT_JOIN
SELECT * FROM transducer._CITY_COORD_INSERT_JOIN
SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates 
FROM transducer._TPERSON_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._TCITY_INSERT_JOIN
NATURAL LEFT OUTER JOIN transducer._CITY_COORD_INSERT_JOIN
WHERE ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
AND city_name IS NOT NULL AND country IS NOT NULL


/* UPDATES */

/*INSERT FROM S*/
INSERT INTO transducer._SPERSON VALUES ('ssn1', 'June',  'phone12', 'mail11', 'Paris');
INSERT INTO transducer._SPERSON VALUES ('ssn1', 'June',  'phone11', 'mail12', 'Paris');
INSERT INTO transducer._SPERSON VALUES ('ssn4', 'Jagg',  'phone41', 'mail41', 'New York');

BEGIN;
INSERT INTO transducer._SCITY VALUES ('Roma', 'Italy', 'coord2');
INSERT INTO transducer._SPERSON VALUES ('ssn5', 'Jiff',  'phone51', 'mail51', 'Roma');
END;

/*DELETE FROM S*/

DELETE FROM transducer._SPERSON WHERE ssn = 'ssn2';
DELETE FROM transducer._SPERSON WHERE ssn = 'ssn1' AND phone = 'phone11';

BEGIN;
INSERT INTO transducer._loop VALUES(3);
DELETE FROM transducer._SPERSON WHERE ssn = 'ssn3';
DELETE FROM transducer._SCITY WHERE city_name = 'New York';
END;


/*INSERT FROM T*/
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn4', 'phone42');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn4', 'mail42');

BEGIN;
INSERT INTO transducer._loop VALUES (5);
INSERT INTO transducer._TCITY VALUES ('Bolzano', 'Italy');
INSERT INTO transducer._TPERSON VALUES ('ssn6', 'Japk', 'Bolzano');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn6', 'phone61');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn6', 'mail61');
END;

BEGIN;
INSERT INTO transducer._loop VALUES (6);
INSERT INTO transducer._TCITY VALUES ('Verona', 'Italy');
INSERT INTO transducer._CITY_COORD VALUES ('Verona', 'Italy', 'coord3');
INSERT INTO transducer._TPERSON VALUES ('ssn7', 'Jumb', 'Verona');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn7', 'phone71');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn7', 'mail71');
END;

BEGIN;
INSERT INTO transducer._loop VALUES (4);
INSERT INTO transducer._TPERSON VALUES ('ssn8', 'Jahn', 'Verona');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn8', 'phone81');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn8', 'mail81');
END;

INSERT INTO transducer._PERSON_PHONE VALUES ('ssn8', 'phone82');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn7', 'mail72');

/*DELETE FROM T*/

DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn8' AND phone = 'phone81';

BEGIN;
INSERT INTO transducer._loop VALUES(4);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn7';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn7';
DELETE FROM transducer._TPERSON WHERE ssn = 'ssn7';
END;

BEGIN;
INSERT INTO transducer._loop VALUES(6);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn8';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn8';
DELETE FROM transducer._TPERSON WHERE ssn = 'ssn8';
DELETE FROM transducer._CITY_COORD WHERE city_name = 'Verona';
DELETE FROM transducer._TCITY WHERE city_name = 'Verona';
END;


ROLLBACK;
