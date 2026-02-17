DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._SPERSON
    (
      ssn VARCHAR(100) NOT NULL,
      name VARCHAR(100) NOT NULL,
      phone VARCHAR(100) NOT NULL,
      email VARCHAR(100) NOT NULL,
      city_name VARCHAR(100) NOT NULL
    );


CREATE TABLE transducer._SCITY
    (
      city_name VARCHAR(100) NOT NULL,
      country VARCHAR(100) NOT NULL,
      coordinates VARCHAR(100),
      mayor VARCHAR(100) NOT NULL
    );

ALTER TABLE transducer._SCITY ADD PRIMARY KEY (city_name, mayor);
ALTER TABLE transducer._SPERSON ADD PRIMARY KEY (ssn,phone,email);
/*
Annoying FK restrictions, perhaps an artisal IND between composite PKs is an alternative
ALTER TABLE transducer._SPERSON ADD FOREIGN KEY (city_name) REFERENCES transducer._SCITY(city_name);
*/

CREATE OR REPLACE FUNCTION transducer.check_SPERSON_mvd_fn_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT DISTINCT r1.ssn, r2.name, r1.phone, r1.email, r2.city_name 
         FROM transducer._SPERSON AS r1,
         (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.city_name) AS r2
            WHERE  r1.ssn = r2.ssn 
         EXCEPT
         SELECT *
         FROM transducer._SPERSON
         ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON PHONE AND EMAIL %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_SPERSON_mvd_fn_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS   (SELECT DISTINCT r1.ssn, r2.name, r1.phone, NEW.email, r2.city_name 
            FROM transducer._SPERSON AS r1, (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.city_name) AS r2
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT DISTINCT r1.ssn, r2.name, NEW.phone, r1.email, r2.city_name 
            FROM transducer._SPERSON AS r1, (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.city_name) AS r2
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._SPERSON)) THEN
      RAISE NOTICE 'THE TUPLE % LEAD TO ADITIONAL ONES', NEW;
      INSERT INTO transducer._SPERSON 
            (SELECT DISTINCT r1.ssn, r2.name, r1.phone, NEW.email, r2.city_name 
            FROM transducer._SPERSON AS r1, (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.city_name) AS r2
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT DISTINCT r1.ssn, r2.name, NEW.phone, r1.email, r2.city_name 
            FROM transducer._SPERSON AS r1, (SELECT NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.city_name) AS r2
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._SPERSON));
      RETURN NEW;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_SCITY_mvd_fn_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT DISTINCT r1.city_name, r2.country, r2.coordinates, r1.mayor
         FROM transducer._SCITY AS r1,
         (SELECT NEW.city_name, NEW.country, NEW.coordinates, NEW.mayor) AS r2
            WHERE  r1.city_name = r2.city_name 
         EXCEPT
         SELECT *
         FROM transducer._SCITY
         ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON MAYOR %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;



CREATE OR REPLACE TRIGGER SPERSON_mvd_trigger_1
BEFORE INSERT ON transducer._SPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_SPERSON_mvd_fn_1();

CREATE OR REPLACE TRIGGER SPERSON_mvd_trigger_2
AFTER INSERT ON transducer._SPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_SPERSON_mvd_fn_2();

CREATE OR REPLACE TRIGGER SCITY_mvd_trigger_1
BEFORE INSERT ON transducer._SCITY
FOR EACH ROW
EXECUTE FUNCTION transducer.check_SCITY_mvd_fn_1();

INSERT INTO transducer._SCITY (city_name, country, coordinates, mayor) VALUES
('Paris', 'France', 'coord1', 'mayor11'),
('New York', 'US', NULL,'mayor21'),
('New York', 'US', NULL,'mayor22')
;

INSERT INTO transducer._SPERSON (ssn, name, phone, email, city_name) VALUES
('ssn1', 'June',  'phone11', 'mail11', 'Paris'),
('ssn2', 'Jovial',  'phone21', 'mail21', 'Paris'),
('ssn3', 'Jord',  'phone31', 'mail31', 'New York')
;


/* /////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

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

CREATE TABLE transducer._CITY_MAYOR AS 
SELECT DISTINCT city_name, mayor FROM transducer._SCITY;



/*BASE CONSTRAINTS*/

ALTER TABLE transducer._TPERSON ADD PRIMARY KEY (ssn);

ALTER TABLE transducer._PERSON_PHONE ADD PRIMARY KEY (ssn,phone);

ALTER TABLE transducer._PERSON_EMAIL ADD PRIMARY KEY (ssn,email);

ALTER TABLE transducer._TCITY ADD PRIMARY KEY (city_name);

ALTER TABLE transducer._CITY_COORD ADD PRIMARY KEY (city_name);

ALTER TABLE transducer._CITY_MAYOR ADD PRIMARY KEY (city_name, mayor);


ALTER TABLE transducer._PERSON_PHONE 
ADD FOREIGN KEY (ssn) REFERENCES transducer._TPERSON(ssn);

ALTER TABLE transducer._PERSON_EMAIL 
ADD FOREIGN KEY (ssn) REFERENCES transducer._TPERSON(ssn);

ALTER TABLE transducer._TPERSON
ADD FOREIGN KEY (city_name) REFERENCES transducer._TCITY(city_name);

ALTER TABLE transducer._CITY_COORD
ADD FOREIGN KEY (city_name) REFERENCES transducer._TCITY(city_name);

ALTER TABLE transducer._CITY_MAYOR
ADD FOREIGN KEY (city_name) REFERENCES transducer._TCITY(city_name);


/* ////////////////////////////////////////////////////////////////////////////////////////////////////////// */

/* S -> T */

CREATE TABLE transducer._SPERSON_INSERT AS
SELECT * FROM transducer._SPERSON
WHERE 1<>1;

CREATE TABLE transducer._SCITY_INSERT AS
SELECT * FROM transducer._SCITY
WHERE 1<>1;

CREATE TABLE transducer._SPERSON_DELETE AS
SELECT * FROM transducer._SPERSON
WHERE 1<>1;

CREATE TABLE transducer._SCITY_DELETE AS
SELECT * FROM transducer._SCITY
WHERE 1<>1;


CREATE TABLE transducer._SPERSON_INSERT_JOIN AS
SELECT * FROM transducer._SPERSON
WHERE 1<>1;

CREATE TABLE transducer._SCITY_INSERT_JOIN AS
SELECT * FROM transducer._SCITY
WHERE 1<>1;

CREATE TABLE transducer._SPERSON_DELETE_JOIN AS
SELECT * FROM transducer._SPERSON
WHERE 1<>1;

CREATE TABLE transducer._SCITY_DELETE_JOIN AS
SELECT * FROM transducer._SCITY
WHERE 1<>1;




/** INSERT T -> S **/

CREATE TABLE transducer._TPERSON_INSERT AS
SELECT * FROM transducer._TPERSON
WHERE 1<>1;

CREATE TABLE transducer._TPERSON_DELETE AS
SELECT * FROM transducer._TPERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_INSERT AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_DELETE AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_INSERT AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_DELETE AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;

CREATE TABLE transducer._TCITY_INSERT AS
SELECT * FROM transducer._TCITY
WHERE 1<>1;

CREATE TABLE transducer._TCITY_DELETE AS
SELECT * FROM transducer._TCITY
WHERE 1<>1;

CREATE TABLE transducer._CITY_COORD_INSERT AS
SELECT * FROM transducer._CITY_COORD
WHERE 1<>1;

CREATE TABLE transducer._CITY_COORD_DELETE AS
SELECT * FROM transducer._CITY_COORD
WHERE 1<>1;

CREATE TABLE transducer._CITY_MAYOR_INSERT AS
SELECT * FROM transducer._CITY_MAYOR
WHERE 1<>1;

CREATE TABLE transducer._CITY_MAYOR_DELETE AS
SELECT * FROM transducer._CITY_MAYOR
WHERE 1<>1;



CREATE TABLE transducer._TPERSON_INSERT_JOIN AS
SELECT * FROM transducer._TPERSON
WHERE 1<>1;

CREATE TABLE transducer._TPERSON_DELETE_JOIN AS
SELECT * FROM transducer._TPERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_INSERT_JOIN AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_PHONE_DELETE_JOIN AS
SELECT * FROM transducer._PERSON_PHONE
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_INSERT_JOIN AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;

CREATE TABLE transducer._PERSON_EMAIL_DELETE_JOIN AS
SELECT * FROM transducer._PERSON_EMAIL
WHERE 1<>1;

CREATE TABLE transducer._TCITY_INSERT_JOIN AS
SELECT * FROM transducer._TCITY
WHERE 1<>1;

CREATE TABLE transducer._TCITY_DELETE_JOIN AS
SELECT * FROM transducer._TCITY
WHERE 1<>1;

CREATE TABLE transducer._CITY_COORD_INSERT_JOIN AS
SELECT * FROM transducer._CITY_COORD
WHERE 1<>1;

CREATE TABLE transducer._CITY_COORD_DELETE_JOIN AS
SELECT * FROM transducer._CITY_COORD
WHERE 1<>1;

CREATE TABLE transducer._CITY_MAYOR_INSERT_JOIN AS
SELECT * FROM transducer._CITY_MAYOR
WHERE 1<>1;

CREATE TABLE transducer._CITY_MAYOR_DELETE_JOIN AS
SELECT * FROM transducer._CITY_MAYOR
WHERE 1<>1;


CREATE TABLE transducer._LOOP (
loop_start INT NOT NULL );

/* /////////////////////////////////////////////////////////////////////////////////////////////////////////// */


/** S->T INSERTS **/

CREATE OR REPLACE FUNCTION transducer.source_SPERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   /*Testing shortcuts for MVD tables PERSON_PHONE and PERSON_EMAIL*/
   
   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      INSERT INTO transducer._loop VALUES (1);
      INSERT INTO transducer._TPERSON VALUES (NEW.ssn, NEW.name, NEW.city_name) ON CONFLICT (ssn) DO NOTHING;
      INSERT INTO transducer._PERSON_PHONE VALUES (NEW.ssn, NEW.phone) ON CONFLICT (ssn,phone) DO NOTHING;
      INSERT INTO transducer._PERSON_EMAIL VALUES (NEW.ssn, NEW.email) ON CONFLICT (ssn,email) DO NOTHING;
      DELETE FROM transducer._loop;
   ELSE
      INSERT INTO transducer._SPERSON_INSERT VALUES(NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.city_name);
   END IF;
   RETURN NEW;
END IF;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.source_SCITY_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      /*We actually need to specify this section as to not allow INSERTs of new cities devoid of persons*/
      INSERT INTO transducer._loop VALUES(1);
      IF EXISTS (SELECT * FROM transducer._SPERSON WHERE city_name = NEW.city_name)
      THEN   
         INSERT INTO transducer._CITY_MAYOR VALUES (NEW.city_name, NEW.mayor) ON CONFLICT (city_name,mayor) DO NOTHING;
      ELSE
         DELETE FROM transducer._SCITY WHERE city_name = NEW.city_name;
      END IF;
      DELETE FROM transducer._loop;
   ELSE
      INSERT INTO transducer._SCITY_INSERT VALUES(NEW.city_name, NEW.country, NEW.coordinates, NEW.mayor);
   END IF;
   RETURN NEW;
END IF;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.source_SPERSON_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
FROM transducer._SPERSON_INSERT
NATURAL LEFT OUTER JOIN transducer._SCITY);

INSERT INTO transducer._SCITY_INSERT_JOIN (SELECT city_name, country, coordinates, mayor FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._SPERSON_INSERT_JOIN (SELECT ssn, name, phone, email, city_name FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;

END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_SCITY_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
FROM transducer._SCITY_INSERT
NATURAL LEFT OUTER JOIN transducer._SPERSON);

INSERT INTO transducer._SCITY_INSERT_JOIN (SELECT city_name, country, coordinates, mayor FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._SPERSON_INSERT_JOIN (SELECT ssn, name, phone, email, city_name FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;

END;  $$;


CREATE OR REPLACE FUNCTION transducer.SOURCE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Something got added in a JOIN table';
IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'But now is not the time to generate the query';
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on TARGET';

   create temporary table UJT(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._SPERSON_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._SCITY_INSERT_JOIN)
   WHERE 
   ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
   AND city_name IS NOT NULL AND country IS NOT NULL AND mayor IS NOT NULL);

   /*I believe that as expected, we will encounter tuples of the form {a,b,c,d,e,f,g}, {a,b,c,d,e,f,NULL}.
   Now, following the proper method, we should use a loop to ensure that this case doesn't occur, but, too lazy,
   don't want to.*/

   /*IFs are mostly required for null values, so I believe that for most tables in T, we can just write*/


   INSERT INTO transducer._TCITY (SELECT DISTINCT city_name, country FROM UJT
                                         WHERE city_name IS NOT NULL AND country IS NOT NULL) 
                                          ON CONFLICT (city_name) DO NOTHING;

   INSERT INTO transducer._TPERSON (SELECT DISTINCT ssn, name, city_name FROM UJT 
                              WHERE ssn IS NOT NULL AND name IS NOT NULL AND city_name IS NOT NULL) 
                              ON CONFLICT (ssn) DO NOTHING;

   INSERT INTO transducer._PERSON_PHONE (SELECT DISTINCT ssn, phone FROM UJT
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,phone) DO NOTHING;

   INSERT INTO transducer._PERSON_EMAIL (SELECT DISTINCT ssn, email FROM UJT
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,email) DO NOTHING;

   INSERT INTO transducer._CITY_MAYOR (SELECT DISTINCT city_name, mayor FROM UJT
                              WHERE city_name IS NOT NULL AND mayor IS NOT NULL) 
                              ON CONFLICT (city_name,mayor) DO NOTHING;

   /*Only for CITY_COORD do we use an IF*/

   IF EXISTS (SELECT * FROM UJT WHERE coordinates IS NOT NULL) THEN
      INSERT INTO transducer._CITY_COORD (SELECT DISTINCT city_name, mayor FROM UJT
                              WHERE city_name IS NOT NULL AND mayor IS NOT NULL) 
                              ON CONFLICT (city_name, mayor) DO NOTHING;
   END IF;

   DELETE FROM transducer._SPERSON_INSERT;
   DELETE FROM transducer._SPERSON_INSERT_JOIN;
   DELETE FROM transducer._SCITY_INSERT;
   DELETE FROM transducer._SCITY_INSERT_JOIN;

   DELETE FROM transducer._loop;
   DELETE FROM UJT;
   DROP TABLE UJT;

   RETURN NEW;
END IF;
END;  $$;


/** S->T DELETES **/

CREATE OR REPLACE FUNCTION transducer.source_SPERSON_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN OLD;
ELSE

   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      IF EXISTS (SELECT ssn, phone, email FROM transducer._SPERSON
                  EXCEPT (SELECT OLD.ssn, OLD.phone, OLD.email))
      THEN
         INSERT INTO transducer._loop VALUES (1);
        /*DELETE FROM PERSON_PHONE*/
         IF NOT EXISTS (SELECT * FROM transducer._SPERSON WHERE ssn = OLD.ssn AND phone = OLD.phone
           EXCEPT(SELECT * FROM transducer._SPERSON 
                  WHERE ssn = OLD.ssn AND phone = OLD.phone AND email = OLD.email ))
         THEN
            DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT OLD.ssn, OLD.phone);
         END IF;
         /*DELETE FROM PERSON_EMAIL*/
         IF NOT EXISTS (SELECT * FROM transducer._SPERSON WHERE ssn = OLD.ssn AND email = OLD.email
           EXCEPT(SELECT * FROM transducer._SPERSON 
                  WHERE ssn = OLD.ssn AND email = OLD.email AND phone = OLD.phone ))
         THEN
            DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT OLD.ssn, OLD.email);
         END IF;

         /*DELETE FROM ALL PERSON RELATED TABLES*/
         IF NOT EXISTS (SELECT ssn, phone, email FROM transducer._SPERSON
               WHERE ssn = OLD.ssn EXCEPT  SELECT OLD.ssn, OLD.phone, OLD.email)
         THEN
            DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT OLD.ssn, OLD.phone);
            DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT OLD.ssn, OLD.email);
            DELETE FROM transducer._TPERSON WHERE (ssn) IN (SELECT OLD.ssn);
         END IF;

         DELETE FROM transducer._loop;
      END IF;      
   ELSE
      INSERT INTO transducer._SPERSON_DELETE VALUES(OLD.ssn, OLD.name, OLD.phone, OLD.email, OLD.city_name);
   END IF;
   RETURN OLD;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_SCITY_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN OLD;
ELSE
   RAISE NOTICE 'Starting DELETE from CITY';
   /*I'm sure the struggle we face DELETing a single mayor can be solve easily via this type of shortcuts*/
   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      INSERT INTO transducer._loop VALUES(1);
      DELETE FROM transducer._CITY_MAYOR WHERE city_name = OLD.city_name AND mayor = OLD.mayor;
      DELETE FROM transducer._loop;
   ELSE
      INSERT INTO transducer._SCITY_DELETE VALUES(OLD.city_name, OLD.country, OLD.coordinates, OLD.mayor);
   END IF;
   RETURN OLD;
END IF;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.source_SPERSON_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
FROM transducer._SPERSON_DELETE
NATURAL LEFT OUTER JOIN transducer._SCITY);

INSERT INTO transducer._SCITY_DELETE_JOIN (SELECT city_name, country, coordinates, mayor FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._SPERSON_DELETE_JOIN (SELECT ssn, name, phone, email, city_name FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;

END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_SCITY_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates, mayor
FROM transducer._SCITY_DELETE
NATURAL LEFT OUTER JOIN transducer._SPERSON);

INSERT INTO transducer._SCITY_DELETE_JOIN (SELECT city_name, country, coordinates, mayor FROM temp_table);
INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._SPERSON_DELETE_JOIN (SELECT ssn, name, phone, email, city_name FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;

END;  $$;


CREATE OR REPLACE FUNCTION transducer.SOURCE_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
RAISE NOTICE 'Something got added in a JOIN table';
IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'But now is not the time to generate the query';
   RETURN NULL;
END IF;
   
RAISE NOTICE 'This should conclude with an DELETE on TARGET';

create temporary table UJT(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._SPERSON_DELETE_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._SCITY_DELETE_JOIN)
   WHERE 
   ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
   AND city_name IS NOT NULL AND country IS NOT NULL AND mayor IS NOT NULL);

/*PHONE*/
IF EXISTS (SELECT * FROM transducer._SPERSON as r1, UJT WHERE r1.ssn = UJT.ssn
           EXCEPT(SELECT * FROM transducer._SPERSON as r1, UJT 
            WHERE r1.ssn = UJT.ssn AND r1.phone = UJT.phone ))
THEN
   DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT ssn, phone FROM UJT);
END IF;

/*EMAIL*/
IF EXISTS (SELECT * FROM transducer._SPERSON as r1, UJT WHERE r1.ssn = UJT.ssn
           EXCEPT(SELECT * FROM transducer._SPERSON as r1, UJT 
            WHERE r1.ssn = UJT.ssn AND r1.email = UJT.email ))
THEN
   DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT ssn, email FROM UJT);
END IF;

/*MAYOR*/
IF EXISTS (SELECT * FROM transducer._SCITY as r1, UJT WHERE r1.city_name = UJT.city_name
           EXCEPT(SELECT * FROM transducer._SCITY as r1, UJT 
            WHERE r1.city_name = UJT.city_name AND r1.mayor = UJT.mayor ))
THEN
   RAISE NOTICE 'Is this the problem?';
   DELETE FROM transducer._CITY_MAYOR WHERE (city_name, mayor) IN (SELECT city_name, mayor FROM UJT);
END IF;

RAISE NOTICE 'Next';
/*We first need to check if the delete will cause the loss of a _SPERSON, or only of its PHONE or EMAIL*/
IF NOT EXISTS (SELECT r1.ssn, r1.phone, r1.email FROM transducer._SPERSON as r1, UJT
               WHERE r1.ssn = UJT.ssn
               EXCEPT
               SELECT ssn, phone, email FROM UJT)
THEN
RAISE NOTICE 'Loss of a person';
/*We then check if deleting a person require deleting a city*/
IF EXISTS (SELECT ssn, city_name, country FROM transducer._SPERSON
           NATURAL LEFT OUTER JOIN transducer._SCITY
           EXCEPT
           (SELECT ssn, city_name, country FROM UJT)) 
THEN
   RAISE NOTICE 'The city should be preserved, however?';
   DELETE FROM transducer._PERSON_PHONE WHERE (ssn) IN (SELECT ssn FROM UJT);
   DELETE FROM transducer._PERSON_EMAIL WHERE (ssn) IN (SELECT ssn FROM UJT);
   DELETE FROM transducer._TPERSON WHERE (ssn) IN (SELECT ssn FROM UJT);
ELSE
   RAISE NOTICE 'AYAYAYAY';
   /*Do we need to raze a city, or just delete a mayor?*/
   IF NOT EXISTS (SELECT r1.city_name, r1.mayor FROM transducer._SCITY AS r1, UJT WHERE r1.city_name = UJT.city_name
              EXCEPT SELECT city_name, mayor FROM UJT)
   THEN
      RAISE NOTICE 'GOOD';
      DELETE FROM transducer._CITY_MAYOR WHERE (city_name) IN (SELECT city_name FROM UJT);
   ELSE
      RAISE NOTICE 'BAD, in this specific context notwhistanding';
      /*If the city has coordinates, we also need to delete the tuple from _CITY_COORD*/
      IF EXISTS (SELECT coordinates FROM UJT WHERE coordinates IS NOT NULL) 
      THEN
         DELETE FROM transducer._PERSON_PHONE WHERE (ssn) IN (SELECT ssn FROM UJT);
         DELETE FROM transducer._PERSON_EMAIL WHERE (ssn) IN (SELECT ssn FROM UJT);
         DELETE FROM transducer._TPERSON WHERE (ssn) IN (SELECT ssn FROM UJT);
         DELETE FROM transducer._CITY_COORD WHERE (city_name) IN (SELECT city_name FROM UJT);
         DELETE FROM transducer._CITY_MAYOR WHERE (city_name) IN (SELECT city_name FROM UJT);
         DELETE FROM transducer._TCITY WHERE (city_name) IN (SELECT city_name FROM UJT);
         
      ELSE
         DELETE FROM transducer._PERSON_PHONE WHERE (ssn) IN (SELECT ssn FROM UJT);
         DELETE FROM transducer._PERSON_EMAIL WHERE (ssn) IN (SELECT ssn FROM UJT);
         DELETE FROM transducer._TPERSON WHERE (ssn) IN (SELECT ssn FROM UJT);
         DELETE FROM transducer._CITY_MAYOR WHERE (city_name) IN (SELECT city_name FROM UJT);
         DELETE FROM transducer._TCITY WHERE (city_name) IN (SELECT city_name FROM UJT);
      END IF;
   END IF; 
END IF;
END IF;


DELETE FROM transducer._SPERSON_DELETE;
DELETE FROM transducer._SPERSON_DELETE_JOIN;
DELETE FROM transducer._SCITY_DELETE;
DELETE FROM transducer._SCITY_DELETE_JOIN;

DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END;  $$;



/** T->S INSERTS **/


CREATE OR REPLACE FUNCTION transducer.target_TPERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _TPERSON';
   INSERT INTO transducer._TPERSON_INSERT VALUES(NEW.ssn, NEW.name, NEW.city_name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_PHONE';
   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      INSERT INTO transducer._loop VALUES (-1);

      INSERT INTO transducer._SPERSON (SELECT r1.ssn, r1.name, phone, email, r1.city_name 
                        FROM transducer._TPERSON as r1
                        NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
                        NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
                        WHERE r1.ssn = NEW.ssn AND phone = NEW.phone);
      DELETE FROM transducer._loop;
   ELSE
      INSERT INTO transducer._PERSON_PHONE_INSERT VALUES(NEW.ssn, NEW.phone);
   END IF;
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PERSON_EMAIL';
   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      INSERT INTO transducer._loop VALUES (-1);

      INSERT INTO transducer._SPERSON (SELECT r1.ssn, r1.name, phone, email, r1.city_name 
                        FROM transducer._TPERSON as r1
                        NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
                        NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
                        WHERE r1.ssn = NEW.ssn AND email = NEW.email);
      DELETE FROM transducer._loop;
   ELSE
      INSERT INTO transducer._PERSON_EMAIL_INSERT VALUES(NEW.ssn, NEW.email);
   END IF;
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TCITY_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _TCITY';
   INSERT INTO transducer._TCITY_INSERT VALUES(NEW.city_name, NEW.country);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COORD_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _CITY_COORD';
   INSERT INTO transducer._CITY_COORD_INSERT VALUES(NEW.city_name, NEW.country, NEW.coordinates);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_MAYOR_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _CITY_MAYOR';
   INSERT INTO transducer._CITY_MAYOR_INSERT VALUES(NEW.city_name, NEW.mayor);
   RETURN NEW;
END IF;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.target_TPERSON_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._TPERSON_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_INSERT_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._PERSON_PHONE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_INSERT_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._PERSON_EMAIL_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_INSERT_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TCITY_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._TCITY_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_INSERT_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COORD_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._CITY_COORD_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_INSERT_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_MAYOR_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._CITY_MAYOR_INSERT
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_INSERT_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_insert_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN

RAISE NOTICE 'Function transducer.target_insert_fn called';

SELECT count(*) INTO v_loop from transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _PERSON';
        
create temporary table UJT(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._TPERSON_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_PHONE_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_EMAIL_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._TCITY_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._CITY_COORD_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._CITY_MAYOR_INSERT_JOIN)
   WHERE 
   ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
   AND city_name IS NOT NULL AND country IS NOT NULL AND mayor IS NOT NULL);


/*Yeah, here again it's a bit annoying as we need to prevent redundant tuples and only INSERT tuples where 
  coordinates is non-null, when it actually is */

IF EXISTS (SELECT * FROM UJT 
         EXCEPT (SELECT * FROM UJT WHERE coordinates IS NULL)) THEN
      DELETE FROM UJT WHERE coordinates IS NULL;    
END IF;

INSERT INTO transducer._SCITY (SELECT DISTINCT city_name, country, coordinates, mayor FROM UJT
                              WHERE city_name IS NOT NULL) ON CONFLICT (city_name,mayor) DO NOTHING;


INSERT INTO transducer._SPERSON (SELECT DISTINCT ssn, name, phone, email, city_name FROM UJT 
                              WHERE ssn IS NOT NULL AND city_name IS NOT NULL) 
                              ON CONFLICT (ssn,phone,email) DO NOTHING;


DELETE FROM transducer._TPERSON_INSERT;
DELETE FROM transducer._PERSON_PHONE_INSERT;
DELETE FROM transducer._PERSON_EMAIL_INSERT;
DELETE FROM transducer._TCITY_INSERT;
DELETE FROM transducer._CITY_COORD_INSERT;
DELETE FROM transducer._CITY_MAYOR_INSERT;

DELETE FROM transducer._TPERSON_INSERT_JOIN;
DELETE FROM transducer._PERSON_PHONE_INSERT_JOIN;
DELETE FROM transducer._PERSON_EMAIL_INSERT_JOIN;
DELETE FROM transducer._TCITY_INSERT_JOIN;
DELETE FROM transducer._CITY_COORD_INSERT_JOIN;
DELETE FROM transducer._CITY_MAYOR_INSERT_JOIN;

DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END IF;
END;    $$;



/** T->S DELETES **/


CREATE OR REPLACE FUNCTION transducer.target_TPERSON_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _TPERSON';
   INSERT INTO transducer._TPERSON_DELETE VALUES(OLD.ssn, OLD.name, OLD.city_name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _PERSON_PHONE';
   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      
      IF EXISTS( SELECT ssn, phone FROM transducer._SPERSON WHERE ssn = OLD.ssn
              EXCEPT SELECT OLD.ssn, OLD.phone) 
      THEN
         INSERT INTO transducer._loop VALUES (-1);
         DELETE FROM transducer._SPERSON WHERE (ssn, phone) IN (SELECT OLD.ssn, OLD.phone);
         DELETE FROM transducer._loop;
      END IF;

   ELSE
      INSERT INTO transducer._PERSON_PHONE_DELETE VALUES(OLD.ssn, OLD.phone);
   END IF;
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _PERSON_EMAIL';
   IF NOT EXISTS (SELECT * FROM transducer._loop) 
   THEN
      IF EXISTS( SELECT ssn, phone FROM transducer._SPERSON WHERE ssn = OLD.ssn
              EXCEPT SELECT OLD.ssn, OLD.email) 
      THEN
         INSERT INTO transducer._loop VALUES (-1);
         DELETE FROM transducer._SPERSON WHERE (ssn, email) IN (SELECT OLD.ssn, OLD.email);
         DELETE FROM transducer._loop;
      END IF;
   ELSE
      INSERT INTO transducer._PERSON_EMAIL_DELETE VALUES(OLD.ssn, OLD.email);
   END IF;
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TCITY_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _TCITY';
   INSERT INTO transducer._TCITY_DELETE VALUES(OLD.city_name, OLD.country);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COORD_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _CITY_COORD';
   INSERT INTO transducer._CITY_COORD_DELETE VALUES(OLD.city_name, OLD.country, OLD.coordinates);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_MAYOR_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _CITY_MAYOR';
   INSERT INTO transducer._CITY_MAYOR_DELETE VALUES(OLD.city_name, OLD.mayor);
   RETURN NEW;
END IF;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_TPERSON_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._TPERSON_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_DELETE_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._PERSON_PHONE_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_DELETE_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._PERSON_EMAIL_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_DELETE_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TCITY_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._TCITY_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_DELETE_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_COORD_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._CITY_COORD_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_MAYOR);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_DELETE_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_CITY_MAYOR_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._CITY_MAYOR_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
INSERT INTO transducer._CITY_MAYOR_DELETE_JOIN (SELECT city_name, mayor FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_DELETE_fn()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
DECLARE
v_loop INT;
BEGIN

RAISE NOTICE 'Function transducer.target_DELETE_fn called';

SELECT count(*) INTO v_loop from transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an DELETE on _PERSON';
        
create temporary table UJT(
   ssn VARCHAR(100),
   name VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   city_name VARCHAR(100),
   country VARCHAR(100),
   coordinates VARCHAR(100),
   mayor VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates, mayor
   FROM transducer._TPERSON_DELETE_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_PHONE_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_EMAIL_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._TCITY_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._CITY_COORD_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._CITY_MAYOR_DELETE_JOIN)
   WHERE 
   ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
   AND city_name IS NOT NULL AND country IS NOT NULL);

/*Yeah, here again it's a bit annoying as we need to prevent redundant tuples and only DELETE tuples where 
  coordinates is non-null, when it actually is */

IF EXISTS (SELECT * FROM UJT 
         EXCEPT (SELECT * FROM UJT WHERE coordinates IS NULL)) THEN
      DELETE FROM UJT WHERE coordinates IS NULL;    
END IF;


/*As always, the DELETion is vibe-based as we still miss a proper theory for it.
  It's not difficult however is this situation to see that only two scenario exists, one where a delete
  over _SPERSON is done independently, and one in which it brings a _SCITY along its fall
  As we now have a MVD in SCITY, this step become a bit more intricate as it is also possible to independently 
  UPDATE ONLY SCITY.*/

IF EXISTS( SELECT ssn, city_name, country FROM transducer._SPERSON
           NATURAL LEFT OUTER JOIN transducer._SCITY
           EXCEPT
           (SELECT ssn, city_name, country FROM UJT)) THEN
   DELETE FROM transducer._SPERSON WHERE (ssn, phone, email) IN (SELECT DISTINCT ssn,phone,email FROM UJT);
ELSE
   /*I believe the check for SCITY independent UPDATE should be here*/
   IF EXISTS (SELECT r1.city_name, r1.mayor FROM transducer._SCITY AS r1, UJT WHERE r1.city_name = UJT.city_name
              EXCEPT SELECT city_name, mayor FROM UJT)
   THEN
      DELETE FROM transducer._SCITY WHERE (city_name, mayor) IN (SELECT DISTINCT city_name, mayor FROM UJT);
   ELSE
      DELETE FROM transducer._SPERSON WHERE (ssn, phone, email) IN (SELECT DISTINCT ssn,phone,email FROM UJT);   
      DELETE FROM transducer._SCITY WHERE (city_name, country) IN (SELECT DISTINCT city_name, country FROM UJT);
   END IF;
END IF;


DELETE FROM transducer._TPERSON_DELETE;
DELETE FROM transducer._PERSON_PHONE_DELETE;
DELETE FROM transducer._PERSON_EMAIL_DELETE;
DELETE FROM transducer._TCITY_DELETE;
DELETE FROM transducer._CITY_COORD_DELETE;
DELETE FROM transducer._CITY_MAYOR_DELETE;

DELETE FROM transducer._TPERSON_DELETE_JOIN;
DELETE FROM transducer._PERSON_PHONE_DELETE_JOIN;
DELETE FROM transducer._PERSON_EMAIL_DELETE_JOIN;
DELETE FROM transducer._TCITY_DELETE_JOIN;
DELETE FROM transducer._CITY_COORD_DELETE_JOIN;
DELETE FROM transducer._CITY_MAYOR_DELETE_JOIN;

DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END IF;
END;    $$;




/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

/** S->T INSERT TRIGGERS **/

CREATE TRIGGER source_SPERSON_INSERT_trigger
AFTER INSERT ON transducer._SPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SPERSON_INSERT_fn();

CREATE TRIGGER source_SCITY_INSERT_trigger
AFTER INSERT ON transducer._SCITY
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SCITY_INSERT_fn();


CREATE TRIGGER source_SPERSON_INSERT_JOIN_trigger
AFTER INSERT ON transducer._SPERSON_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SPERSON_INSERT_JOIN_fn();

CREATE TRIGGER source_SCITY_INSERT_JOIN_trigger
AFTER INSERT ON transducer._SCITY_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SCITY_INSERT_JOIN_fn();


CREATE TRIGGER source_INSERT_trigger_1
AFTER INSERT ON transducer._SPERSON_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_INSERT_fn();

CREATE TRIGGER source_INSERT_trigger_2
AFTER INSERT ON transducer._SCITY_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_INSERT_fn();


/** S->T DELETE TRIGGERS **/

CREATE TRIGGER source_SPERSON_DELETE_trigger
AFTER DELETE ON transducer._SPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SPERSON_DELETE_fn();

CREATE TRIGGER source_SCITY_DELETE_trigger
AFTER DELETE ON transducer._SCITY
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SCITY_DELETE_fn();


CREATE TRIGGER source_SPERSON_DELETE_JOIN_trigger
AFTER INSERT ON transducer._SPERSON_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SPERSON_DELETE_JOIN_fn();

CREATE TRIGGER source_SCITY_DELETE_JOIN_trigger
AFTER INSERT ON transducer._SCITY_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.source_SCITY_DELETE_JOIN_fn();


CREATE TRIGGER source_DELETE_trigger_1
AFTER INSERT ON transducer._SPERSON_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_DELETE_fn();

CREATE TRIGGER source_DELETE_trigger_2
AFTER INSERT ON transducer._SCITY_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_DELETE_fn();



/** T->S INSERT **/

CREATE TRIGGER target_TPERSON_INSERT_trigger
AFTER INSERT ON transducer._TPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TPERSON_INSERT_fn();

CREATE TRIGGER target_PERSON_PHONE_INSERT_trigger
AFTER INSERT ON transducer._PERSON_PHONE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_fn();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_trigger
AFTER INSERT ON transducer._PERSON_EMAIL
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_fn();

CREATE TRIGGER target_TCITY_INSERT_trigger
AFTER INSERT ON transducer._TCITY
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TCITY_INSERT_fn();

CREATE TRIGGER target_CITY_COORD_INSERT_trigger
AFTER INSERT ON transducer._CITY_COORD
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COORD_INSERT_fn();

CREATE TRIGGER target_CITY_MAYOR_INSERT_trigger
AFTER INSERT ON transducer._CITY_MAYOR
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_MAYOR_INSERT_fn();


CREATE TRIGGER target_TPERSON_INSERT_JOIN_trigger
AFTER INSERT ON transducer._TPERSON_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TPERSON_INSERT_JOIN_fn();

CREATE TRIGGER target_PERSON_PHONE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_PHONE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_fn();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_fn();

CREATE TRIGGER target_TCITY_INSERT_JOIN_trigger
AFTER INSERT ON transducer._TCITY_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TCITY_INSERT_JOIN_fn();

CREATE TRIGGER target_CITY_COORD_INSERT_JOIN_trigger
AFTER INSERT ON transducer._CITY_COORD_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COORD_INSERT_JOIN_fn();

CREATE TRIGGER target_CITY_MAYOR_INSERT_JOIN_trigger
AFTER INSERT ON transducer._CITY_MAYOR_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_MAYOR_INSERT_JOIN_fn();


CREATE TRIGGER target_INSERT_trigger_1
AFTER INSERT ON transducer._TPERSON_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_2
AFTER INSERT ON transducer._PERSON_PHONE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_3
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_4
AFTER INSERT ON transducer._TCITY_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_5
AFTER INSERT ON transducer._CITY_COORD_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_6
AFTER INSERT ON transducer._CITY_MAYOR_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

/** T->S DELETE **/

CREATE TRIGGER target_TPERSON_DELETE_trigger
AFTER DELETE ON transducer._TPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TPERSON_DELETE_fn();

CREATE TRIGGER target_PERSON_PHONE_DELETE_trigger
AFTER DELETE ON transducer._PERSON_PHONE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_DELETE_fn();

CREATE TRIGGER target_PERSON_EMAIL_DELETE_trigger
AFTER DELETE ON transducer._PERSON_EMAIL
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_DELETE_fn();

CREATE TRIGGER target_TCITY_DELETE_trigger
AFTER DELETE ON transducer._TCITY
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TCITY_DELETE_fn();

CREATE TRIGGER target_CITY_COORD_DELETE_trigger
AFTER DELETE ON transducer._CITY_COORD
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COORD_DELETE_fn();

CREATE TRIGGER target_CITY_MAYOR_DELETE_trigger
AFTER DELETE ON transducer._CITY_MAYOR
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_MAYOR_DELETE_fn();


CREATE TRIGGER target_TPERSON_DELETE_JOIN_trigger
AFTER INSERT ON transducer._TPERSON_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TPERSON_DELETE_JOIN_fn();

CREATE TRIGGER target_PERSON_PHONE_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_PHONE_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_DELETE_JOIN_fn();

CREATE TRIGGER target_PERSON_EMAIL_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_EMAIL_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_DELETE_JOIN_fn();

CREATE TRIGGER target_TCITY_DELETE_JOIN_trigger
AFTER INSERT ON transducer._TCITY_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TCITY_DELETE_JOIN_fn();

CREATE TRIGGER target_CITY_COORD_DELETE_JOIN_trigger
AFTER INSERT ON transducer._CITY_COORD_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_COORD_DELETE_JOIN_fn();

CREATE TRIGGER target_CITY_MAYOR_DELETE_JOIN_trigger
AFTER INSERT ON transducer._CITY_MAYOR_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CITY_MAYOR_DELETE_JOIN_fn();


CREATE TRIGGER target_DELETE_trigger_1
AFTER INSERT ON transducer._TPERSON_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_2
AFTER INSERT ON transducer._PERSON_PHONE_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_3
AFTER INSERT ON transducer._PERSON_EMAIL_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_4
AFTER INSERT ON transducer._TCITY_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_5
AFTER INSERT ON transducer._CITY_COORD_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_6
AFTER INSERT ON transducer._CITY_MAYOR_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();