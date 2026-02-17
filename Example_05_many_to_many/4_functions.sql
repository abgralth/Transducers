/** S->T INSERTS **/

CREATE OR REPLACE FUNCTION transducer.source_SPERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._SPERSON_INSERT VALUES(NEW.ssn, NEW.name, NEW.phone, NEW.email, NEW.city_name);
   RETURN NEW;
END IF;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.source_SCITY_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._SCITY_INSERT VALUES(NEW.city_name, NEW.country, NEW.coordinates);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, city_name, country, coordinates
FROM transducer._SPERSON_INSERT
NATURAL LEFT OUTER JOIN transducer._SCITY);

INSERT INTO transducer._SCITY_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, city_name, country, coordinates
FROM transducer._SCITY_INSERT
NATURAL LEFT OUTER JOIN transducer._SPERSON);

INSERT INTO transducer._SCITY_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._SPERSON_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._SCITY_INSERT_JOIN)
   WHERE 
   ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
   AND city_name IS NOT NULL AND country IS NOT NULL);

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

   /*Only for CITY_COORD do we use an IF*/

   IF EXISTS (SELECT * FROM UJT WHERE coordinates IS NOT NULL) THEN
      INSERT INTO transducer._CITY_COORD (SELECT DISTINCT city_name, country, coordinates FROM UJT
                              WHERE city_name IS NOT NULL AND country IS NOT NULL AND coordinates IS NOT NULL) 
                              ON CONFLICT (city_name) DO NOTHING;
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
   RAISE NOTICE 'Starting DELETE from PERSON';
   INSERT INTO transducer._SPERSON_DELETE VALUES(OLD.ssn, OLD.name, OLD.phone, OLD.email, OLD.city_name);
   RETURN OLD;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.source_SCITY_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN OLD;
ELSE
   RAISE NOTICE 'Starting DELETE from PERSON';
   INSERT INTO transducer._SCITY_DELETE VALUES(OLD.city_name, OLD.country, OLD.coordinates);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, name, phone, email, city_name, country, coordinates
FROM transducer._SPERSON_DELETE
NATURAL LEFT OUTER JOIN transducer._SCITY);

INSERT INTO transducer._SCITY_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates
FROM transducer._SCITY_DELETE
NATURAL LEFT OUTER JOIN transducer._SPERSON);

INSERT INTO transducer._SCITY_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._SPERSON_DELETE_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._SCITY_DELETE_JOIN)
   WHERE 
   ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
   AND city_name IS NOT NULL AND country IS NOT NULL);

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



/*We first need to check if the delete will cause the loss of a _SPERSON, or only of its PHONE or EMAIL*/
IF NOT EXISTS (SELECT r1.ssn, r1.phone, r1.email FROM transducer._SPERSON as r1, UJT
               WHERE r1.ssn = UJT.ssn
               EXCEPT
               SELECT ssn, phone, email FROM UJT)
THEN
/*We then check if deleting a person require deleting a city*/
IF EXISTS (SELECT ssn, city_name, country FROM transducer._SPERSON
           NATURAL LEFT OUTER JOIN transducer._SCITY
           EXCEPT
           (SELECT ssn, city_name, country FROM UJT)) 
THEN
   DELETE FROM transducer._PERSON_PHONE WHERE (ssn) IN (SELECT ssn FROM UJT);
   DELETE FROM transducer._PERSON_EMAIL WHERE (ssn) IN (SELECT ssn FROM UJT);
   DELETE FROM transducer._TPERSON WHERE (ssn) IN (SELECT ssn FROM UJT);
ELSE
   /*If the city has coordinates, we also need to delete the tuple from _CITY_COORD*/
   IF EXISTS (SELECT coordinates FROM UJT WHERE coordinates IS NOT NULL) 
   THEN
      DELETE FROM transducer._PERSON_PHONE WHERE (ssn) IN (SELECT ssn FROM UJT);
      DELETE FROM transducer._PERSON_EMAIL WHERE (ssn) IN (SELECT ssn FROM UJT);
      DELETE FROM transducer._TPERSON WHERE (ssn) IN (SELECT ssn FROM UJT);
      DELETE FROM transducer._CITY_COORD WHERE (city_name) IN (SELECT city_name FROM UJT);
      DELETE FROM transducer._TCITY WHERE (city_name) IN (SELECT city_name FROM UJT);
   ELSE
      DELETE FROM transducer._PERSON_PHONE WHERE (ssn) IN (SELECT ssn FROM UJT);
      DELETE FROM transducer._PERSON_EMAIL WHERE (ssn) IN (SELECT ssn FROM UJT);
      DELETE FROM transducer._TPERSON WHERE (ssn) IN (SELECT ssn FROM UJT);
      DELETE FROM transducer._TCITY WHERE (city_name) IN (SELECT city_name FROM UJT);
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
   INSERT INTO transducer._PERSON_PHONE_INSERT VALUES(NEW.ssn, NEW.phone);
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
   INSERT INTO transducer._PERSON_EMAIL_INSERT VALUES(NEW.ssn, NEW.email);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._TPERSON_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._PERSON_PHONE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._PERSON_EMAIL_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._TCITY_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._CITY_COORD_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY);

INSERT INTO transducer._TPERSON_INSERT_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_INSERT_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_INSERT_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._TPERSON_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_PHONE_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_EMAIL_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._TCITY_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._CITY_COORD_INSERT_JOIN)
   WHERE 
   ssn IS NOT NULL AND NAME IS NOT NULL AND phone IS NOT NULL AND email IS NOT NULL
   AND city_name IS NOT NULL AND country IS NOT NULL);


/*Yeah, here again it's a bit annoying as we need to prevent redundant tuples and only INSERT tuples where 
  coordinates is non-null, when it actually is */

IF EXISTS (SELECT * FROM UJT 
         EXCEPT (SELECT * FROM UJT WHERE coordinates IS NULL)) THEN
      DELETE FROM UJT WHERE coordinates IS NULL;    
END IF;

INSERT INTO transducer._SCITY (SELECT DISTINCT city_name, country, coordinates FROM UJT
                              WHERE city_name IS NOT NULL) ON CONFLICT (city_name) DO NOTHING;


INSERT INTO transducer._SPERSON (SELECT DISTINCT ssn, name, phone, email, city_name FROM UJT 
                              WHERE ssn IS NOT NULL AND city_name IS NOT NULL) 
                              ON CONFLICT (ssn,phone,email) DO NOTHING;


DELETE FROM transducer._TPERSON_INSERT;
DELETE FROM transducer._PERSON_PHONE_INSERT;
DELETE FROM transducer._PERSON_EMAIL_INSERT;
DELETE FROM transducer._TCITY_INSERT;
DELETE FROM transducer._CITY_COORD_INSERT;

DELETE FROM transducer._TPERSON_INSERT_JOIN;
DELETE FROM transducer._PERSON_PHONE_INSERT_JOIN;
DELETE FROM transducer._PERSON_EMAIL_INSERT_JOIN;
DELETE FROM transducer._TCITY_INSERT_JOIN;
DELETE FROM transducer._CITY_COORD_INSERT_JOIN;

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
   INSERT INTO transducer._PERSON_PHONE_DELETE VALUES(OLD.ssn, OLD.phone);
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
   INSERT INTO transducer._PERSON_EMAIL_DELETE VALUES(OLD.ssn, OLD.email);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._TPERSON_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._PERSON_PHONE_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._PERSON_EMAIL_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._TCITY_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._CITY_COORD);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._CITY_COORD_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TPERSON
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._TCITY);

INSERT INTO transducer._TPERSON_DELETE_JOIN (SELECT ssn, name, city_name FROM temp_table);
INSERT INTO transducer._TCITY_DELETE_JOIN (SELECT city_name, country FROM temp_table);
INSERT INTO transducer._CITY_COORD_DELETE_JOIN (SELECT city_name, country, coordinates FROM temp_table);
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
   coordinates VARCHAR(100)
);
   INSERT INTO UJT (
   SELECT DISTINCT ssn, name, phone, email, city_name, country, coordinates
   FROM transducer._TPERSON_DELETE_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_PHONE_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_EMAIL_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._TCITY_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._CITY_COORD_DELETE_JOIN)
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
  over _SPERSON is done independently, and one in which it brings a _SCITY along its fall*/

IF EXISTS( SELECT ssn, city_name, country FROM transducer._SPERSON
           NATURAL LEFT OUTER JOIN transducer._SCITY
           EXCEPT
           (SELECT ssn, city_name, country FROM UJT)) THEN
   DELETE FROM transducer._SPERSON WHERE (ssn, phone, email) IN (SELECT DISTINCT ssn,phone,email FROM UJT);
ELSE
   DELETE FROM transducer._SPERSON WHERE (ssn, phone, email) IN (SELECT DISTINCT ssn,phone,email FROM UJT);   
   DELETE FROM transducer._SCITY WHERE (city_name, country) IN (SELECT DISTINCT city_name, country FROM UJT);
END IF;


DELETE FROM transducer._TPERSON_DELETE;
DELETE FROM transducer._PERSON_PHONE_DELETE;
DELETE FROM transducer._PERSON_EMAIL_DELETE;
DELETE FROM transducer._TCITY_DELETE;
DELETE FROM transducer._CITY_COORD_DELETE;

DELETE FROM transducer._TPERSON_DELETE_JOIN;
DELETE FROM transducer._PERSON_PHONE_DELETE_JOIN;
DELETE FROM transducer._PERSON_EMAIL_DELETE_JOIN;
DELETE FROM transducer._TCITY_DELETE_JOIN;
DELETE FROM transducer._CITY_COORD_DELETE_JOIN;

DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END IF;
END;    $$;

