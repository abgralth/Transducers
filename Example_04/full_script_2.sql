DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._PERSON
    (
      ssn VARCHAR(100) NOT NULL,
      empid VARCHAR(100),
      name VARCHAR(100) NOT NULL,
      hdate VARCHAR(100),
      phone VARCHAR(100) NOT NULL,
      email VARCHAR(100) NOT NULL,
      dept VARCHAR(100),
      manager VARCHAR(100)
    );
	
ALTER TABLE transducer._PERSON ADD PRIMARY KEY (ssn,phone,email);


CREATE OR REPLACE FUNCTION transducer.check_PERSON_guard_fn_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF NOT EXISTS (
	   		SELECT * 
         	FROM (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS rnew
            WHERE (NEW.empid IS NOT NULL AND NEW.hdate IS NOT NULL AND NEW.dept IS NOT NULL AND NEW.manager IS NOT NULL)
	   		OR	  (NEW.empid IS NOT NULL AND NEW.hdate IS NOT NULL AND NEW.dept IS NULL AND NEW.manager IS NULL)
	        OR    (NEW.empid IS NULL AND NEW.hdate IS NULL AND NEW.dept IS NULL AND NEW.manager IS NULL)
         	) THEN
      			RAISE EXCEPTION 'THIS ADDED VALUES DO NOT CORRESPOND TO ANY ALLOWS TUPLE PATTERNS %', NEW;
      			RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;


CREATE OR REPLACE FUNCTION transducer.check_PERSON_mvd_fn_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT DISTINCT r1.ssn, r2.empid, r2.name, r2.hdate, r1.phone, r1.email, r2.dept, r2.manager 
         FROM transducer._PERSON AS r1,
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS r2
            WHERE  r1.ssn = r2.ssn 
         EXCEPT
         SELECT *
         FROM transducer._PERSON
         ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE MVD CONSTRAINT ON PHONE AND EMAIL %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_mvd_fn_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS   (SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._PERSON)) THEN
      RAISE NOTICE 'THE TUPLE % LEAD TO ADITIONAL ONES', NEW;
      INSERT INTO transducer._PERSON 
            (SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, NEW.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            UNION
            SELECT r1.ssn, r1.empid, r1.name, r1.hdate, NEW.phone, r1.email, r1.dept, r1.manager
            FROM transducer._PERSON as r1
            WHERE r1.ssn = NEW.ssn
            EXCEPT 
            (SELECT * FROM transducer._PERSON));
      RETURN NEW;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_IND_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF (NEW.manager IS NULL) THEN
      RETURN NEW;
   END IF;
   IF(NEW.manager = NEW.ssn) THEN
      RETURN NEW;
   END IF;
   IF EXISTS (SELECT DISTINCT NEW.manager 
            FROM transducer._person
         EXCEPT(
         SELECT ssn AS manager
         FROM transducer._person
         UNION
         SELECT NEW.ssn as manager)) THEN
         RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE INC1 CONSTRAINT';
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;


CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._PERSON AS R1, 
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
         WHERE R2.empid IS NOT NULL AND R2.hdate IS NOT NULL 
            AND R1.empid = R2.empid 
            AND R1.hdate <> R2.hdate
            ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT empid -> hdate %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;


CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._PERSON AS R1, 
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
         WHERE R2.empid IS NOT NULL AND R2.dept IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL 
            AND R1.empid = R2.empid 
            AND R1.dept <> R2.dept 
            ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT empid -> dept %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_3()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._PERSON AS R1, 
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
         WHERE R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL 
            AND R1.dept = R2.dept 
            AND R1.manager <> R2.manager
            ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT dept -> manager %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;


CREATE OR REPLACE TRIGGER PERSON_GUARD_trigger_1
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_guard_fn_1();


CREATE OR REPLACE TRIGGER PERSON_mvd_trigger_1
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_mvd_fn_1();

CREATE OR REPLACE TRIGGER PERSON_mvd_trigger_2
AFTER INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_mvd_fn_2();

CREATE TRIGGER PERSON_cfd_trigger_1
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_cfd_fn_1();



CREATE TRIGGER PERSON_cfd_trigger_2
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_cfd_fn_2();

CREATE TRIGGER PERSON_cfd_trigger_3
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_cfd_fn_3();



CREATE TRIGGER PERSON_IND_trigger_1
BEFORE INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_PERSON_ind_fn_1();



INSERT INTO transducer._PERSON (ssn, empid, name, hdate, phone, email, dept, manager) VALUES
('ssn1', 'emp1', 'June', 'hdate1', 'phone11', 'mail11', 'dep1', 'ssn1'),
('ssn2', 'emp2', 'Jovial', 'hdate2', 'phone21', 'mail21', NULL, NULL),
('ssn3', NULL, 'Jord', NULL, 'phone31', 'mail31', NULL, NULL)
;

/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

CREATE TABLE transducer._P AS 
   SELECT DISTINCT ssn, name FROM transducer._PERSON;

CREATE TABLE transducer._PE AS
   SELECT DISTINCT ssn, empid FROM transducer._PERSON
   WHERE empid IS NOT NULL AND hdate IS NOT NULL;

CREATE TABLE transducer._PED AS
   SELECT DISTINCT ssn, empid FROM transducer._PERSON
   WHERE empid IS NOT NULL AND dept IS NOT NULL;

CREATE TABLE transducer._PE_HDATE AS
   SELECT DISTINCT empid, hdate FROM transducer._PERSON
   WHERE empid IS NOT NULL AND hdate IS NOT NULL;

CREATE TABLE transducer._PED_DEPT AS
   SELECT DISTINCT empid, dept FROM transducer._PERSON
   WHERE empid IS NOT NULL AND dept IS NOT NULL;

CREATE TABLE transducer._DEPT_MANAGER AS
   SELECT DISTINCT dept, manager FROM transducer._PERSON
   WHERE dept IS NOT NULL AND manager IS NOT NULL;

CREATE TABLE transducer._PERSON_PHONE AS
SELECT DISTINCT ssn, phone FROM transducer._PERSON;

CREATE TABLE transducer._PERSON_EMAIL AS 
SELECT DISTINCT ssn, email FROM transducer._PERSON;





/*BASE CONSTRAINTS*/

ALTER TABLE transducer._P ADD PRIMARY KEY (ssn);

ALTER TABLE transducer._PE ADD PRIMARY KEY (empid);

ALTER TABLE transducer._PED ADD PRIMARY KEY (empid);

ALTER TABLE transducer._PE_HDATE ADD PRIMARY KEY (empid);

ALTER TABLE transducer._PED_DEPT ADD PRIMARY KEY (empid);

ALTER TABLE transducer._DEPT_MANAGER ADD PRIMARY KEY (dept);

ALTER TABLE transducer._PERSON_PHONE ADD PRIMARY KEY (ssn,phone);

ALTER TABLE transducer._PERSON_EMAIL ADD PRIMARY KEY (ssn,email);





ALTER TABLE transducer._PE
ADD FOREIGN KEY (ssn) REFERENCES transducer._P(ssn);

ALTER TABLE transducer._PED
ADD FOREIGN KEY (empid) REFERENCES transducer._PE(empid);

ALTER TABLE transducer._PE_HDATE
ADD FOREIGN KEY (empid) REFERENCES transducer._PE(empid);

ALTER TABLE transducer._PED_DEPT
ADD FOREIGN KEY (empid) REFERENCES transducer._PED(empid);

/*
ALTER TABLE transducer._DEPT_MANAGER
ADD FOREIGN KEY (dept) REFERENCES transducer._PED_DEPT(dept);
*/

ALTER TABLE transducer._PED_DEPT
ADD FOREIGN KEY (dept) REFERENCES transducer._DEPT_MANAGER(dept);

ALTER TABLE transducer._DEPT_MANAGER
ADD FOREIGN KEY (manager) REFERENCES transducer._P(ssn);

ALTER TABLE transducer._PERSON_PHONE 
ADD FOREIGN KEY (ssn) REFERENCES transducer._P(ssn);

ALTER TABLE transducer._PERSON_EMAIL 
ADD FOREIGN KEY (ssn) REFERENCES transducer._P(ssn);




/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

/* S -> T */

CREATE TABLE transducer._PERSON_INSERT AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_DELETE AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;


CREATE TABLE transducer._PERSON_INSERT_JOIN AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;

CREATE TABLE transducer._PERSON_DELETE_JOIN AS
SELECT * FROM transducer._PERSON
WHERE 1<>1;






/** INSERT T -> S **/

CREATE TABLE transducer._P_INSERT AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._P_DELETE AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._PE_INSERT AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PE_DELETE AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PED_INSERT AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PED_DELETE AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_INSERT AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_DELETE AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_INSERT AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_DELETE AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_INSERT AS
SELECT * FROM transducer._DEPT_MANAGER
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_DELETE AS
SELECT * FROM transducer._DEPT_MANAGER
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


CREATE TABLE transducer._P_INSERT_JOIN AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._P_DELETE_JOIN AS
SELECT * FROM transducer._P
WHERE 1<>1;

CREATE TABLE transducer._PE_INSERT_JOIN AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PE_DELETE_JOIN AS
SELECT * FROM transducer._PE
WHERE 1<>1;

CREATE TABLE transducer._PED_INSERT_JOIN AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PED_DELETE_JOIN AS
SELECT * FROM transducer._PED
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_INSERT_JOIN AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PE_HDATE_DELETE_JOIN AS
SELECT * FROM transducer._PE_HDATE
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_INSERT_JOIN AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._PED_DEPT_DELETE_JOIN AS
SELECT * FROM transducer._PED_DEPT
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_INSERT_JOIN AS
SELECT * FROM transducer._DEPT_MANAGER
WHERE 1<>1;

CREATE TABLE transducer._DEPT_MANAGER_DELETE_JOIN AS
SELECT * FROM transducer._DEPT_MANAGER
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


CREATE TABLE transducer._LOOP (
loop_start INT NOT NULL );



/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */


/** S->T INSERTS **/

CREATE OR REPLACE FUNCTION transducer.source_PERSON_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._PERSON_INSERT VALUES(NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager);
   RETURN NEW;
END IF;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.source_PERSON_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, empid, name, hdate, phone, email, dept, manager
FROM transducer._PERSON_INSERT);

INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._PERSON_INSERT_JOIN (SELECT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table);

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

   INSERT INTO transducer._P (SELECT DISTINCT ssn, name FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND name IS NOT NULL) ON CONFLICT (ssn) DO NOTHING;


   /* Okay, regarding the INSERTs into subtables, I'm not sure how to generalize this. For instance, we have a table _P which contains every persons of _PERSON and another
      two tables _PE and _PED, both defining persons with a non-null empid attribute and a non-null dept attribute for the second.
      Checking for these is easy in our current URA scenario, but in a many to many table case, I think we would recreate the join table and check it */
   
   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
      INSERT INTO transducer._PE (SELECT DISTINCT ssn, empid FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL) THEN
      INSERT INTO transducer._PE_HDATE (SELECT DISTINCT empid, hdate FROM transducer._PERSON_INSERT_JOIN 
                              WHERE empid IS NOT NULL AND hdate IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._PED (SELECT DISTINCT ssn, empid FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT (empid) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._DEPT_MANAGER (SELECT DISTINCT dept, manager FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT (dept) DO NOTHING;
   END IF;

   IF EXISTS (SELECT * FROM transducer._PERSON_INSERT_JOIN WHERE empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) THEN
      INSERT INTO transducer._PED_DEPT (SELECT DISTINCT empid, dept FROM transducer._PERSON_INSERT_JOIN 
                              WHERE ssn IS NOT NULL AND empid IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL) ON CONFLICT DO NOTHING;
   END IF;



   INSERT INTO transducer._PERSON_PHONE (SELECT DISTINCT ssn, phone FROM transducer._PERSON_INSERT_JOIN
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,phone) DO NOTHING;

   INSERT INTO transducer._PERSON_EMAIL (SELECT DISTINCT ssn, email FROM transducer._PERSON_INSERT_JOIN
                                         WHERE ssn IS NOT NULL) ON CONFLICT (ssn,email) DO NOTHING;


   DELETE FROM transducer._PERSON_INSERT;
   DELETE FROM transducer._PERSON_INSERT_JOIN;
   DELETE FROM transducer._loop;
   RETURN NEW;
END IF;
END;  $$;



/** S->T DELETES **/

CREATE OR REPLACE FUNCTION transducer.source_PERSON_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETE from PERSON';
   INSERT INTO transducer._PERSON_DELETE VALUES(OLD.ssn, OLD.empid, OLD.name, OLD.hdate, OLD.phone, OLD.email, OLD.dept, OLD.manager);
   RETURN OLD;
END IF;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.source_PERSON_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (SELECT ssn, empid, name, hdate, phone, email, dept, manager
FROM transducer._PERSON_DELETE);

INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._PERSON_DELETE_JOIN (SELECT ssn, empid, name, hdate, phone, email, dept, manager FROM temp_table);

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


/* Doin deletes

\begin{verbatim}
   IF EXISTS (
      SELECT UTJ1.<UTJ1 ATTRIBUTES> 
      FROM <Universal Join Table over all S tables> AS UJT1,
          <Universal Join Table over all S Join tables> AS UJT2,
      
      WHERE UJT1.X1 = UJT2.X2
      ...
      AND UJT1.Xi-1 = UJT2.Xi-1
      AND UJT1.Xj+1 = UJT2.Xj-1
      ...
      AND UJT1.Xm = UJT2.Xm
      EXCEPT 
      SELECT * 
      FROM UJT2
      ) THEN
         DELETE FROM Ti
         WHERE Xi = UJT.Xi
         AND Xi+1 = UJT.Xi+1
         ...
         AND Xj = UJT.Xj;
   END IF;
\end{verbatim}
*/



/*Person_Phone

IF EXISTS (SELECT * FROM transducer._PERSON 
            WHERE ssn = NEW.ssn AND email = NEW.email
            EXCEPT
            SELECT * FROM (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.manager, NEW.dept, NEW.manager)) THEN
                  DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT ssn, phone FROM transducer._PERSON_DELETE_JOIN);
END IF;
*/
/*Person_Email
IF EXISTS (SELECT * FROM transducer._PERSON 
            WHERE ssn = NEW.ssn AND phone = NEW.phone
            EXCEPT
            SELECT * FROM (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.manager, NEW.dept, NEW.manager)) THEN
                  DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._PERSON_DELETE_JOIN);
END IF;


IF NOT EXISTS (SELECT * FROM transducer._PERSON 
            WHERE ssn = NEW.ssn AND name = NEW.name AND hdate = NEW.hdate
            AND phone = NEW.phone AND email = NEW.email AND manager = NEW.manager
            EXCEPT
            SELECT * FROM (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.manager, NEW.dept, NEW.manager)) THEN
   DELETE FROM transducer._PED_DEPT WHERE (empid) IN (SELECT empid FROM transducer._PERSON_DELETE_JOIN);
END IF;

IF NOT EXISTS (SELECT * FROM transducer._PERSON 
            WHERE ssn = NEW.ssn AND name = NEW.name AND dept = NEW.dept
            AND phone = NEW.phone AND email = NEW.email AND manager = NEW.manager
            EXCEPT
            SELECT * FROM (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.manager, NEW.dept, NEW.manager)) THEN
   DELETE FROM transducer._PE_HDATE WHERE (empid) IN (SELECT empid FROM transducer._PERSON_DELETE_JOIN);
END IF;

*/

IF EXISTS (SELECT * FROM transducer._PERSON WHERE ssn = NEW.ssn 
              EXCEPT (SELECT * FROM transducer._PERSON WHERE ssn = NEW.ssn AND phone = NEW.phone)) THEN
                  DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT ssn, phone FROM transducer._PERSON_DELETE_JOIN);
END IF;

   /*EMAIL*/
IF EXISTS (SELECT * FROM transducer._PERSON WHERE ssn = NEW.ssn 
              EXCEPT (SELECT * FROM transducer._PERSON WHERE ssn = NEW.ssn AND email = NEW.email)) THEN
                  DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._PERSON_DELETE_JOIN);
END IF;


/*Dept_Manager*/




/*P,PE,PED*/

IF NOT EXISTS (SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, r1.email, r1.dept, r1.manager
               FROM transducer._PERSON as r1, transducer._PERSON_DELETE_JOIN as r2
               WHERE r1.ssn = r2.ssn
               EXCEPT
               SELECT * FROM transducer._PERSON_DELETE_JOIN) THEN
                     IF NOT EXISTS (SELECT r1.ssn, r1.empid, r1.name, r1.hdate, r1.phone, r1.email, r1.dept, r1.manager
                                    FROM transducer._PERSON as r1, transducer._PERSON_DELETE_JOIN as r2
                     WHERE r1.dept = r2.dept
                     EXCEPT
                     SELECT * FROM transducer._PERSON_DELETE_JOIN) THEN
             RAISE NOTICE 'Starting DELETE from EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEHHHHHHHHHHHHHHHHH';
              
                  DELETE FROM transducer._PED_DEPT WHERE (empid) IN (SELECT empid FROM transducer._PERSON_DELETE_JOIN);
              DELETE FROM transducer._DEPT_MANAGER WHERE (dept) IN (SELECT dept FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PED WHERE (ssn) IN (SELECT ssn FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PE_HDATE WHERE (empid) IN (SELECT empid FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PE WHERE (ssn) IN (SELECT ssn FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT ssn, phone FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._P WHERE (ssn) IN (SELECT ssn FROM transducer._PERSON_DELETE_JOIN);
            ELSE
              RAISE NOTICE 'Starting DELETE from UUUUUUUUUUUUUUUUUUUUUHHHHHHHHHHHHHHHHHHHHHHH';
                  
                  DELETE FROM transducer._PED_DEPT WHERE (empid) IN (SELECT empid FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PED WHERE (ssn) IN (SELECT ssn FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PE_HDATE WHERE (empid) IN (SELECT empid FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PE WHERE (ssn) IN (SELECT ssn FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PERSON_PHONE WHERE (ssn, phone) IN (SELECT ssn, phone FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._PERSON_EMAIL WHERE (ssn, email) IN (SELECT ssn, email FROM transducer._PERSON_DELETE_JOIN);
                  DELETE FROM transducer._P WHERE (ssn) IN (SELECT ssn FROM transducer._PERSON_DELETE_JOIN);
            
            END IF;
END IF;






DELETE FROM transducer._PERSON_DELETE;
DELETE FROM transducer._PERSON_DELETE_JOIN;
DELETE FROM transducer._loop;
RETURN NEW;

END;  $$;



/** T->S INSERTS **/

CREATE OR REPLACE FUNCTION transducer.target_P_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _P';
   INSERT INTO transducer._P_INSERT VALUES(NEW.ssn, NEW.name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_P_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PE';
   INSERT INTO transducer._PE_INSERT VALUES(NEW.ssn, NEW.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PE_HDATE';
   INSERT INTO transducer._PE_HDATE_INSERT VALUES(NEW.empid, NEW.hdate);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_HDATE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PED';
   INSERT INTO transducer._PED_INSERT VALUES(NEW.ssn, NEW.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PED_DEPT';
   INSERT INTO transducer._PED_DEPT_INSERT VALUES(NEW.empid, NEW.dept);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_DEPT_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _DEPT_MANAGER';
   INSERT INTO transducer._DEPT_MANAGER_INSERT VALUES(NEW.dept, NEW.manager);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._DEPT_MANAGER_INSERT 
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
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

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_PHONE_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_INSERT_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_INSERT_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
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

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_EMAIL_INSERT 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE);

INSERT INTO transducer._P_INSERT_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_INSERT_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_INSERT_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_INSERT_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_INSERT_JOIN (SELECT dept, manager FROM temp_table);
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
id_loop VARCHAR(100);
BEGIN

RAISE NOTICE 'Function transducer.target_insert_fn called';

SELECT count(*) INTO v_loop from transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an INSERT on _PERSON';



			
create temporary table UJT (
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO UJT (
   SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_INSERT_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PE_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PE_HDATE_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PED_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PED_DEPT_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._DEPT_MANAGER_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_PHONE_INSERT_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_EMAIL_INSERT_JOIN)
   WHERE 
		ssn IS NOT NULL AND NAME IS NOT NULL 
   AND phone IS NOT NULL AND email IS NOT NULL
	
   AND ((empid IS NULL AND hdate IS NULL)
   OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL)
   OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL)));



FOR id_loop IN (SELECT CONCAT(ssn,phone,email) FROM UJT)
LOOP

IF EXISTS (SELECT * FROM UJT WHERE id_loop = CONCAT(ssn,phone,email)
      	   EXCEPT 
		   (SELECT * FROM UJT WHERE empid IS NULL AND id_loop = CONCAT(ssn,phone,email))
      		)THEN 
         		DELETE FROM UJT WHERE empid IS NULL AND id_loop = CONCAT(ssn,phone,email);
END IF;

IF EXISTS (SELECT * FROM UJT WHERE id_loop = CONCAT(ssn,phone,email)
      	   EXCEPT 
		   (SELECT * FROM UJT WHERE dept IS NULL AND id_loop = CONCAT(ssn,phone,email))
      		)THEN 
         		DELETE FROM UJT WHERE dept IS NULL AND id_loop = CONCAT(ssn,phone,email);
END IF;
END LOOP;


INSERT INTO transducer._PERSON (SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager FROM UJT) ON CONFLICT (ssn, phone, email) DO NOTHING;


DELETE FROM transducer._P_INSERT;
DELETE FROM transducer._PE_INSERT;
DELETE FROM transducer._PE_HDATE_INSERT;
DELETE FROM transducer._PED_INSERT;
DELETE FROM transducer._PED_DEPT_INSERT;
DELETE FROM transducer._DEPT_MANAGER_INSERT;
DELETE FROM transducer._PERSON_PHONE_INSERT;
DELETE FROM transducer._PERSON_EMAIL_INSERT;

DELETE FROM transducer._P_INSERT_JOIN;
DELETE FROM transducer._PE_INSERT_JOIN;
DELETE FROM transducer._PE_HDATE_INSERT_JOIN;
DELETE FROM transducer._PED_INSERT_JOIN;
DELETE FROM transducer._PED_DEPT_INSERT_JOIN;
DELETE FROM transducer._DEPT_MANAGER_INSERT_JOIN;
DELETE FROM transducer._PERSON_PHONE_INSERT_JOIN;
DELETE FROM transducer._PERSON_EMAIL_INSERT_JOIN;


DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END IF;
END;    $$;


/** T->S DELETES **/

CREATE OR REPLACE FUNCTION transducer.target_P_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _P';
   INSERT INTO transducer._P_DELETE VALUES(OLD.ssn, OLD.name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_P_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _PE';
   INSERT INTO transducer._PE_DELETE VALUES(OLD.ssn, OLD.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_DELETE 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _PE_HDATE';
   INSERT INTO transducer._PE_HDATE_DELETE VALUES(OLD.empid, OLD.hdate);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PE_HDATE_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PE_HDATE_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _PED';
   INSERT INTO transducer._PED_DELETE VALUES(OLD.ssn, OLD.empid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_DELETE 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _PED_DEPT';
   INSERT INTO transducer._PED_DEPT_DELETE VALUES(OLD.empid, OLD.dept);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PED_DEPT_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PED_DEPT_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _DEPT_MANAGER';
   INSERT INTO transducer._DEPT_MANAGER_DELETE VALUES(OLD.dept, OLD.manager);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_DEPT_MANAGER_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._DEPT_MANAGER_DELETE 
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _PERSON_PHONE';
   INSERT INTO transducer._PERSON_PHONE_DELETE VALUES(OLD.ssn, OLD.phone);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_PHONE_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_PHONE_DELETE 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
INSERT INTO transducer._PERSON_PHONE_DELETE_JOIN (SELECT ssn, phone FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PERSON_EMAIL_DELETE_JOIN (SELECT ssn, email FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETion from _PERSON_EMAIL';
   INSERT INTO transducer._PERSON_EMAIL_DELETE VALUES(OLD.ssn, OLD.email);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PERSON_EMAIL_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._PERSON_EMAIL_DELETE 
   NATURAL LEFT OUTER JOIN transducer._P
   NATURAL LEFT OUTER JOIN transducer._PE
   NATURAL LEFT OUTER JOIN transducer._PE_HDATE
   NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE);

INSERT INTO transducer._P_DELETE_JOIN (SELECT ssn, name FROM temp_table);
INSERT INTO transducer._PE_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PE_HDATE_DELETE_JOIN (SELECT empid, hdate FROM temp_table);
INSERT INTO transducer._PED_DELETE_JOIN (SELECT ssn, empid FROM temp_table);
INSERT INTO transducer._PED_DEPT_DELETE_JOIN (SELECT empid, dept FROM temp_table);
INSERT INTO transducer._DEPT_MANAGER_DELETE_JOIN (SELECT dept, manager FROM temp_table);
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
id_loop VARCHAR(100);
BEGIN

RAISE NOTICE 'Function transducer.target_DELETE_fn called';

SELECT count(*) INTO v_loop from transducer._loop;

IF NOT EXISTS (SELECT * FROM transducer._loop, (SELECT COUNT(*) as rc_value FROM transducer._loop) AS row_count
WHERE ABS(loop_start) = row_count.rc_value) THEN
   RAISE NOTICE 'Wait %', v_loop;
   RETURN NULL;
ELSE
   RAISE NOTICE 'This should conclude with an DELETE on _PERSON';
END IF;
         
create temporary table UJT (
   ssn VARCHAR(100),
   empid VARCHAR(100),
   name VARCHAR(100),
   hdate VARCHAR(100),
   phone VARCHAR(100),
   email VARCHAR(100),
   dept VARCHAR(100),
   manager VARCHAR(100)
);

INSERT INTO UJT (
   SELECT DISTINCT ssn, empid, name, hdate, phone, email, dept, manager
   FROM transducer._P_DELETE_JOIN 
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PE_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PE_HDATE_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PED_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PED_DEPT_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._DEPT_MANAGER_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_PHONE_DELETE_JOIN)
   NATURAL LEFT OUTER JOIN (SELECT DISTINCT * FROM transducer._PERSON_EMAIL_DELETE_JOIN)
   WHERE 
      ssn IS NOT NULL AND NAME IS NOT NULL 
   AND phone IS NOT NULL AND email IS NOT NULL
   
   AND ((empid IS NULL AND hdate IS NULL)
   OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NULL)
   OR (empid IS NOT NULL AND hdate IS NOT NULL AND dept IS NOT NULL AND manager IS NOT NULL)));


/*
FOR id_loop IN (SELECT CONCAT(ssn,phone,email) FROM UJT)
LOOP

IF EXISTS (SELECT * FROM UJT WHERE id_loop = CONCAT(ssn,phone,email)
            EXCEPT 
         (SELECT * FROM UJT WHERE empid IS NULL AND id_loop = CONCAT(ssn,phone,email))
            )THEN 
               DELETE FROM UJT WHERE empid IS NULL AND id_loop = CONCAT(ssn,phone,email);
END IF;

IF EXISTS (SELECT * FROM UJT WHERE id_loop = CONCAT(ssn,phone,email)
            EXCEPT 
         (SELECT * FROM UJT WHERE dept IS NULL AND id_loop = CONCAT(ssn,phone,email))
            )THEN 
               DELETE FROM UJT WHERE dept IS NULL AND id_loop = CONCAT(ssn,phone,email);
END IF;
END LOOP;
*/

DELETE FROM transducer._PERSON WHERE (ssn, phone, email) IN (SELECT DISTINCT ssn,phone,email FROM UJT);


DELETE FROM transducer._P_DELETE;
DELETE FROM transducer._PE_DELETE;
DELETE FROM transducer._PE_HDATE_DELETE;
DELETE FROM transducer._PED_DELETE;
DELETE FROM transducer._PED_DEPT_DELETE;
DELETE FROM transducer._DEPT_MANAGER_DELETE;
DELETE FROM transducer._PERSON_PHONE_DELETE;
DELETE FROM transducer._PERSON_EMAIL_DELETE;

DELETE FROM transducer._P_DELETE_JOIN;
DELETE FROM transducer._PE_DELETE_JOIN;
DELETE FROM transducer._PE_HDATE_DELETE_JOIN;
DELETE FROM transducer._PED_DELETE_JOIN;
DELETE FROM transducer._PED_DEPT_DELETE_JOIN;
DELETE FROM transducer._DEPT_MANAGER_DELETE_JOIN;
DELETE FROM transducer._PERSON_PHONE_DELETE_JOIN;
DELETE FROM transducer._PERSON_EMAIL_DELETE_JOIN;


DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END;    $$;




/* //////////////////////////////////////////////////////////////////////////////////////////////////////////////// */



/** S->T INSERT TRIGGERS **/

CREATE TRIGGER source_PERSON_INSERT_trigger
AFTER INSERT ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.source_PERSON_INSERT_fn();

CREATE TRIGGER source_PERSON_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_PERSON_INSERT_JOIN_fn();

CREATE TRIGGER source_INSERT_trigger_1
AFTER INSERT ON transducer._PERSON_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_INSERT_fn();

/** S->T DELETE TRIGGERS **/

CREATE TRIGGER source_PERSON_DELETE_trigger
BEFORE DELETE ON transducer._PERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.source_PERSON_DELETE_fn();

CREATE TRIGGER source_PERSON_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.source_PERSON_DELETE_JOIN_fn();

CREATE TRIGGER source_DELETE_trigger_1
AFTER INSERT ON transducer._PERSON_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_DELETE_fn();


/** T->S INSERT TRIGGERS **/

CREATE TRIGGER target_P_INSERT_trigger
AFTER INSERT ON transducer._P
FOR EACH ROW
EXECUTE FUNCTION transducer.target_P_INSERT_fn();

CREATE TRIGGER target_PE_INSERT_trigger
AFTER INSERT ON transducer._PE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_INSERT_fn();

CREATE TRIGGER target_PE_HDATE_INSERT_trigger
AFTER INSERT ON transducer._PE_HDATE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_HDATE_INSERT_fn();

CREATE TRIGGER target_PED_INSERT_trigger
AFTER INSERT ON transducer._PED
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_INSERT_fn();

CREATE TRIGGER target_PED_DEPT_INSERT_trigger
AFTER INSERT ON transducer._PED_DEPT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DEPT_INSERT_fn();

CREATE TRIGGER target_DEPT_MANAGER_INSERT_trigger
AFTER INSERT ON transducer._DEPT_MANAGER
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPT_MANAGER_INSERT_fn();

CREATE TRIGGER target_PERSON_PHONE_INSERT_trigger
AFTER INSERT ON transducer._PERSON_PHONE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_fn();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_trigger
AFTER INSERT ON transducer._PERSON_EMAIL
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_fn();




CREATE TRIGGER target_P_INSERT_JOIN_trigger
AFTER INSERT ON transducer._P_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_P_INSERT_JOIN_fn();

CREATE TRIGGER target_PE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_INSERT_JOIN_fn();

CREATE TRIGGER target_PE_HDATE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PE_HDATE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_HDATE_INSERT_JOIN_fn();

CREATE TRIGGER target_PED_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PED_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_INSERT_JOIN_fn();

CREATE TRIGGER target_PED_DEPT_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PED_DEPT_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DEPT_INSERT_JOIN_fn();

CREATE TRIGGER target_DEPT_MANAGER_INSERT_JOIN_trigger
AFTER INSERT ON transducer._DEPT_MANAGER_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPT_MANAGER_INSERT_JOIN_fn();

CREATE TRIGGER target_PERSON_PHONE_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_PHONE_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_INSERT_JOIN_fn();

CREATE TRIGGER target_PERSON_EMAIL_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_INSERT_JOIN_fn();


CREATE TRIGGER target_INSERT_trigger_1
AFTER INSERT ON transducer._P_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_2
AFTER INSERT ON transducer._PE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_3
AFTER INSERT ON transducer._PE_HDATE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_4
AFTER INSERT ON transducer._PED_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_5
AFTER INSERT ON transducer._PED_DEPT_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_6
AFTER INSERT ON transducer._DEPT_MANAGER_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_7
AFTER INSERT ON transducer._PERSON_PHONE_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_8
AFTER INSERT ON transducer._PERSON_EMAIL_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();



/** T->S DELETE TRIGGERS **/

CREATE TRIGGER target_P_DELETE_trigger
AFTER DELETE ON transducer._P
FOR EACH ROW
EXECUTE FUNCTION transducer.target_P_DELETE_fn();

CREATE TRIGGER target_PE_DELETE_trigger
AFTER DELETE ON transducer._PE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_DELETE_fn();

CREATE TRIGGER target_PE_HDATE_DELETE_trigger
AFTER DELETE ON transducer._PE_HDATE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_HDATE_DELETE_fn();

CREATE TRIGGER target_PED_DELETE_trigger
AFTER DELETE ON transducer._PED
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DELETE_fn();

CREATE TRIGGER target_PED_DEPT_DELETE_trigger
AFTER DELETE ON transducer._PED_DEPT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DEPT_DELETE_fn();

CREATE TRIGGER target_DEPT_MANAGER_DELETE_trigger
AFTER DELETE ON transducer._DEPT_MANAGER
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPT_MANAGER_DELETE_fn();

CREATE TRIGGER target_PERSON_PHONE_DELETE_trigger
AFTER DELETE ON transducer._PERSON_PHONE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_DELETE_fn();

CREATE TRIGGER target_PERSON_EMAIL_DELETE_trigger
AFTER DELETE ON transducer._PERSON_EMAIL
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_DELETE_fn();


CREATE TRIGGER target_P_DELETE_JOIN_trigger
AFTER INSERT ON transducer._P_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_P_DELETE_JOIN_fn();

CREATE TRIGGER target_PE_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PE_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_DELETE_JOIN_fn();

CREATE TRIGGER target_PE_HDATE_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PE_HDATE_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PE_HDATE_DELETE_JOIN_fn();

CREATE TRIGGER target_PED_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PED_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DELETE_JOIN_fn();

CREATE TRIGGER target_PED_DEPT_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PED_DEPT_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PED_DEPT_DELETE_JOIN_fn();

CREATE TRIGGER target_DEPT_MANAGER_DELETE_JOIN_trigger
AFTER INSERT ON transducer._DEPT_MANAGER_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DEPT_MANAGER_DELETE_JOIN_fn();

CREATE TRIGGER target_PERSON_PHONE_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_PHONE_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_PHONE_DELETE_JOIN_fn();

CREATE TRIGGER target_PERSON_EMAIL_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PERSON_EMAIL_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PERSON_EMAIL_DELETE_JOIN_fn();


CREATE TRIGGER target_DELETE_trigger_1
AFTER INSERT ON transducer._P_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_2
AFTER INSERT ON transducer._PE_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_3
AFTER INSERT ON transducer._PE_HDATE_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_4
AFTER INSERT ON transducer._PED_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_5
AFTER INSERT ON transducer._PED_DEPT_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_6
AFTER INSERT ON transducer._DEPT_MANAGER_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_7
AFTER INSERT ON transducer._PERSON_PHONE_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_8
AFTER INSERT ON transducer._PERSON_EMAIL_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();




