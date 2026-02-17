DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._CTP
    (
      cid VARCHAR(100) NOT NULL,
      c_name VARCHAR(100) NOT NULL,
      pid VARCHAR(100) NOT NULL,
      p_name VARCHAR(100) NOT NULL,
      quantity VARCHAR(100) NOT NULL,
      tid VARCHAR(100) NOT NULL
    );

ALTER TABLE transducer._CTP ADD PRIMARY KEY (cid, pid, tid);


INSERT INTO transducer._CTP (cid, c_name, pid, p_name, quantity, tid) VALUES
('cid1', 'June',  'pid1', 'pn1', '5000', 't11'),
('cid2', 'Jovial', 'pid1', 'pn1', '5000', 't21'),
('cid2', 'Jovial',  'pid2', 'pn2', '300', 't22'),
('cid3', 'Jord',  'pid2', 'pn2', '300', 't32'),
('cid1', 'June',  'pid3', 'pn3', '5', 't13'),
('cid4', 'Jhin',  'pid4', 'pn4', '7000', 't44')
;


/* ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

CREATE TABLE transducer._CLIENT AS 
  SELECT DISTINCT cid, c_name FROM transducer._CTP;

CREATE TABLE transducer._TRANSACTION AS
SELECT DISTINCT cid, pid, tid FROM transducer._CTP;

CREATE TABLE transducer._PRODUCT AS 
SELECT DISTINCT pid, p_name, quantity FROM transducer._CTP;


/*BASE CONSTRAINTS*/

ALTER TABLE transducer._CLIENT ADD PRIMARY KEY (cid);

ALTER TABLE transducer._TRANSACTION ADD PRIMARY KEY (cid, pid, tid);

ALTER TABLE transducer._PRODUCT ADD PRIMARY KEY (pid);



ALTER TABLE transducer._TRANSACTION
ADD FOREIGN KEY (cid) REFERENCES transducer._CLIENT(cid);

ALTER TABLE transducer._TRANSACTION 
ADD FOREIGN KEY (pid) REFERENCES transducer._PRODUCT(pid);


/* ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

/* S -> T */

CREATE TABLE transducer._CTP_INSERT AS
SELECT * FROM transducer._CTP
WHERE 1<>1;

CREATE TABLE transducer._CTP_DELETE AS
SELECT * FROM transducer._CTP
WHERE 1<>1;



CREATE TABLE transducer._CTP_INSERT_JOIN  AS
SELECT * FROM transducer._CTP
WHERE 1<>1;

CREATE TABLE transducer._CTP_DELETE_JOIN AS
SELECT * FROM transducer._CTP
WHERE 1<>1;


/** INSERT T -> S **/

CREATE TABLE transducer._CLIENT_INSERT AS
SELECT * FROM transducer._CLIENT
WHERE 1<>1;

CREATE TABLE transducer._CLIENT_DELETE AS
SELECT * FROM transducer._CLIENT
WHERE 1<>1;

CREATE TABLE transducer._TRANSACTION_INSERT AS
SELECT * FROM transducer._TRANSACTION
WHERE 1<>1;

CREATE TABLE transducer._TRANSACTION_DELETE AS
SELECT * FROM transducer._TRANSACTION
WHERE 1<>1;

CREATE TABLE transducer._PRODUCT_INSERT AS
SELECT * FROM transducer._PRODUCT
WHERE 1<>1;

CREATE TABLE transducer._PRODUCT_DELETE AS
SELECT * FROM transducer._PRODUCT
WHERE 1<>1;


CREATE TABLE transducer._CLIENT_INSERT_JOIN AS
SELECT * FROM transducer._CLIENT
WHERE 1<>1;

CREATE TABLE transducer._CLIENT_DELETE_JOIN AS
SELECT * FROM transducer._CLIENT
WHERE 1<>1;

CREATE TABLE transducer._TRANSACTION_INSERT_JOIN AS
SELECT * FROM transducer._TRANSACTION
WHERE 1<>1;

CREATE TABLE transducer._TRANSACTION_DELETE_JOIN AS
SELECT * FROM transducer._TRANSACTION
WHERE 1<>1;

CREATE TABLE transducer._PRODUCT_INSERT_JOIN AS
SELECT * FROM transducer._PRODUCT
WHERE 1<>1;

CREATE TABLE transducer._PRODUCT_DELETE_JOIN AS
SELECT * FROM transducer._PRODUCT
WHERE 1<>1;


CREATE TABLE transducer._LOOP (
loop_start INT NOT NULL );


CREATE TABLE transducer._SWITCH (
update_type INT NOT NULL );

/* ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */

/** S->T INSERTS **/

CREATE OR REPLACE FUNCTION transducer.source_CTP_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN NULL;
ELSE
   INSERT INTO transducer._CTP_INSERT VALUES(NEW.cid, NEW.c_name, NEW.pid, NEW.p_name, NEW.quantity, NEW.tid);
   RETURN NEW;
END IF;
END;  $$;




CREATE OR REPLACE FUNCTION transducer.source_CTP_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   cid VARCHAR(100) NOT NULL,
   c_name  VARCHAR(100) NOT NULL,
   pid VARCHAR(100) NOT NULL,
   p_name VARCHAR(100) NOT NULL,
   quantity VARCHAR(100) NOT NULL,
   tid VARCHAR(100) NOT NULL
);

INSERT INTO temp_table (SELECT cid, c_name, pid, p_name, quantity, tid
FROM transducer._CTP_INSERT);


INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._CTP_INSERT_JOIN (SELECT cid, c_name, pid, p_name, quantity, tid FROM temp_table);

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
      cid VARCHAR(100) NOT NULL,
      c_name  VARCHAR(100) NOT NULL,
      pid VARCHAR(100) NOT NULL,
      p_name VARCHAR(100) NOT NULL,
      quantity VARCHAR(100) NOT NULL,
      tid VARCHAR(100) NOT NULL
);
   INSERT INTO UJT (
   SELECT DISTINCT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._CTP_INSERT_JOIN 
   WHERE 
   cid IS NOT NULL AND c_name  IS NOT NULL AND pid IS NOT NULL AND p_name IS NOT NULL
   AND quantity IS NOT NULL AND tid IS NOT NULL);

   /*I believe that as expected, we will encounter tuples of the form {a,b,c,d,e,f,g}, {a,b,c,d,e,f,NULL}.
   Now, following the proper method, we should use a loop to ensure that this case doesn't occur, but, too lazy,
   don't want to.*/

   /*IFs are mostly required for null values, so I believe that for most tables in T, we can just write*/


   INSERT INTO transducer._CLIENT (SELECT DISTINCT cid, c_name  FROM UJT
                                   WHERE cid IS NOT NULL AND c_name  IS NOT NULL) 
                                   ON CONFLICT (cid) DO NOTHING;

   INSERT INTO transducer._TRANSACTION (SELECT DISTINCT cid, pid, tid FROM UJT 
                                        WHERE cid IS NOT NULL AND pid IS NOT NULL AND tid IS NOT NULL) 
                                        ON CONFLICT (cid,pid,tid) DO NOTHING;

   INSERT INTO transducer._PRODUCT (SELECT DISTINCT pid, p_name, quantity FROM UJT
                                   WHERE pid IS NOT NULL AND p_name IS NOT NULL AND quantity IS NOT NULL) 
                                   ON CONFLICT (pid) DO NOTHING;




   DELETE FROM transducer._CTP_INSERT;
   DELETE FROM transducer._CTP_INSERT_JOIN;

   DELETE FROM transducer._loop;
   DELETE FROM UJT;
   DROP TABLE UJT;

   RETURN NEW;
END IF;
END;  $$;


/** S->T DELETES **/

CREATE OR REPLACE FUNCTION transducer.source_CTP_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = -1) THEN
   RETURN OLD;
ELSE
   RAISE NOTICE 'Starting DELETE from CTP';
   /*Whereas only one function for INSERTs is required, many should be considered for DELETES:
      - U1: Transaction has an unique deletion with non-unique cid and pid
      - U2: Client and Transaction have an unique deletion with an non-unique pid
      - U3: Product and Transaction have an unique deletion with a non-unique cid
      - U4: Client, Transaction and Product have a triple of unique cid,pid,tid values
   */


   /* U4 : The most straightforward condition. If the DELETEd values are nowhere to be found in their respective
      tables, then we DELETE from each target tables */
   IF NOT EXISTS(SELECT cid, pid, tid FROM transducer._CTP EXCEPT 
              SELECT cid, pid, tid FROM transducer._CTP WHERE cid != OLD.cid AND pid != OLD.pid)
   THEN
      INSERT INTO transducer._SWITCH VALUES (4);
      INSERT INTO transducer._CTP_DELETE VALUES(OLD.cid, OLD.c_name, OLD.pid, OLD.p_name, OLD.quantity, OLD.tid);
      RETURN OLD;
   END IF;


   /* U2 */
   IF EXISTS(SELECT ucount, rcount FROM 
            (SELECT COUNT(*) as rcount FROM transducer._CTP WHERE pid = OLD.pid OR tid = OLD.tid ) as rkeys,
            (SELECT COUNT(*) as ucount FROM transducer._CTP   WHERE cid = OLD.cid ) as ukeys
            WHERE ucount = 0 and rcount > 0)
   THEN
      INSERT INTO transducer._SWITCH VALUES (2);
      INSERT INTO transducer._CTP_DELETE VALUES(OLD.cid, OLD.c_name, OLD.pid, OLD.p_name, OLD.quantity, OLD.tid);
      RETURN OLD;
   END IF;

   /* U3 */
   IF EXISTS(SELECT ucount, rcount FROM 
            (SELECT COUNT(*) as ucount FROM transducer._CTP WHERE pid = OLD.pid OR tid = OLD.tid ) as ukeys,
            (SELECT COUNT(*) as rcount FROM transducer._CTP WHERE cid = OLD.cid ) as rkeys
            WHERE ucount = 0 and rcount > 0)
   THEN
      INSERT INTO transducer._SWITCH VALUES (3);
      INSERT INTO transducer._CTP_DELETE VALUES(OLD.cid, OLD.c_name, OLD.pid, OLD.p_name, OLD.quantity, OLD.tid);
      RETURN OLD;
   END IF;

   /*ELSE... U1*/
   INSERT INTO transducer._SWITCH VALUES (1);
   INSERT INTO transducer._CTP_DELETE VALUES(OLD.cid, OLD.c_name, OLD.pid, OLD.p_name, OLD.quantity, OLD.tid);
   RETURN OLD;
   
END IF;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.source_CTP_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
create temporary table temp_table(
   cid VARCHAR(100) NOT NULL,
   c_name VARCHAR(100) NOT NULL,
   pid VARCHAR(100) NOT NULL,
   p_name VARCHAR(100) NOT NULL,
   quantity VARCHAR(100) NOT NULL,
   tid VARCHAR(100) NOT NULL
);

INSERT INTO temp_table (SELECT cid, c_name, pid, p_name, quantity, tid
FROM transducer._CTP_DELETE);

INSERT INTO transducer._loop VALUES (1);
INSERT INTO transducer._CTP_DELETE_JOIN (SELECT cid, c_name, pid, p_name, quantity, tid FROM temp_table);

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

/*Here again, the UJT varies dependening on the update type*/


create temporary table UJT(
   cid VARCHAR(100) NOT NULL,
   c_name VARCHAR(100) NOT NULL,
   pid VARCHAR(100) NOT NULL,
   p_name VARCHAR(100) NOT NULL,
   quantity VARCHAR(100) NOT NULL,
   tid VARCHAR(100) NOT NULL
);
   INSERT INTO UJT (
   SELECT DISTINCT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._CTP_DELETE_JOIN 
   WHERE 
   cid IS NOT NULL AND c_name IS NOT NULL AND pid IS NOT NULL AND p_name IS NOT NULL
   AND quantity IS NOT NULL AND tid IS NOT NULL);

/*Now for the most important, the actual condition tree */
/*U1*/
IF EXISTS (SELECT update_type FROM transducer._SWITCH WHERE update_type = 1) THEN
   RAISE NOTICE 'UPDATE TYPE 1 : DELETE ONLY IN _TRANSACTION';
   DELETE FROM transducer._TRANSACTION WHERE tid IN (SELECT tid FROM UJT);
END IF;

/*U2*/
IF EXISTS (SELECT update_type FROM transducer._SWITCH WHERE update_type = 2) THEN
   RAISE NOTICE 'UPDATE TYPE 2 : DELETE ONLY IN _CLIENT and _TRANSACTION';
   DELETE FROM transducer._TRANSACTION WHERE tid IN (SELECT tid FROM UJT);
   DELETE FROM transducer._CLIENT WHERE cid IN (SELECT cid FROM UJT);
END IF;

/*U3*/
IF EXISTS (SELECT update_type FROM transducer._SWITCH WHERE update_type = 3) THEN
   RAISE NOTICE 'UPDATE TYPE 3 : DELETE ONLY IN _PRODUCT and _TRANSACTION';
   DELETE FROM transducer._TRANSACTION WHERE tid IN (SELECT tid FROM UJT);
   DELETE FROM transducer._PRODUCT WHERE pid IN (SELECT pid FROM UJT);
END IF;

/*U4*/
IF EXISTS (SELECT update_type FROM transducer._SWITCH WHERE update_type = 4) THEN
   RAISE NOTICE 'UPDATE TYPE 4 : DELETE in all tables';
   DELETE FROM transducer._TRANSACTION WHERE tid IN (SELECT tid FROM UJT);
   DELETE FROM transducer._PRODUCT WHERE pid IN (SELECT pid FROM UJT);
   DELETE FROM transducer._CLIENT WHERE cid IN (SELECT cid FROM UJT);
END IF;


DELETE FROM transducer._CTP_DELETE;
DELETE FROM transducer._CTP_DELETE_JOIN;

DELETE FROM transducer._loop;
DELETE FROM transducer._SWITCH;   
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END;  $$;



/** T->S INSERTS **/


CREATE OR REPLACE FUNCTION transducer.target_CLIENT_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _CLIENT';
   INSERT INTO transducer._CLIENT_INSERT VALUES(NEW.cid, NEW.c_name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TRANSACTION_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _TRANSACTION';
   INSERT INTO transducer._TRANSACTION_INSERT VALUES(NEW.cid, NEW.pid, NEW.tid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PRODUCT_INSERT_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting INSERTion from _PRODUCT';
   INSERT INTO transducer._PRODUCT_INSERT VALUES(NEW.pid, NEW.p_name, NEW.quantity);
   RETURN NEW;
END IF;
END;  $$;


CREATE OR REPLACE FUNCTION transducer.target_CLIENT_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   cid VARCHAR(100),
   c_name VARCHAR(100),
   pid VARCHAR(100),
   p_name VARCHAR(100),
   quantity VARCHAR(100),
   tid VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._CLIENT_INSERT 
   NATURAL LEFT OUTER JOIN transducer._TRANSACTION
   NATURAL LEFT OUTER JOIN transducer._PRODUCT);

INSERT INTO transducer._CLIENT_INSERT_JOIN (SELECT cid, c_name FROM temp_table);
INSERT INTO transducer._TRANSACTION_INSERT_JOIN (SELECT cid, pid, tid FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PRODUCT_INSERT_JOIN (SELECT pid, p_name, quantity FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TRANSACTION_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   cid VARCHAR(100),
   c_name VARCHAR(100),
   pid VARCHAR(100),
   p_name VARCHAR(100),
   quantity VARCHAR(100),
   tid VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._TRANSACTION_INSERT 
   NATURAL LEFT OUTER JOIN transducer._CLIENT
   NATURAL LEFT OUTER JOIN transducer._PRODUCT);

INSERT INTO transducer._CLIENT_INSERT_JOIN (SELECT cid, c_name FROM temp_table);
INSERT INTO transducer._TRANSACTION_INSERT_JOIN (SELECT cid, pid, tid FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PRODUCT_INSERT_JOIN (SELECT pid, p_name, quantity FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PRODUCT_INSERT_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   cid VARCHAR(100),
   c_name VARCHAR(100),
   pid VARCHAR(100),
   p_name VARCHAR(100),
   quantity VARCHAR(100),
   tid VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._PRODUCT_INSERT 
   NATURAL LEFT OUTER JOIN transducer._CLIENT
   NATURAL LEFT OUTER JOIN transducer._TRANSACTION);

INSERT INTO transducer._CLIENT_INSERT_JOIN (SELECT cid, c_name FROM temp_table);
INSERT INTO transducer._TRANSACTION_INSERT_JOIN (SELECT cid, pid, tid FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PRODUCT_INSERT_JOIN (SELECT pid, p_name, quantity FROM temp_table);

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
   RAISE NOTICE 'This should conclude with an INSERT on _CTP';
        
create temporary table UJT(
   cid VARCHAR(100) NOT NULL,
   c_name VARCHAR(100) NOT NULL,
   pid VARCHAR(100) NOT NULL,
   p_name VARCHAR(100) NOT NULL,
   quantity VARCHAR(100) NOT NULL,
   tid VARCHAR(100) NOT NULL
);
   INSERT INTO UJT (
   SELECT DISTINCT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._CLIENT_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._TRANSACTION_INSERT_JOIN
   NATURAL LEFT OUTER JOIN transducer._PRODUCT_INSERT_JOIN
   WHERE 
   cid IS NOT NULL AND c_name  IS NOT NULL AND pid IS NOT NULL AND p_name IS NOT NULL
   AND quantity IS NOT NULL AND tid IS NOT NULL);


INSERT INTO transducer._CTP (SELECT DISTINCT * FROM UJT) ON CONFLICT (cid,pid,tid) DO NOTHING;

DELETE FROM transducer._CLIENT_INSERT;
DELETE FROM transducer._TRANSACTION_INSERT;
DELETE FROM transducer._PRODUCT_INSERT;

DELETE FROM transducer._CLIENT_INSERT_JOIN;
DELETE FROM transducer._TRANSACTION_INSERT_JOIN;
DELETE FROM transducer._PRODUCT_INSERT_JOIN;

DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END IF;
END;    $$;



/** T->S DELETES **/


CREATE OR REPLACE FUNCTION transducer.target_CLIENT_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _CLIENT';
   INSERT INTO transducer._CLIENT_DELETE VALUES(OLD.cid, OLD.c_name);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TRANSACTION_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _TRANSACTION';
   INSERT INTO transducer._TRANSACTION_DELETE VALUES(OLD.cid, OLD.pid, OLD.tid);
   RETURN NEW;
END IF;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PRODUCT_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
IF EXISTS (SELECT * FROM transducer._loop WHERE loop_start = 1) THEN
   RETURN NULL;
ELSE
   RAISE NOTICE 'Starting DELETEion from _PRODUCT';
   INSERT INTO transducer._PRODUCT_DELETE VALUES(OLD.pid, OLD.p_name, OLD.quantity);
   RETURN NEW;
END IF;
END;  $$;



CREATE OR REPLACE FUNCTION transducer.target_CLIENT_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   cid VARCHAR(100),
   c_name VARCHAR(100),
   pid VARCHAR(100),
   p_name VARCHAR(100),
   quantity VARCHAR(100),
   tid VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._CLIENT_DELETE 
   NATURAL LEFT OUTER JOIN transducer._TRANSACTION
   NATURAL LEFT OUTER JOIN transducer._PRODUCT);

INSERT INTO transducer._CLIENT_DELETE_JOIN (SELECT cid, c_name FROM temp_table);
INSERT INTO transducer._TRANSACTION_DELETE_JOIN (SELECT cid, pid, tid FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PRODUCT_DELETE_JOIN (SELECT pid, p_name, quantity FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_TRANSACTION_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   cid VARCHAR(100),
   c_name VARCHAR(100),
   pid VARCHAR(100),
   p_name VARCHAR(100),
   quantity VARCHAR(100),
   tid VARCHAR(100)
);

INSERT INTO temp_table (
   SELECT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._TRANSACTION_DELETE 
   NATURAL LEFT OUTER JOIN transducer._CLIENT
   NATURAL LEFT OUTER JOIN transducer._PRODUCT);

INSERT INTO transducer._CLIENT_DELETE_JOIN (SELECT cid, c_name FROM temp_table);
INSERT INTO transducer._TRANSACTION_DELETE_JOIN (SELECT cid, pid, tid FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PRODUCT_DELETE_JOIN (SELECT pid, p_name, quantity FROM temp_table);

DELETE FROM temp_table;
DROP TABLE temp_table;
RETURN NEW;
END;  $$;

CREATE OR REPLACE FUNCTION transducer.target_PRODUCT_DELETE_JOIN_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

create temporary table temp_table(
   cid VARCHAR(100),
   c_name VARCHAR(100),
   pid VARCHAR(100),
   p_name VARCHAR(100),
   quantity VARCHAR(100),
   tid VARCHAR(100)
);
INSERT INTO temp_table (
   SELECT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._PRODUCT_DELETE 
   NATURAL LEFT OUTER JOIN transducer._CLIENT
   NATURAL LEFT OUTER JOIN transducer._TRANSACTION);

INSERT INTO transducer._CLIENT_DELETE_JOIN (SELECT cid, c_name FROM temp_table);
INSERT INTO transducer._TRANSACTION_DELETE_JOIN (SELECT cid, pid, tid FROM temp_table);
INSERT INTO transducer._loop VALUES (-1);
INSERT INTO transducer._PRODUCT_DELETE_JOIN (SELECT pid, p_name, quantity FROM temp_table);

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
   RAISE NOTICE 'This should conclude with an DELETE on _CTP';
        
create temporary table UJT(
   cid VARCHAR(100) NOT NULL,
   c_name  VARCHAR(100) NOT NULL,
   pid VARCHAR(100) NOT NULL,
   p_name VARCHAR(100) NOT NULL,
   quantity VARCHAR(100) NOT NULL,
   tid VARCHAR(100) NOT NULL
);
   INSERT INTO UJT (
   SELECT DISTINCT cid, c_name, pid, p_name, quantity, tid
   FROM transducer._CLIENT_DELETE_JOIN
   NATURAL LEFT OUTER JOIN transducer._TRANSACTION_DELETE_JOIN
   NATURAL LEFT OUTER JOIN transducer._PRODUCT_DELETE_JOIN
   WHERE 
   cid IS NOT NULL AND c_name  IS NOT NULL AND pid IS NOT NULL AND p_name IS NOT NULL
   AND quantity IS NOT NULL AND tid IS NOT NULL);


DELETE FROM transducer._CTP WHERE (cid,pid,tid) IN (SELECT cid, pid, tid FROM UJT);


DELETE FROM transducer._CLIENT_DELETE;
DELETE FROM transducer._TRANSACTION_DELETE;
DELETE FROM transducer._PRODUCT_DELETE;

DELETE FROM transducer._CLIENT_DELETE_JOIN;
DELETE FROM transducer._TRANSACTION_DELETE_JOIN;
DELETE FROM transducer._PRODUCT_DELETE_JOIN;

DELETE FROM transducer._loop;
DELETE FROM UJT;
DROP TABLE UJT;

RETURN NEW;
END IF;
END;    $$;


/* ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// */


/** S->T INSERT TRIGGERS **/

CREATE TRIGGER source_CTP_INSERT_trigger
AFTER INSERT ON transducer._CTP
FOR EACH ROW
EXECUTE FUNCTION transducer.source_CTP_INSERT_fn();


CREATE TRIGGER source_CTP_INSERT_JOIN_trigger
AFTER INSERT ON transducer._CTP_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.source_CTP_INSERT_JOIN_fn();


CREATE TRIGGER source_INSERT_trigger_1
AFTER INSERT ON transducer._CTP_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_INSERT_fn();


/** S->T DELETE TRIGGERS **/

CREATE TRIGGER source_CTP_DELETE_trigger
AFTER DELETE ON transducer._CTP
FOR EACH ROW
EXECUTE FUNCTION transducer.source_CTP_DELETE_fn();


CREATE TRIGGER source_CTP_DELETE_JOIN_trigger
AFTER INSERT ON transducer._CTP_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.source_CTP_DELETE_JOIN_fn();
END;

CREATE TRIGGER source_DELETE_trigger_1
AFTER INSERT ON transducer._CTP_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.source_DELETE_fn();


/** T->S INSERT **/

CREATE TRIGGER target_CLIENT_INSERT_trigger
AFTER INSERT ON transducer._CLIENT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CLIENT_INSERT_fn();

CREATE TRIGGER target_TRANSACTION_INSERT_trigger
AFTER INSERT ON transducer._TRANSACTION
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TRANSACTION_INSERT_fn();

CREATE TRIGGER target_PRODUCT_INSERT_trigger
AFTER INSERT ON transducer._PRODUCT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PRODUCT_INSERT_fn();



CREATE TRIGGER target_CLIENT_INSERT_JOIN_trigger
AFTER INSERT ON transducer._CLIENT_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CLIENT_INSERT_JOIN_fn();

CREATE TRIGGER target_TRANSACTION_INSERT_JOIN_trigger
AFTER INSERT ON transducer._TRANSACTION_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TRANSACTION_INSERT_JOIN_fn();

CREATE TRIGGER target_PRODUCT_INSERT_JOIN_trigger
AFTER INSERT ON transducer._PRODUCT_INSERT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PRODUCT_INSERT_JOIN_fn();


CREATE TRIGGER target_INSERT_trigger_1
AFTER INSERT ON transducer._CLIENT_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_2
AFTER INSERT ON transducer._TRANSACTION_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();

CREATE TRIGGER target_INSERT_trigger_4
AFTER INSERT ON transducer._PRODUCT_INSERT_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_INSERT_fn();


/** T->S DELETE **/

CREATE TRIGGER target_CLIENT_DELETE_trigger
AFTER DELETE ON transducer._CLIENT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CLIENT_DELETE_fn();

CREATE TRIGGER target_TRANSACTION_DELETE_trigger
AFTER DELETE ON transducer._TRANSACTION
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TRANSACTION_DELETE_fn();

CREATE TRIGGER target_PRODUCT_DELETE_trigger
AFTER DELETE ON transducer._PRODUCT
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PRODUCT_DELETE_fn();



CREATE TRIGGER target_CLIENT_DELETE_JOIN_trigger
AFTER INSERT ON transducer._CLIENT_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_CLIENT_DELETE_JOIN_fn();

CREATE TRIGGER target_TRANSACTION_DELETE_JOIN_trigger
AFTER INSERT ON transducer._TRANSACTION_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_TRANSACTION_DELETE_JOIN_fn();

CREATE TRIGGER target_PRODUCT_DELETE_JOIN_trigger
AFTER INSERT ON transducer._PRODUCT_DELETE
FOR EACH ROW
EXECUTE FUNCTION transducer.target_PRODUCT_DELETE_JOIN_fn();



CREATE TRIGGER target_DELETE_trigger_1
AFTER INSERT ON transducer._CLIENT_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_2
AFTER INSERT ON transducer._TRANSACTION_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

CREATE TRIGGER target_DELETE_trigger_4
AFTER INSERT ON transducer._PRODUCT_DELETE_JOIN
FOR EACH ROW
EXECUTE FUNCTION transducer.target_DELETE_fn();

