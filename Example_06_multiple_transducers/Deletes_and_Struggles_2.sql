DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._CTP
    (
        ssn VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
		country VARCHAR(100) NOT NULL,
        dep VARCHAR(100) NOT NULL
    );
	
CREATE TABLE transducer._CTP_U1
    (
        ssn VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
		country VARCHAR(100) NOT NULL,
        dep VARCHAR(100) NOT NULL
    );

CREATE TABLE transducer._CTP_U2
    (
        ssn VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
		country VARCHAR(100) NOT NULL,
        dep VARCHAR(100) NOT NULL
    );
	
CREATE TABLE transducer._CTP_U3
    (
        ssn VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
		country VARCHAR(100) NOT NULL,
        dep VARCHAR(100) NOT NULL
    );
	
CREATE TABLE transducer._CTP_U4
    (
        ssn VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
		country VARCHAR(100) NOT NULL,
        dep VARCHAR(100) NOT NULL
    );
	
CREATE TABLE transducer._CTP_U5
    (
        ssn VARCHAR(100) NOT NULL,
        city VARCHAR(100) NOT NULL,
		country VARCHAR(100) NOT NULL,
        dep VARCHAR(100) NOT NULL
    );
	
ALTER TABLE transducer._CTP ADD PRIMARY KEY (ssn);


INSERT INTO transducer._CTP VALUES
('ssn1', 'city1', 'country1', 'dep1'),
('ssn2', 'city2', 'country2', 'dep2'),
('ssn3', 'city2', 'country2', 'dep3'),
('ssn4', 'city4', 'country4', 'dep1'),
('ssn5', 'city2', 'country2', 'dep2'),
('ssn6', 'city6', 'country6', 'dep6'),
('ssn7', 'city7', 'country1', 'dep2')
;

INSERT INTO transducer._CTP_U1 VALUES ('ssn5', 'city2', 'country2', 'dep2');
INSERT INTO transducer._CTP_U2 VALUES ('ssn7', 'city7', 'country1', 'dep2');
INSERT INTO transducer._CTP_U3 VALUES ('ssn3', 'city2', 'country2', 'dep3');
INSERT INTO transducer._CTP_U4 VALUES ('ssn6', 'city6', 'country6', 'dep6');
INSERT INTO transducer._CTP_U5 VALUES ('ssn4', 'city4', 'country4', 'dep1');


CREATE OR REPLACE FUNCTION transducer.source_CTP_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

/*U4*/
IF NOT EXISTS(SELECT * FROM transducer._CTP EXCEPT 
              SELECT * FROM transducer._CTP WHERE ssn != OLD.ssn AND city != OLD.city AND dep != OLD.dep AND country != OLD.country)
THEN
	RAISE NOTICE 'U4 DETECTED';
	RETURN OLD;
END IF;

/*U5*/
IF EXISTS(SELECT ucount, rcount FROM 
         (SELECT COUNT(*) as ucount FROM transducer._CTP WHERE ssn = OLD.ssn OR city = OLD.city OR country = OLD.country) as ukeys,
         (SELECT COUNT(*) as rcount FROM transducer._CTP WHERE dep = OLD.dep) as rkeys
          WHERE ucount = 0 and rcount > 0 )
THEN
	RAISE NOTICE 'U5 DETECTED';
	RETURN OLD;
END IF;

/*U2*/
IF EXISTS(SELECT ucount, rcount FROM 
         (SELECT COUNT(*) as ucount FROM transducer._CTP WHERE ssn = OLD.ssn OR city = OLD.city ) as ukeys,
         (SELECT COUNT(*) as rcount FROM transducer._CTP   WHERE dep = OLD.dep OR country = OLD.country ) as rkeys
          WHERE ucount = 0 and rcount > 0 )
THEN
	RAISE NOTICE 'U2 DETECTED';
	RETURN OLD;
END IF;

/*U3*/
IF EXISTS(SELECT ucount, rcount FROM 
         (SELECT COUNT(*) as ucount FROM transducer._CTP WHERE ssn = OLD.ssn OR dep = OLD.dep ) as ukeys,
         (SELECT COUNT(*) as rcount FROM transducer._CTP   WHERE city = OLD.city OR country = OLD.country ) as rkeys
          WHERE ucount = 0 and rcount > 0 )
THEN
	RAISE NOTICE 'U3 DETECTED';
	RETURN OLD;
END IF;


/*ELSE... U1*/
RAISE NOTICE 'U1 DETECTED';
RETURN OLD;

END;  $$;

CREATE TRIGGER source_CTP_DELETE_trigger
AFTER DELETE ON transducer._CTP
FOR EACH ROW
EXECUTE FUNCTION transducer.source_CTP_DELETE_fn();