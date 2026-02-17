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
    	coordinates VARCHAR(100)
    );

ALTER TABLE transducer._SCITY ADD PRIMARY KEY (city_name);
ALTER TABLE transducer._SPERSON ADD PRIMARY KEY (ssn,phone,email);
ALTER TABLE transducer._SPERSON ADD FOREIGN KEY (city_name) REFERENCES transducer._SCITY(city_name);


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
	IF EXISTS 	(SELECT DISTINCT r1.ssn, r2.name, r1.phone, NEW.email, r2.city_name 
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



CREATE OR REPLACE TRIGGER SPERSON_mvd_trigger_1
BEFORE INSERT ON transducer._SPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_SPERSON_mvd_fn_1();

CREATE OR REPLACE TRIGGER SPERSON_mvd_trigger_2
AFTER INSERT ON transducer._SPERSON
FOR EACH ROW
EXECUTE FUNCTION transducer.check_SPERSON_mvd_fn_2();


INSERT INTO transducer._SCITY (city_name, country, coordinates) VALUES
('Paris', 'France', 'coord1'),
('New York', 'US', NULL)
;

INSERT INTO transducer._SPERSON (ssn, name, phone, email, city_name) VALUES
('ssn1', 'June',  'phone11', 'mail11', 'Paris'),
('ssn2', 'Jovial',  'phone21', 'mail21', 'Paris'),
('ssn3', 'Jord',  'phone31', 'mail31', 'New York')
;


