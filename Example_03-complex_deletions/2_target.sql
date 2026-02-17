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
