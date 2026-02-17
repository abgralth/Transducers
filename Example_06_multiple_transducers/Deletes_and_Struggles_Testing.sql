/* TESTING AREA */

SELECT * FROM transducer._CTP;
SELECT * FROM transducer._CTP_U1;
SELECT * FROM transducer._CTP_U2;
SELECT * FROM transducer._CTP_U3;
SELECT * FROM transducer._CTP_U4;

SELECT * FROM transducer._CTP
EXCEPT 
SELECT * FROM transducer._CTP_U1


/*U1*/
DELETE FROM transducer._CTP WHERE tid = 't21';
/*U2*/
DELETE FROM transducer._CTP WHERE tid = 't32';
/*U3*/
DELETE FROM transducer._CTP WHERE tid = 't13';
/*U4*/
DELETE FROM transducer._CTP WHERE tid = 't44';
/*U5*/
DELETE FROM transducer._CTP WHERE tid = 't51';
/*U6*/
DELETE FROM transducer._CTP WHERE tid = 't66';
/*U7*/
DELETE FROM transducer._CTP WHERE tid = 't71';
/*U8*/
DELETE FROM transducer._CTP WHERE tid = 't88';
/*U9*/
DELETE FROM transducer._CTP WHERE tid = 't91';
/*U10*/
DELETE FROM transducer._CTP WHERE tid = 't1010';


/* UPDATES */

/*Hmm, each time the first EXCEPT is require to simulate the after DELETE state of the table*/
/*This seems intriguing, I want to check it with other DELETEs*/
SELECT * FROM transducer._CTP
EXCEPT
SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U1 as r2
WHERE r1.tid = r2.tid
EXCEPT
SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U1 as r2
WHERE r1.cid != r2.cid AND r1.pid != r2.pid AND r1.tid != r2.tid

SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U2 as r2 WHERE r1.cid = r2.cid
INTERSECT
(
SELECT * FROM transducer._CTP
EXCEPT
SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U2 as r2 WHERE r1.tid = r2.tid
EXCEPT
SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U2 as r2 WHERE r1.cid != r2.cid AND r1.pid != r2.pid AND r1.tid != r2.tid
)

SELECT ucount, rcount FROM 
(SELECT COUNT(*) as ucount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U3 as r2 WHERE r1.tid = r2.tid) as r1, transducer._CTP_U3 as r2 
WHERE r1.pid = r2.pid OR r1.tid = r2.tid ) as ukeys,
(SELECT COUNT(*) as rcount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U3 as r2 WHERE r1.tid = r2.tid) as r1, transducer._CTP_U3 as r2 
WHERE r1.cid = r2.cid ) as rkeys
WHERE ucount = 1 and rcount = 0


SELECT * FROM
(SELECT COUNT(*) as ucount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U3 as r2 WHERE r1.tid = r2.tid) as r1, transducer._CTP_U3 as r2 
WHERE r1.pid = r2.pid OR r1.tid = r2.tid )
WHERE ucount != 0


SELECT * FROM 
(SELECT COUNT(*) as ucount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U3 as r2 WHERE r1.tid = r2.tid) as r1, transducer._CTP_U3 as r2 
WHERE r1.tid = r2.tid  ) as ukeys,
(SELECT COUNT(*) as rcount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U3 as r2 WHERE r1.tid = r2.tid) as r1, transducer._CTP_U3 as r2 
WHERE r1.cid = r2.cid OR r1.pid = r2.pid) as rkeys


SELECT ucount, rcount FROM 
(SELECT COUNT(*) as ucount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U3 as r2 WHERE r1.tid = r2.tid) as r1, transducer._CTP_U3 as r2 
WHERE r1.pid = r2.pid OR r1.tid = r2.tid  ) as ukeys,
(SELECT COUNT(*) as rcount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U3 as r2 WHERE r1.tid = r2.tid) as r1, transducer._CTP_U3 as r2 
WHERE r1.cid = r2.cid) as rkeys













