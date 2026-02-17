SELECT * FROM transducer._CTP;
SELECT * FROM transducer._CTP_U1;
SELECT * FROM transducer._CTP_U2;
SELECT * FROM transducer._CTP_U3;
SELECT * FROM transducer._CTP_U4;

/*U1*/
DELETE FROM transducer._CTP WHERE ssn = 'ssn5';
/*U2*/
DELETE FROM transducer._CTP WHERE ssn = 'ssn7';
/*U3*/
DELETE FROM transducer._CTP WHERE ssn = 'ssn3';
/*U4*/
DELETE FROM transducer._CTP WHERE ssn = 'ssn6';
/*U5*/
DELETE FROM transducer._CTP WHERE ssn = 'ssn4';

SELECT ucount, rcount FROM 
(SELECT COUNT(*) as ucount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U2 as r2 WHERE r1.ssn = r2.ssn) as r1, transducer._CTP_U2 as r2 
WHERE r1.ssn = r2.ssn OR r1.city = r2.city  ) as ukeys,
(SELECT COUNT(*) as rcount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U2 as r2 WHERE r1.ssn = r2.ssn) as r1, transducer._CTP_U2 as r2 
WHERE r1.dep = r2.dep OR r1.country = r2.country) as rkeys

SELECT ucount, rcount FROM 
(SELECT COUNT(*) as ucount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U2 as r2 WHERE r1.ssn = r2.ssn) as r1, transducer._CTP_U2 as r2 
WHERE r1.ssn = r2.ssn OR r1.city = r2.city OR r1.country = r2.country) as ukeys,
(SELECT COUNT(*) as rcount FROM 
(SELECT * FROM transducer._CTP EXCEPT SELECT r1.* FROM transducer._CTP as r1, transducer._CTP_U2 as r2 WHERE r1.ssn = r2.ssn) as r1, transducer._CTP_U2 as r2 
WHERE r1.dep = r2.dep) as rkeys



