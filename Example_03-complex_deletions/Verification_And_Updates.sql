/* TESTING AREA */

SELECT * FROM transducer._CTP;

SELECT * FROM transducer._PRODUCT

SELECT cid, c_name, pid, p_name, quantity, tid FROM transducer._CLIENT
NATURAL LEFT OUTER JOIN transducer._TRANSACTION
NATURAL LEFT OUTER JOIN transducer._PRODUCT
ORDER BY tid;


/* UPDATES */

/*INSERT FROM S*/

/*U1*/
INSERT INTO transducer._CTP VALUES ('cid1', 'June', 'pid2', 'pn2', '300', 't12');
/*U2*/
INSERT INTO transducer._CTP VALUES ('cid5', 'Jebf', 'pid2', 'pn2', '300', 't52');
/*U3*/
INSERT INTO transducer._CTP VALUES ('cid1', 'June', 'pid5', 'pn5', '300', 't15');
/*U4*/
INSERT INTO transducer._CTP VALUES ('cid6', 'Jam', 'pid6', 'pn6', '4520', 't66');

/*DELETE FROM S*/

/*U1*/
DELETE FROM transducer._CTP WHERE tid = 't21';
/*U2*/
DELETE FROM transducer._CTP WHERE tid = 't32';
/*U3*/
DELETE FROM transducer._CTP WHERE tid = 't13';
/*U4*/
DELETE FROM transducer._CTP WHERE tid = 't44';

/*U1*/
DELETE FROM transducer._CTP WHERE tid = 't12';
/*U2*/
DELETE FROM transducer._CTP WHERE tid = 't52';
/*U3*/
DELETE FROM transducer._CTP WHERE tid = 't15';
/*U4*/
DELETE FROM transducer._CTP WHERE tid = 't66';


/*INSERT FROM T*/
/*U1*/
INSERT INTO transducer._TRANSACTION VALUES ('cid1', 'pid2', 't12');
/*U2*/
BEGIN;
INSERT INTO transducer._loop VALUES(3);
INSERT INTO transducer._CLIENT VALUES ('cid5', 'Jebf');
INSERT INTO transducer._TRANSACTION VALUES ('cid5', 'pid2', 't52');
END;
/*U3*/
BEGIN;
INSERT INTO transducer._loop VALUES(3);
INSERT INTO transducer._PRODUCT VALUES ('pid5', 'pn5', '300');
INSERT INTO transducer._TRANSACTION VALUES ('cid1', 'pid5', 't15');
END;
/*U4*/
BEGIN;
INSERT INTO transducer._loop VALUES(4);
INSERT INTO transducer._CLIENT VALUES ('cid6', 'Jam');
INSERT INTO transducer._PRODUCT VALUES ('pid6', 'pn6', '4520');
INSERT INTO transducer._TRANSACTION VALUES ('cid6', 'pid6', 't66');
END;

/*DELETE FROM T*/
/*U1*/
DELETE FROM transducer._TRANSACTION WHERE tid = 't12';
/*U2*/
BEGIN;
INSERT INTO transducer._loop VALUES(3);
DELETE FROM transducer._TRANSACTION WHERE tid = 't52';
DELETE FROM transducer._CLIENT WHERE cid = 'cid5';
END;
/*U3*/
BEGIN;
INSERT INTO transducer._loop VALUES(3);
DELETE FROM transducer._TRANSACTION WHERE tid = 't15';
DELETE FROM transducer._PRODUCT WHERE pid = 'pid5';
END;
/*U4*/
BEGIN;
INSERT INTO transducer._loop VALUES(4);
DELETE FROM transducer._TRANSACTION WHERE tid = 't66';
DELETE FROM transducer._PRODUCT WHERE pid = 'pid6';
DELETE FROM transducer._CLIENT WHERE cid = 'cid6';
END;



ROLLBACK;
