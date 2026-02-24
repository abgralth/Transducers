/* TESTING AREA */

SELECT * FROM transducer._PERSON ORDER BY ssn;

SELECT * FROM transducer._P;
SELECT * FROM transducer._PE;
SELECT * FROM transducer._PE_HDATE;
SELECT * FROM transducer._PED;
SELECT * FROM transducer._PED_DEPT;
SELECT * FROM transducer._DEPT_MANAGER;
SELECT * FROM transducer._PERSON_PHONE;
SELECT * FROM transducer._PERSON_EMAIL;

SELECT ssn, empid, name, hdate, phone, email, dept, manager
FROM transducer._P
NATURAL LEFT OUTER JOIN transducer._PE
NATURAL LEFT OUTER JOIN transducer._PE_HDATE
NATURAL LEFT OUTER JOIN transducer._PED
   NATURAL LEFT OUTER JOIN transducer._PED_DEPT
   NATURAL LEFT OUTER JOIN transducer._DEPT_MANAGER
   NATURAL LEFT OUTER JOIN transducer._PERSON_PHONE
   NATURAL LEFT OUTER JOIN transducer._PERSON_EMAIL
ORDER BY ssn;



/* UPDATES */
/* INSERT FROM SOURCE */
/* BASIC NEW INSERT */
INSERT INTO transducer._PERSON VALUES ('ssn4', 'emp4', 'Jitte', 'hdate4', 'phone41', 'mail42', 'dep1', 'ssn1');

/* MVD RELATED INSERTS (see how adding those two tuples lead to the creation of a fourth one with phone12,mail12) */
INSERT INTO transducer._PERSON VALUES ('ssn1', 'emp1', 'June', 'hdate1', 'phone12', 'mail11', 'dep1', 'ssn1');
INSERT INTO transducer._PERSON VALUES ('ssn1', 'emp1', 'June', 'hdate1', 'phone11', 'mail12', 'dep1', 'ssn1');


/*INCORRECT UPDATE VIOLATING THE MVD ssn->>phone and ssn->>email */
INSERT INTO transducer._PERSON VALUES ('ssn4', 'emp4', 'Jnope', 'hdate4', 'phone42', 'mail42', 'dep1', 'ssn1');
/*INCORRECT UPDATE VIOLATING THE CFD empid->dep */
INSERT INTO transducer._PERSON VALUES ('ssn4', 'emp4', 'Jitte', 'hdate4', 'phone41', 'mail42', 'dep2', 'ssn2');
/*INCORRECT UPDATE VIOLATING THE CFD dep->manager */
INSERT INTO transducer._PERSON VALUES ('ssn4', 'emp4', 'Jitte', 'hdate4', 'phone41', 'mail42', 'dep1', 'ssn2');
/*INCORRECT UPDATE VIOLATING THE CFD empid->hdate */
INSERT INTO transducer._PERSON VALUES ('ssn4', 'emp4', 'Jitte', 'hdate5', 'phone41', 'mail42', 'dep1', 'ssn1');
/*INCORRECT UPDATE VIOLATING THE INC managerCssn */
INSERT INTO transducer._PERSON VALUES ('ssn5', 'emp5', 'Joff', 'hdate5', 'phone51', 'mail51', 'dep3', 'ssn6');

/* DELETE FROM SOURCE */

/* INCORRECT DELETE VIOLATING THE INC managerCssn*/
DELETE FROM transducer._PERSON WHERE  ssn = 'ssn1';

/* WORKING DELETION */
DELETE FROM transducer._PERSON WHERE  ssn = 'ssn4';


/* INSERT FROM TARGET */
/* OVIOUSLY INCORRECT INSERTS */
INSERT INTO transducer._PED_DEPT VALUES ('emp5', 'dep3');
INSERT INTO transducer._PED VALUES ('ssn5','emp5');
INSERT INTO transducer._PE VALUES ('ssn5','emp5');
INSERT INTO transducer._PE_HDATE VALUES ('emp5','hdate5');


/* BUNCH OF INDEPENDENT INDEPENDENT INSERTS RELATED TO THE MVDS */
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn2', 'phone22');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn2', 'mail22');
  
/* BASIC PERSON INSERT FROM SOURCE */
BEGIN;
INSERT INTO transducer._loop VALUES (4);
INSERT INTO transducer._P VALUES ('ssn6', 'Jolly');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn6', 'phone61');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn6', 'mail61');
END;

/* BASIC PERSON_EMPLOYEE INSERT FROM SOURCE */
BEGIN;
INSERT INTO transducer._loop VALUES (6);
INSERT INTO transducer._P VALUES ('ssn5', 'Jex');
INSERT INTO transducer._PE VALUES ('ssn5', 'emp5');
INSERT INTO transducer._PE_HDATE VALUES ('emp5', 'hdate5');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn5', 'phone51');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn5', 'mail51');
END;

/* BASIC PERSON_EMPLOYEE_DEP INSERT FROM SOURCE */
BEGIN;
INSERT INTO transducer._loop VALUES (9);
INSERT INTO transducer._P VALUES ('ssn7', 'Jad');
INSERT INTO transducer._PE VALUES ('ssn7', 'emp7');
INSERT INTO transducer._PE_HDATE VALUES ('emp7', 'hdate7');
INSERT INTO transducer._PED VALUES ('ssn7', 'emp7');
INSERT INTO transducer._DEPT_MANAGER VALUES ('dep2', 'ssn7');
INSERT INTO transducer._PED_DEPT VALUES ('emp7', 'dep2');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn7', 'phone71');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn7', 'mail71');
END;

/* MULTIPLE INSERTS IN A SINGLE TRANSACTION */
BEGIN;
INSERT INTO transducer._loop VALUES (19);
INSERT INTO transducer._P VALUES ('ssn8', 'Jund');
INSERT INTO transducer._PE VALUES ('ssn8', 'emp8');
INSERT INTO transducer._PE_HDATE VALUES ('emp8', 'hdate8');
INSERT INTO transducer._PED VALUES ('ssn8', 'emp8');
INSERT INTO transducer._DEPT_MANAGER VALUES ('dep3', 'ssn8');
INSERT INTO transducer._PED_DEPT VALUES ('emp8', 'dep3');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn8', 'phone81');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn8', 'mail81');
INSERT INTO transducer._P VALUES ('ssn9', 'Jeppy');
INSERT INTO transducer._PE VALUES ('ssn9', 'emp9');
INSERT INTO transducer._PE_HDATE VALUES ('emp9', 'hdate9');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn9', 'phone91');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn9', 'mail91');
INSERT INTO transducer._P VALUES ('ssn10', 'Jiff');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn10', 'phone101');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn10', 'phone102');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn10', 'mail101');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn10', 'mail102');
END;

/* INSERT OF A EMPLOYEE_DEP OF AN ALREADY EXISTING DEPARTMENT */
BEGIN;
INSERT INTO transducer._loop VALUES (8);
INSERT INTO transducer._P VALUES ('ssn11', 'Jirge');
INSERT INTO transducer._PE VALUES ('ssn11', 'emp11');
INSERT INTO transducer._PE_HDATE VALUES ('emp11', 'hdate11');
INSERT INTO transducer._PED VALUES ('ssn11', 'emp11');
INSERT INTO transducer._PED_DEPT VALUES ('emp11', 'dep2');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn11', 'phone111');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn11', 'mail111');
END;


/* DELETE FROM TARGET */
/* DELETE OF OUR DEAR EMPLOYEE_DEP, JIRGE (Keeping the department dep2 for now)*/
BEGIN;
INSERT INTO transducer._loop VALUES (8);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn11';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn11';
DELETE FROM transducer._PED_DEPT WHERE empid = 'emp11';
DELETE FROM transducer._PED WHERE empid = 'emp11';
DELETE FROM transducer._PE_HDATE WHERE empid = 'emp11';
DELETE FROM transducer._PE WHERE empid = 'emp11';
DELETE FROM transducer._P WHERE ssn = 'ssn11';
END;

/* DELETE OF JOVIAL AND THEIR DEPARTMENT */
BEGIN;
INSERT INTO transducer._loop VALUES (9);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn7';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn7';
DELETE FROM transducer._PED_DEPT WHERE empid = 'emp7';
DELETE FROM transducer._DEPT_MANAGER WHERE dept = 'dep2';
DELETE FROM transducer._PED WHERE empid = 'emp7';
DELETE FROM transducer._PE_HDATE WHERE empid = 'emp7';
DELETE FROM transducer._PE WHERE empid = 'emp7';
DELETE FROM transducer._P WHERE ssn = 'ssn7';
END;

/* DELETE OF AN EMPLOYEE */
BEGIN;
INSERT INTO transducer._loop VALUES (6);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn5';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn5';
DELETE FROM transducer._PE_HDATE WHERE empid = 'emp5';
DELETE FROM transducer._PE WHERE empid = 'emp5';
DELETE FROM transducer._P WHERE ssn = 'ssn5';
END;

/* DELETE OF A PERSON */
BEGIN;
INSERT INTO transducer._loop VALUES (4);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn6';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn6';
DELETE FROM transducer._P WHERE ssn = 'ssn6';
END;


/* MASS DELETE OF SEVERAL PERSONS */
BEGIN;
INSERT INTO transducer._loop VALUES (17);
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn8';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn8';
DELETE FROM transducer._PED_DEPT WHERE empid = 'emp8';
DELETE FROM transducer._DEPT_MANAGER WHERE dept = 'dep3';
DELETE FROM transducer._PED WHERE empid = 'emp8';
DELETE FROM transducer._PE_HDATE WHERE empid = 'emp8';
DELETE FROM transducer._PE WHERE empid = 'emp8';
DELETE FROM transducer._P WHERE ssn = 'ssn8';
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn9';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn9';
DELETE FROM transducer._PE_HDATE WHERE empid = 'emp9';
DELETE FROM transducer._PE WHERE empid = 'emp9';
DELETE FROM transducer._P WHERE ssn = 'ssn9';
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn10';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn10';
DELETE FROM transducer._P WHERE ssn = 'ssn10';
END;

/*Trying to INSERT AND DELETE AT THE SAME TIME (nothing happened?) */
BEGIN;
INSERT INTO transducer._loop VALUES (7);
INSERT INTO transducer._P VALUES ('ssn6', 'Jolly');
INSERT INTO transducer._PERSON_PHONE VALUES ('ssn6', 'phone61');
INSERT INTO transducer._PERSON_EMAIL VALUES ('ssn6', 'mail61');
DELETE FROM transducer._PERSON_PHONE WHERE ssn = 'ssn6';
DELETE FROM transducer._PERSON_EMAIL WHERE ssn = 'ssn6';
DELETE FROM transducer._P WHERE ssn = 'ssn6';
END;

ROLLBACK;
