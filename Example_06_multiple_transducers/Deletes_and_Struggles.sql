DROP SCHEMA IF EXISTS transducer CASCADE;
CREATE SCHEMA transducer;

CREATE TABLE transducer._CTP
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );
    
CREATE TABLE transducer._CTP_U1
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );
    
CREATE TABLE transducer._CTP_U2
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );
    
CREATE TABLE transducer._CTP_U3
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );
    
CREATE TABLE transducer._CTP_U4
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );

CREATE TABLE transducer._CTP_U5
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );
    
CREATE TABLE transducer._CTP_U6
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );

CREATE TABLE transducer._CTP_U7
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );

CREATE TABLE transducer._CTP_U8
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );

CREATE TABLE transducer._CTP_U9
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );

CREATE TABLE transducer._CTP_U10
    (
        cid VARCHAR(100) NOT NULL,
        c_name VARCHAR(100) NOT NULL,
        subscribedTime VARCHAR(100),
        vipDate VARCHAR(100),
        pid VARCHAR(100) NOT NULL,
        p_name VARCHAR(100) NOT NULL,
        quantity VARCHAR(100) NOT NULL,
        tid VARCHAR(100) NOT NULL
    );

ALTER TABLE transducer._CTP ADD PRIMARY KEY (cid, pid, tid);


INSERT INTO transducer._CTP VALUES
('cid1', 'June', NULL, NULL, 'pid1', 'Book', '5000', 't11'),
('cid2', 'Jovial',  'sub1', NULL, 'pid1', 'Book', '5000', 't21'),
('cid2', 'Jovial',  'sub1', NULL, 'pid2', 'Curry', '300', 't22'),
('cid3', 'Jord',  NULL, NULL, 'pid2', 'Curry', '300', 't32'),
('cid1', 'June',  NULL, NULL,'pid3', 'List', '5', 't13'),
('cid4', 'Jhin',  NULL, NULL, 'pid4', 'Miso', '7000', 't44'),
('cid2', 'Jovial',  'sub1', NULL, 'pid5', 'Unwound - Repetition', '300', 't25'),
('cid5', 'Jarre',  'sub2', NULL, 'pid1', 'Book', '5000', 't51'),
('cid6', 'Jehf',  'sub3', NULL, 'pid6', 'Unwound - Leaves Turn Inside Me', '30', 't66'),

('cid7', 'Jon',  NULL , 'vip7', 'pid1', 'Book', '5000', 't71'),
('cid8', 'JJ',  NULL , 'vip8', 'pid8', 'Unwound - Last Train', '50', 't88'),
('cid9', 'Jharl',  'sub9' , 'vip9', 'pid1', 'Book', '5000', 't91'),
('cid10', 'Jloppse',  'sub10' , 'vip10', 'pid10', 'Mendelson - QQPART', '80', 't1010')
;

INSERT INTO transducer._CTP_U1  VALUES
('cid2', 'Jovial', 'sub1', NULL, 'pid1', 'Book', '5000', 't21')
;

INSERT INTO transducer._CTP_U2  VALUES
('cid3', 'Jord', NULL, NULL, 'pid2', 'Curry', '300', 't32')
;

INSERT INTO transducer._CTP_U3  VALUES
('cid1', 'June', NULL, NULL, 'pid3', 'List', '5', 't13')
;

INSERT INTO transducer._CTP_U4 VALUES
('cid4', 'Jhin', NULL, NULL, 'pid4', 'Miso', '7000', 't44')
;

INSERT INTO transducer._CTP_U5  VALUES
('cid5', 'Jarre',  'sub2', NULL, 'pid1', 'Book', '5000', 't51')
;

INSERT INTO transducer._CTP_U6  VALUES
('cid6', 'Jehf',  'sub3', NULL,  'pid6', 'Unwound - Leaves Turn Inside Me', '30', 't66')
;

INSERT INTO transducer._CTP_U7  VALUES
('cid7', 'Jon',  NULL , 'vip7', 'pid1', 'Book', '5000', 't71')
;

INSERT INTO transducer._CTP_U8  VALUES
('cid8', 'JJ',  NULL , 'vip8', 'pid8', 'Unwound - Last Train', '50', 't88')
;

INSERT INTO transducer._CTP_U9  VALUES
('cid9', 'Jharl',  'sub9' , 'vip9', 'pid1', 'Book', '5000', 't91')
;

INSERT INTO transducer._CTP_U10  VALUES
('cid10', 'Jloppse',  'sub10' , 'vip10', 'pid10', 'Mendelson - QQPART', '80', 't1010')
;


CREATE OR REPLACE FUNCTION transducer.source_CTP_DELETE_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN

/*U4, U6, U8, U10 */  
IF NOT EXISTS(SELECT cid, pid, tid FROM transducer._CTP EXCEPT 
              SELECT cid, pid, tid FROM transducer._CTP WHERE cid != OLD.cid AND pid != OLD.pid)
THEN
    IF EXISTS (SELECT ncount1, ncount2 FROM
              (SELECT COUNT(*) as ncount1 FROM transducer._CTP WHERE OLD.subscribedTime IS NOT NULL) as nullAtt1,
              (SELECT COUNT(*) as ncount2 FROM transducer._CTP WHERE OLD.vipDate IS NOT NULL) as nullAtt2
               WHERE ncount1 = 0 AND ncount2 = 0)
    THEN
        RAISE NOTICE 'U4 DETECTED';
    END IF;
    IF EXISTS (SELECT ncount1, ncount2 FROM
              (SELECT COUNT(*) as ncount1 FROM transducer._CTP WHERE OLD.subscribedTime IS NOT NULL) as nullAtt1,
              (SELECT COUNT(*) as ncount2 FROM transducer._CTP WHERE OLD.vipDate IS NOT NULL) as nullAtt2
               WHERE ncount1 > 0 AND ncount2 = 0)
    THEN
        RAISE NOTICE 'U6 DETECTED';
    END IF;
    IF EXISTS (SELECT ncount1, ncount2 FROM
              (SELECT COUNT(*) as ncount1 FROM transducer._CTP WHERE OLD.subscribedTime IS NOT NULL) as nullAtt1,
              (SELECT COUNT(*) as ncount2 FROM transducer._CTP WHERE OLD.vipDate IS NOT NULL) as nullAtt2
               WHERE ncount1 = 0 AND ncount2 > 0)
    THEN
        RAISE NOTICE 'U8 DETECTED';
    END IF;
    IF EXISTS (SELECT ncount1, ncount2 FROM
              (SELECT COUNT(*) as ncount1 FROM transducer._CTP WHERE OLD.subscribedTime IS NOT NULL) as nullAtt1,
              (SELECT COUNT(*) as ncount2 FROM transducer._CTP WHERE OLD.vipDate IS NOT NULL) as nullAtt2
               WHERE ncount1 > 0 AND ncount2 > 0)
    THEN
        RAISE NOTICE 'U10 DETECTED';
    END IF;
    RETURN OLD;
    END IF;
   
 
/* U2 */
IF EXISTS(SELECT ucount, rcount FROM 
         (SELECT COUNT(*) as rcount FROM transducer._CTP WHERE pid = OLD.pid OR tid = OLD.tid ) as rkeys,
         (SELECT COUNT(*) as ucount FROM transducer._CTP   WHERE cid = OLD.cid ) as ukeys
          WHERE ucount = 0 and rcount > 0 )
   THEN
      RAISE NOTICE 'U2 DETECTED';
      RETURN OLD;
   END IF;

   /* U3 */
   IF EXISTS(SELECT ucount, rcount FROM 
                (SELECT COUNT(*) as ucount FROM transducer._CTP WHERE pid = OLD.pid OR tid = OLD.tid ) as ukeys,
                (SELECT COUNT(*) as rcount FROM transducer._CTP WHERE cid = OLD.cid ) as rkeys
                WHERE ucount = 0 and rcount > 0)
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

