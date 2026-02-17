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


