/*
In this document I compiled all of the generic SQL constraint function I thought off.
Through this document I use RNEW as a shorthand for the SQL variable containing either NEW or OLD value. Well, for now let's say only NEW.
In SQL proper, we define RNEW as such
*/

(SELECT NEW.X1, NEW.X2, ..., NEW.Xn) AS RNEW

/*
Starting with the Functional Dependency translation of a FD X1,X2,...,Xn -> Y1,Y2,...,Ym :
*/

IF EXISTS (
         SELECT * 
         FROM R, RNEW
         WHERE  R.X1 = RNEW.X1 
         AND      R.X2 = RNEW.X2
         /*...*/
         AND      R.Xn = RNEW.Xn
      
         AND R.Y1 <> R2.Y1
         AND R.Y2 <> R2.Y2
         /*...*/
         AND R.Ym <> R2.Ym
         ) THEN
            RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE FD CONSTRAINT IN R';
            RETURN NULL;
      ELSE
            RETURN NEW;
      END IF;

/*
Rather self-explanatory, this applies to each FD individually.

Unfortunately, unless struck by genius in the upcoming weeks, I cannot say the same for multivalue dependencies. As such, their SQL translation
amount to grouping all MVD over the same table together, and resolve them in a pair of function. Let's assume a set of MVDs X1 ->> Y1,..., Xn->>Yn, it get translated as such:
*/

IF EXISTS (
         SELECT DISTINCT R.<ALL COMPOSITE PRIMARY KEY>, RNEW.<ALL NON PK ATTRIBUTES>
         FROM R, RNEW
         WHERE R.X1 = RNEW.X1
         AND R.X2 = RNEW.X2
         ...
         AND R.Xn = RNEW.Xn
         EXCEPT
         SELECT *
         FROM R
         ) THEN
            RETURN NULL;
      ELSE
            RETURN NEW;
      END IF;

/*

In this query we basically check that the addition of a new tuple doesn't violate the set of MVD by adding something different to the rest.
To illustrate, in the table R:(X,Y,Z,T) with a MVD X ->> Y, we need to ensure that whenever we add a new Y value for a given X, the Z,T values remain the same.
This function check for this.

Then we generate the complement so to not have situations where {<x1, y11, z11, t1>, <x1, y12, z11, t1>, <x1, y11, z12, t1>} exists as tuple in R:(X,Y,Z,T),
X->>Y, X->>Z, get decomposed into R1:(X,T), R2:(X,Y), R3:(X,Z), populating the target database along the way and the re-composition of this target database generating
a new tuple <x1, y12, z12, t1> not present in the source database initially.

*/

INSERT INTO R(
         SELECT R.X1, ..., R.Xn, NEW.Y1, ..., R.Yn, R.REST
         FROM R
         WHERE R.X1 = NEW.X1
         UNION
         /*...*/
         UNION
         SELECT R.X1, ..., R.Xn, R.Y1, ..., NEW.Yn, R.REST
         FROM R
         WHERE R.Xn = NEW.Xn
         EXCEPT 
         (SELECT * FROM R));
      RETURN NEW; 


/*
Moving to the translation of guard dependencies, a constraint we use to express the possible presence of some condition over a given table. The notation can get a bit tricky,
as, for instance, we write for a table R:(X,Y,Z) that Y is nullable as: \sigma(Y = NULL)R != \emptyset.
But where it gets really interesting is when this guard constraint enforces a jointly null condition, meaning that a given set of attributes can only be nullable together. 
Writing \sigma(YZ = NULL)R != \emptyset is how we write this type of jointly null dependency. If instead Y and Z were independently null, we would write them as the following
pair \sigma(Y = NULL)R != \emptyset, \sigma(Z = NULL)R != \emptyset.

We translate this type of jointly null guard as:
*/

IF EXISTS (
         SELECT *
         FROM  RNEW
         WHERE (RNEW.Y IS NOT NULL AND RNEW.Z IS NULL)
         OR (RNEW.Y IS NULL AND RNEW.Z IS NOT NULL)
         )THEN
            RETURN NULL;
      ELSE
            RETURN NEW;
      END IF;

/*
Here, we check only for counterexample. If we were to check for tuples patterns satisfying a given condition instead, as this prospect sounds more efficient, 
then we would find ourselves block in any scenario where overlapping guard constraint exists. In the transducer example, empid,hdate can be jointly null, 
and so can empid,hdate,dept,manager.
Here, we cannot individually check for tuples satisfying either side of a jointly null guard independently:
Checking only for empid,hdate always strictly null or strictly non-null may work by itself, but once we add the other condition satisfied only whenever
empid,hdate,dept,manager are jointly null or non-null, we face a contradiction. We face a contradiction as it is possible for empid,hdate non-null to hold, while
empid,hdate,dept,manager non-null not to hold.
It's a whole kerfuffle as the negation of a nullable join is not the opposite condition but the set difference from the satisfy condition.

Anyway, I believe what we do for join dependencies is to merge them as well into a single constraint function, like we did for MVD beforehand.
That function would have to check for every incorrect tuple pattern configuration and prevent an insertion if any such tuple is found to satisfy one of these.


Surprisingly, adding a FD to this type of constraint doesn't affect the translation much.
*/


IF EXISTS (
            SELECT * 
            FROM  R, RNEW
            WHERE (RNEW.Y IS NOT NULL AND RNEW.Z IS NOT NULL 
            AND R.Y = RNEW.Y 
            AND R.Z <> RNEW.Z) 
            OR (RNEW.Y IS NULL AND RNEW.Z IS NOT NULL) 
            OR (RNEW.Y IS NOT NULL AND RNEW.Z IS NULL)
            ) THEN
               RETURN NULL;
         ELSE
               RETURN NEW;
END IF;

/*
We generate this type of function for each individual CFD found, adding to each the same batch of incorrect tuple patterns to cross.

...


...

Or, we could think about it differently and instead of repeating this bothersome battery of condition, we apply it once in its own function and we simplify
the CFD translation. Basically, in our example we would write four functions instead of three, the first one filtering away undesirable tuple patterns and the last
three being translated as such instead:
*/


IF EXISTS (
            SELECT * 
            FROM  R, RNEW
            WHERE (RNEW.Y IS NOT NULL AND RNEW.Z IS NOT NULL 
            AND R.Y = RNEW.Y 
            AND R.Z <> RNEW.Z) 
            ) THEN
               RETURN NULL;
         ELSE
               RETURN NEW;
END IF;

/*

Applied fully to our transducer example, we would get:

*/

CREATE OR REPLACE FUNCTION transducer.check_TUPLE_PATTERNS_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (SELECT * 
         FROM transducer._PERSON AS R1, 
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS RNEW
         WHERE 

            (RNEW.empid IS NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NULL AND RNEW.manager IS NOT NULL)
            OR (RNEW.empid IS NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NULL)
            OR (RNEW.empid IS NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NOT NULL)
            OR (RNEW.empid IS NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NULL AND RNEW.manager IS NULL)
            OR (RNEW.empid IS NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NULL AND RNEW.manager IS NOT NULL)
            OR (RNEW.empid IS NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NULL)
            OR (RNEW.empid IS NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NOT NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NULL AND RNEW.manager IS NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NULL AND RNEW.manager IS NOT NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NOT NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NULL AND RNEW.manager IS NOT NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NULL)

            ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES CONTAINS AN INCORRECT TUPLE PATTERN %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_1()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (
      SELECT * 
      FROM transducer._PERSON AS R1, 
      (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
      WHERE R2.empid IS NOT NULL AND R2.hdate IS NOT NULL
         AND R1.empid = R2.empid 
         AND R1.hdate <> R2.hdate
      ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT empid -> hdate %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (
      SELECT * 
      FROM transducer._PERSON AS R1, 
      (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
      WHERE R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL 
         AND R1.empid = R2.empid 
         AND R1.dept <> R2.dept
      ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT empid -> dept %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

CREATE OR REPLACE FUNCTION transducer.check_PERSON_CFD_FN_2()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF EXISTS (
      SELECT * 
      FROM transducer._PERSON AS R1, 
      (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS R2
      WHERE R2.empid IS NOT NULL AND R2.hdate IS NOT NULL AND R2.dept IS NOT NULL AND R2.manager IS NOT NULL 
         AND R1.dept = R2.dept 
         AND R1.manager <> R2.manager
      ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES VIOLATE THE CFD CONSTRAINT empid -> dept %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

/*
But then again, if we check for correct tuple patterns in a separate table, and have it require an operation over all conditions present over a given table,
why bother checking for counterexample and not just stop any tuple patterns not in the specified few:

*/

CREATE OR REPLACE FUNCTION transducer.check_TUPLE_PATTERNS_FN()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
   IF NOT EXISTS (SELECT * 
         FROM transducer._PERSON AS R1, 
         (SELECT NEW.ssn, NEW.empid, NEW.name, NEW.hdate, NEW.phone, NEW.email, NEW.dept, NEW.manager) AS RNEW
         WHERE 
            (RNEW.empid IS NULL AND RNEW.hdate IS NULL AND RNEW.dept IS NULL AND RNEW.manager IS NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NULL AND RNEW.manager IS NULL)
            OR (RNEW.empid IS NOT NULL AND RNEW.hdate IS NOT NULL AND RNEW.dept IS NOT NULL AND RNEW.manager IS NOT NULL)
            ) THEN
      RAISE EXCEPTION 'THIS ADDED VALUES CONTAINS AN INCORRECT TUPLE PATTERN %', NEW;
      RETURN NULL;
   ELSE
      RETURN NEW;
   END IF;
END;
$$;

/*
So much to do, so much to see figured out clearly. I'll play around with that this week end and I'll see if it's worth anything.



Moving on to the translation of internal inclusion dependency. This constraint expressing that the domain of value allowed for a set of attributes is constrained by
its existence in another set. For Z \subseteq X, {Z NULLABLE} we write

*/

{
   IF (NEW.Z IS NULL ) THEN
   RETURN NEW;
}
      
IF (NEW.Z = NEW.X ) THEN
   RETURN NEW;
      
IF EXISTS (
   SELECT DISTINCT NEW.Z  FROM R
   EXCEPT(
   SELECT X AS Z FROM R)
   ) THEN
      RETURN NULL;
ELSE
      RETURN NEW;
END IF;

/*
Extremely easy to understand once we spend a few minutes over it. We basically cast X in R as Z and do the set difference between the old domain of X/Z and the new one
of Z. If this query return something, this means that the value in the new Z is does not already exist in the table R, thus violating the internal inclusion dependency 
between Z and X.

Another fun one is the distinct constraint:
*/

IF EXISTS (
   SELECT * FROM  RNEW
   INTERSECT
   SELECT * FROM R2
) THEN
      RETURN NULL;
ELSE
      RETURN NEW;
END IF;

IF EXISTS (
   SELECT * FROM  RNEW
   INTERSECT
   SELECT * FROM R1
) THEN
      RETURN NULL;
ELSE
      RETURN NEW;
END IF;

/*
Pretty self-explanatory, if two table R1,R2 are defined as distinct distinct[R1,R2], then they must have the same signature and no shared tuples. As such, to insert into
R1, for instance, we need to check the intersection of NEW and R2. If the presented query return something this is obviously wrong. Other it's cool.





Also, this is somewhat unrelated but here is how I think we could write the target_inster_function. Well, the first half of it at least:

*/

create temporary table UJT(
      X1 DOMAIN(X),
      X2 DOMAIN(X2),
      ...
      Xm DOMAIN(Xm)
      );
      
INSERT INTO UJT (
SELECT DISTINCT X1, X2, ..., Xm
FROM R1_{INSERT/DELETE}_JOIN
NATURAL LEFT OUTER JOIN R2_{INSERT/DELETE}_JOIN
...
NATURAL LEFT OUTER JOIN Rn_{INSERT/DELETE}_JOIN
WHERE
         
<EACH NON NULL ATTRIBUTES IN [S/T]> IS NOT NULL

AND

<CONDITIONS DENOTING THE ALLOWED TUPLE PATTERNS IF APPLICABLE>
      
);
      
FOR i IN (SELECT CONCAT(<COMPOSITE PKS IN [S/T]>))
LOOP
      
   IF EXISTS ( 
      SELECT * 
      FROM UJ WHERE id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>)
      EXCEPT (SELECT * FROM UJ 
      WHERE X1 IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>))
      )THEN 
         DELETE FROM UJT 
         WHERE X1 IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>);
   END IF;
         
   ...
         
   IF EXISTS ( 
      SELECT * 
      FROM UJ WHERE id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>)
      EXCEPT (SELECT * FROM UJ 
      WHERE Xn IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>))
      )THEN 
         DELETE FROM UJT 
         WHERE Xn IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>);
   END IF;
         
END LOOP

/*

More will follow, soon, eventually.

*/