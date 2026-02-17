/*
Just a short compilation of how I thought of generically writing all SQL trigger functions.
We kinda omit all code but the main body of these functions
The use of bracket denotes variations as follows:
   -square brackets alternate between an function from the source/target
   -curly brackets alternate between INSERT/DELETE functions
   -angle brackets (although just <>) usually provides more information

Starting with the trigger function on update over a table R:
*/

IF EXISTS (SELECT * FROM LOOP WHERE loop_start = [-1/1]) THEN
      RETURN NULL;
ELSE
      RAISE NOTICE 'Starting {INSERT/DELETE} from R';
      INSERT INTO R_{INSERT/DELETE} 
      VALUES({NEW.X, NEW.X / OLD.X, OLD.X});
      RETURN NEW;
END IF;

/*
Moving to the trigger function on INSERTs over update tables, lets say Ri_UPDATE:
*/


create temporary table temp_table(
      X1 DOMAIN(X),
      X2 DOMAIN(X2),
      ...
      Xm DOMAIN(Xm)
      );
      
      INSERT INTO temp_table (
         SELECT DISTINCT X1, X2, ..., Xm
         FROM Ri_{INSERT/DELETE}
         NATURAL LEFT OUTER JOIN R1
         NATURAL LEFT OUTER JOIN R2
         ...
         NATURAL LEFT OUTER JOIN Ri-1
         NATURAL LEFT OUTER JOIN Ri+1
         ...
         NATURAL LEFT OUTER JOIN Rn
      );
      
      INSERT INTO R1_{INSERT/DELETE}_JOIN (SELECT  <R1 ATTRIBUTES> FROM temp_table);
      INSERT INTO R2_{INSERT/DELETE}_JOIN (SELECT <R2 ATTRIBUTES> FROM temp_table);
      ...
      INSERT INTO Rn-1_{INSERT/DELETE}_JOIN (SELECT <Rn-1 ATTRIBUTES> FROM temp_table);
      INSERT INTO transducer._loop VALUES ([1/-1]);
      INSERT INTO Rn_{INSERT/DELETE}_JOIN (SELECT <Rn ATTRIBUTES> FROM temp_table);
      
      DELETE FROM temp_table;
      DROP TABLE temp_table;
      RETURN NEW;

/*
So far so good. But this is where it gets a bit more intricate as we now unfold the trigger function for INSERTs over join tables Ri_UPDATE_JOIN.
This first describe how I though of defining the INSERT function. That being said, the stuff in curly brackets would also applies for the delete function.
Starting with the generation of the UJT:
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
            WHERE Xm IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>))
            )THEN 
               DELETE FROM UJT 
               WHERE Xm IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>);
         END IF;
      
      END LOOP

/*
This basically present a two step process of generating the universal join table in full and then filtering away all the generated garbage.
The filtering is first done in the query, preserving only tuples satisfying the tuple patterns allowed in the other database.
As for the loop, it may look intimidating but the idea is as follows:

We recall the main problem of this query being that it is possible for several tuple satisfying the tuple patterns on the other database to exist at the same time, 
with some obviously subsuming other. For instance, we could get:
{
{ssn1, empid1, Jex, date1, phone11, email11, dept1, empid1},
{ssn1, empid1, Jex, date1, phone11, email11, NULL, NULL},
{ssn1, NULL, Jex, NULL, phone11, email11, NULL, NULL}
}

All three tuples are valid, but only one should be inserted into the source table Person. 
Now, perhaps there is a way to curate the proper result somewhere upstream, but for now let's see what we can done for filtering.
The main check here is done with the following conditions:
*/

IF EXISTS ( 
   SELECT * FROM UJ WHERE id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>)
   EXCEPT 
   (SELECT * FROM UJ WHERE X1 IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>))
   )THEN 
         DELETE FROM UJT 
         WHERE X1 IS NULL AND id_loop = CONCAT(<COMPOSITE PKS IN [S/T]>);
END IF;

/*
It seems a bit complex but the principle is quite intuitive. In the first half of this query we basically fetch all tuple with a given unique identifier.
As we may face composite keys, such as in our example, I thought of using CONCAT to basically ensure a single unique identifier.
Then, in this generic function, we check for each nullable attribute if the difference between the tableau containing all tuple with the given identifier and the tableau
containing the same identifier but with one of its value set to null. If the difference is empty, this means that there is no subsuming tuple for this given attribute, and so we cool.
Otherwise, this means that we have a pair of tuple with the same unique identifier, one containing a null value where the other doesn't, thus justifying the removal from the UJT of all
tuples with this attribute set to null.
Well, all tuples with the given unique identifier.

And that's where the loop come into play as we can very well imagine multiple distinct entity being added in the same transaction, and so for the resulting UJT to contains the following:
{
{ssn1, empid1, Jex, date1, phone11, email11, dept1, empid1},
{ssn1, empid1, Jex, date1, phone11, email11, NULL, NULL},
{ssn1, NULL, Jex, NULL, phone11, email11, NULL, NULL},
{ssn2, empid2, Jord, date2, phone21, email21, NULL, NULL},
{ssn2, NULL, Jord, NULL, phone21, email21, NULL, NULL}
}

Here, we can't just broadly remove all tuples where dept is null as it would remove Jord from the UJT. As such, we browse with a loop all unique identifier and remove at this level.
Regarding the loop index, we declare it at the very start of the function, right before the begin clause as such:
*/

DECLARE
   id_loop VARCHAR(255);

/*
Ending the INSERT trigger function, we get:
*/

IF EXISTS (
      SELECT * FROM R1 WHERE <NON NULL CONSTRAINTS IN R1> IS NOT NULL
      )THEN
         INSERT INTO R1 (
            SELECT <ALL R1 ATTRIBUTES> 
            FROM UTJ WHERE
            WHERE <NON NULL CONSTRAINTS IN R1> IS NOT NULL) 
            ON CONFLICT DO NOTHING;
END IF;
   
...
   
IF EXISTS (
      SELECT * FROM Rn WHERE <NON NULL CONSTRAINTS IN Rn> IS NOT NULL
      )THEN
         INSERT INTO Rn (
            SELECT <ALL Rn ATTRIBUTES> 
            FROM UTJ WHERE
            WHERE <NON NULL CONSTRAINTS IN Rn> IS NOT NULL) 
            ON CONFLICT DO NOTHING;
END IF;
   
DELETE FROM R1_INSERT;
...
DELETE FROM Rn_INSERT;
   
DELETE FROM R1_INSERT_JOIN;
...
DELETE FROM Rn_INSERT_JOIN;
   
DROP TABLE UTJ;

/*
Relatively self-explanatory I'd say.
Now, there is some difference when it comes to the delete function.
Notably, within it we don't need to bother with a loop, an can just repeat this first half:
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


/*
And that's where we have to admit that we don't yet have a proper generic way of ensuring that deletes over the other database don't result in unnecessary deletes.
Even looking at the solution I found for our given example, I fail short to defining precisely how we could properly generalise this stuff:
*/

/*Phone*/
   IF EXISTS (
      SELECT R.ssn, R.empid, R.name, R.hdate, R.phone, R.email, R.dept, R.manager
      FROM PERSON_URA as R, UJT
      WHERE R.ssn = UJT.ssn 
      EXCEPT 
      SELECT R.ssn, R.empid, R.name, R.hdate, R.phone, R.email, R.dept, R.manager
      FROM PERSON_URA as R, UJT
      WHERE R.ssn = UJT.ssn AND R.phone = UJT.phone
      ) THEN
         DELETE FROM Person_Phone WHERE (ssn, phone) IN (SELECT ssn, phone FROM UJT);
   END IF;
   
/*Email*/
   IF EXISTS (
      SELECT R.ssn, R.empid, R.name, R.hdate, R.phone, R.email, R.dept, R.manager
      FROM PERSON_URA as R, UJT
      WHERE R.ssn = UJT.ssn 
      EXCEPT 
      SELECT R.ssn, R.empid, R.name, R.hdate, R.phone, R.email, R.dept, R.manager
      FROM PERSON_URA as R, UJT
      WHERE R.ssn = UJT.ssn AND R.email = UJT.email
      ) THEN
         DELETE FROM Person_Emain WHERE (ssn, email) IN (SELECT ssn, email FROM UJT);
   END IF;

/*Person*/
IF NOT EXISTS (
      SELECT R.ssn, R.empid, R.name, R.hdate, R.phone, R.email, R.dept, R.manager
      FROM PERSON_URA as R, UJT
      WHERE R.ssn = UJT.ssn
      EXCEPT
      SELECT * FROM UJT
      ) THEN
         IF NOT EXISTS (
            SELECT R.ssn, R.empid, R.name, R.hdate, R.phone, R.email, R.dept, R.manager
            FROM transducer._PERSON_URA as r1, UJT
            WHERE R.dept = UJT.dept
            EXCEPT
            SELECT * FROM UJT
            ) THEN
               DELETE FROM PED_Dept WHERE (empid) IN (SELECT empid FROM UJT);
               DELETE FROM Dept_Manager WHERE (dept) IN (SELECT dept FROM UJT);
               DELETE FROM PED WHERE (ssn) IN (SELECT ssn FROM UJT);
               DELETE FROM Employee_Date WHERE (empid) IN (SELECT empid FROM UJT);
               DELETE FROM Employee WHERE (ssn) IN (SELECT ssn FROM UJT);
               DELETE FROM Person_Phone WHERE (ssn, phone) IN (SELECT ssn, phone FROM UJT);
               DELETE FROM Person_Email WHERE (ssn, email) IN (SELECT ssn, email FROM UJT);
               DELETE FROM Person WHERE (ssn) IN (SELECT ssn FROM UJT);
         ELSE
               DELETE FROM PED_Dept WHERE (empid) IN (SELECT empid FROM UJT);
               DELETE FROM PED WHERE (ssn) IN (SELECT ssn FROM UJT);
               DELETE FROM Employee_Date WHERE (empid) IN (SELECT empid FROM UJT);
               DELETE FROM Employee WHERE (ssn) IN (SELECT ssn FROM UJT);
               DELETE FROM Person_Phone WHERE (ssn, phone) IN (SELECT ssn, phone FROM UJT);
               DELETE FROM Person_Email WHERE (ssn, email) IN (SELECT ssn, email FROM UJT);
               DELETE FROM Person WHERE (ssn) IN (SELECT ssn FROM UJT);
         END IF;
   END IF;

/*
It's not so complicated at face value. For instance, for the phone condition, we check if there exists other phone number associated to a given person.
If so, then we can remove one alone.
Otherwise, we skip it until we can actually delete a person, and we then delete from Person_Phone in that condition.
However, there remain an extra bit of difficulty incarnated by the Dept_Manager table.
The reason for this complexity is rather simple, this table denote another kind, another conceptual concept in principle independent from the concept of a person.
As such, it is possible for multiple employees to work in the same department, the source of evil.

That being said, the condition employed is, yet again, rather simple. We make two checks:
- One signifying the removal of a person
- One signifying the removal of a department

For the latter, if the removal of a person also lead to the removal of the last reference to a given department, then that later get deleted as well from Dept_Manager.
All that to said that, returning to the problem at hand, I have no idea at this point on how to automatically come up to these sets of condition.

A problem for another time, possibly fixed by the reinterpretation of the updates as increment/decrement of entity with a variable multiplicity in a pseudo bag semantics.
But we'll see that later.
*/