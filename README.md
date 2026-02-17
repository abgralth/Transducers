# Transducer
Repertory Containing many of the transducer examples I made, including their evolution through time
The general core of each example is based on 5 structural SQL files, their aggregation in a final SQL script and an additional file for testing updates:
  1. The source relational schema and its set of translated dependencies
  2. The target relational schema created from a list of project/select mappings over the source schema
  3. The update tables associated with each table in both the source and target schema. For a given table R, we got 4 associated update tables:
     - R_INSERT
     - R_DELETE
     - R_INSERT_JOIN
     - R_DELETE_JOIN 
  4. The core of the architecture, the PL/pgSQL functions, ensures the propagation of the updates through the two layers of our architecture
  5. Finally, the trigger function detects new INSERTs and DELETEs over each table and calls the previously defined functions

Regarding each example, all attempt to introduce a new dimension of concept onto the mainframe identified in the first example.
The second example notably illustrates that not only does our architecture work for many-to-many tables, but it is possible to consider shortcuts and sub-transducers as a faster alternative to the main transducer process.
With the third example, we return to our lasting problematic of lacking a proper way of generalizing the delete functions. The crux of the problem is looking at the uniqueness of a newly deleted tuple and deducing how many tables on the other side of the transducer are in the range of the update.


        
  
