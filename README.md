# Transducer
Repertory Containing many of the transducer examples I made, including their evolution through time
The general core of each example is based on 5 structural SQL files, their aggregation in final SQL script and an additional file for testing updates:
  1. The source relational schema and its set of translated dependencies
  2. The target relational schema created from a list of project/select mappings over the source schema
  3. The update tables associated to each tables in both the source and target schema. For a given table R we got 4 associated update tables:
     - R_INSERT
     - R_DELETE
     - R_INSERT_JOIN
     - R_DELETE_JOIN 
  4. The core of the architecture, the PL/pgSQL functions ensuring the propagation of the updates through the two layers of our architecture
  5. Finally, the trigger function detecting new INSERTs and DELETEs over each table and calling the previously defined functions


        
  
