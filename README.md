# FireX Keeper

FireX Keeper is a lighweight process that collects & persists data from a FireX execution. 
If you are not already familiar with FireX, it's worthwhile [reading the docs](http://www.firexapp.com/)
 before continuing here. 


Briefy, FireX is a general-purpose automation platform that affords task definition and execution 
by writing lightly annotated Python code. 

FireX Keeper creates an SQLite database that can be queried for task results, runstates (success,
failure, etc), or arguments. See db_model.py for the full database schema. 


