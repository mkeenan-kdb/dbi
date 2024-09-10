Demonstration code - for teaching purposes

We want to catalogue kdb+ databases.
Cannot just load db (\l /path/to/db) because if the db is corrupted, that may not work
We want to label each column within a table, so it should gather metrics even if the table or specific columns are corrupted

==dbi.q
For each DB;
  1) Get a list of files and type of files
  2) Label binary, splayed, partitioned tables
  For each table;
    3) Gather metrics on table, columns
  4)store results to date partitioned db
==analyse.q
  1)Report on the metrics created by dbi.q
