# dh-staff-ae
- query_1 folder contains all models needed.

- query_2_and_3.sql has the queries requested. The wording in request for query 3 was a bit confusing. If the query provided in this file doesn't answer the request, then it could be answered directly from querying the model in query_1/session_summary.sql

- **At the top of each file there's a couple of notes about the model.**

- Since dbt usage is assumed:

    - I haven't included the code for updating the tables in the script through a merge or delete+insert statements. I would use the config block and is_incremental function in the correct place in dbt instead.
    
    - Primary and foregin key constraints would also be added in the model yaml files. Surrogate keys would be generated using the dbt_utils package function.

    - In general, models would be partitioned by a date field. 