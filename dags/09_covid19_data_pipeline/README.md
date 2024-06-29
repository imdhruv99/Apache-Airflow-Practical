## Covid19 Data Pipeline

This ETL will consume open source Covid19 API, which gives details for india's statewide covid19 cases information.


### API
- I am using below API to fetch information 
`https://data.covid19india.org/v4/min/timeseries.min.json`

#### Prerequisites

- Before running this Data Pipeline, please create database in postgresql or your choice sql DB.
- Here are the steps to create PostgreSQL DB which is running inside the container.
    ```
    docker exec <container-name/id> psql -U airflow

    # inside the container
    ################################

    # create database
    CREATE DATABASE covid19;

    # switch database
    \c covid19

    # create table
    CREATE TABLE covid19_data (
        date DATE PRIMARY KEY,
        state VARCHAR(100),
        confirmed INTEGER,
        recovered INTEGER,
        deceased INTEGER
    );
    ```

- Once above steps are done, create connection named as `covid19_connection` to the database using Airflow UI. Check example in [07_connection_and_hooks](https://github.com/imdhruv99/Apache-Airflow-Practical/tree/main/dags/07_connection_and_hooks/images).