# Udacity Data Engineer Capstone Project

## Goal of the project
The purpose of this project is to build an ETL pipeline that will be able to provide information to data analysts, researchers and the general public with COVID-19  statistics for different cities, states/provinces, and countries across the world. It extracts all information from John Hopkins University dataset, and using Apache Spark, it performs several transformations on it and persists the data into csv files into a S3 bucket, that later will be loaded to a Redshift database. 
The csv files get migrated to s3, then the data gets uploaded to Redshift, undergoes further transformation and gets loaded to normalized fact and dimension tables using a series of reusable tasks that allow for easy backfills. Then, using Redshift, some calculations are performed to identify the occurrences of new case numbers over the days.
Finally, data checks are run against the data in the fact and dimension tables so as to catch any discrepancies that might be found in the data.

## Use Cases
### End Use Cases
Considering that the number of cases during the pandemic varies differently in each country, the objective is to create analytic tables for future analysis and development of dashboards.

## Dataset

### John Hopkins University Dataset 
Global COVID-19 cases data files are daily updated from John Hopkins Institute at github [JHU Github](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports).

This dataset tracks confirmed, deaths, and recovered COVID-19 cases in provinces, states, and countries across the world with a breakdown to the county level in the US.

### [Daily reports (csse_covid_19_daily_reports)](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports)

This folder contains daily case reports. All timestamps are in UTC (GMT+0).

#### File naming convention
MM-DD-YYYY.csv in UTC.

#### Field description
* <b>FIPS</b>: US only. Federal Information Processing Standards code that uniquely identifies counties within the USA.
* <b>Admin2</b>: County name. US only.
* <b>Province_State</b>: Province, state or dependency name.
* <b>Country_Region</b>: Country, region or sovereignty name. The names of locations included on the Website correspond with the official designations used by the U.S. Department of State.
* <b>Last Update</b>: MM/DD/YYYY HH:mm:ss  (24 hour format, in UTC).
* <b>Lat</b> and <b>Long_</b>: Dot locations on the dashboard. All points (except for Australia) shown on the map are based on geographic centroids, and are not representative of a specific address, building or any location at a spatial scale finer than a province/state. Australian dots are located at the centroid of the largest city in each state.
* <b>Confirmed</b>: Counts include confirmed and probable (where reported).
* <b>Deaths</b>: Counts include confirmed and probable (where reported).
* <b>Recovered</b>: Recovered cases are estimates based on local media reports, and state and local reporting when available, and therefore may be substantially lower than the true number. US state-level recovered cases are from [COVID Tracking Project](https://covidtracking.com/).
* <b>Active:</b> Active cases = total cases - total recovered - total deaths.
* <b>Incident_Rate</b>: Incidence Rate = cases per 100,000 persons.
* <b>Case_Fatality_Ratio (%)</b>: Case-Fatality Ratio (%) = Number recorded deaths / Number cases.
* All cases, deaths, and recoveries reported are based on the date of initial report. Exceptions to this are noted in the "Data Modification" and "Retrospective reporting of (probable) cases and deaths" subsections below.


## Data Warehouse Model

The project comprises of a redshift postgres database in the cluster with staging tables that contain all the data retrieved from the s3 bucket and copied over to the tables. It also contains a fact table fact_covid_cases and two dimensional tables namely dim_location,and dim_date. The data model representation for the fact and dimension tables is as below:

![Model](images/model.png "Model Diagram")

### Reasons for the model
I settled on the above model since I found that the common data field from the dataset is number of cases and with that I could be able to extrapolate the other data fields that I required for the data pipeline. With the fact and dimension tables, I utilized the star schema which is more suitable and optimized for OLAP (Online Analytical Processing) operations.

## Tools and Technologies used
The tools used in this project include:
- __Apache Spark__ - This was needed to process data from the big data csv files to dataframes and convert them to the more readable and transformed data file. In the process, it maps the columns from the dataset to the relevant columns required for the staging tables and also maps and standardize country and region names, combine country with province to produce the combined_key, calculates the incidence rate for the missing values and creates several indicator for the date. i.e. year, month, and day. 
To view how this is done, check out this [Transform DAG](dags/covid_etl_dag.py).

- __Apache Airflow__ - This was required to automate workflows, namely uploading processed csv files from the local filesystem to s3, creating the staging, fact and dimension tables, copying the s3 files to the redshift staging table then performing ETL to load the final data to the fact and dimension tables. This is all done by pre-configured dags, the [ETL](dags/covid_etl_dag.py) and the [Redshift](dags/covid_redshift_dag.py) that both perform a series of tasks.

- __Amazon Redshift__ - The database is located in a redshift cluster that store the data from s3 and the eventual data that gets added to the fact and dimension tables.

- __Amazon S3__ - Stores the csv files generated by spark that are uploaded from the local filesystem.

### DAGs Airflow Graph View Representation
#### File Upload DAG
![covid_etl_dag](images/airflow_covid_etl.png "Extract Transform and Load DAG")
#### Capstone DAG
![covid_redshift_dag](images/airflow_capstone_dag.png "Redshift operations, fact and dimensions")

## Installation
Clone the repo from github by running:
```
$ git clone git@github.com/LeoArruda/UdacityCapstone.git
```
Once cloned, create a virtualenv on your local machine that will be your local development environment:
```
$ virtualenv venv
$ source venv/bin/activate
```

If running on your local machine, ensure that you have the following main requirements installed in the virtualenv:
- pyspark
- apache-airflow
- requests

Alternatively you can install them from the provided `requirements.txt` file by running the following in your virtual environment.
```
$ pip install -r requirements.txt
 ```

## Workflow/Process
1. A sample size of the csv data files that were processed by spark are already part of this project's files. You can find them in `data/processed`. If you would wish to generate them from scratch, you can follow the same step by step process I went through [here](#Steps-to-generate-the-s3-csv-data-files)
2. On the terminal, ensure your virtualenv is set then add the AIRFLOW_HOME environment variable.
    ```
    $ export AIRFLOW_HOME=~/path/to/project
    ```
3. Run `airflow initdb` to setup the airflow db locally the run both the `airflow scheduler` and `airflow webserver` commands on seperate terminal windows. If you get a psycopg2 related error, you might need to check if the postgresql service is running on your computer.
4. Once you run the above commands successfully you can open the airflow UI on localhost using port 8080 http://0.0.0.0:8080
5. Navigate to your AWS s3 console and create a bucket named `udacity-data-lake`. If you wish to provide another name, ensure you set it in the `covid_etl_dag` and `covid_redshift_dag` operator configs for s3_bucket.
6. Create a Redshift cluster with a redshift database. Once it's finished creating, take note of the endpoint and database credentials.
7. Add your AWS and Redshift credentials in the airflow UI. You can accomplish this in the following steps:
    - Click on the __Admin tab__ and select __Connections__.
    - Under __Connections__, select __Create__.
    - In the Create tab enter the following creds:
        - Conn Id: `aws_credentials`
        - Conn Type: `Amazon Web Services`
        - Login: Your `<AWS Access Key ID>`
        - Password: `<Your AWS Secret Access Key>`
    - Once done, click on __Save and Add Another__
    - On the new create page add the following:
        - Conn Id: `redshift`
        - Conn Type: `Postgres`
        - Host: `<Endpoint of your redshift cluster>`
        - Schema: `<Redshift database name>`
        - Login: `<Database username>`
        - Password: `<Database password>`
        - Port: `<Database port which is usually 5439>`
    - Click save
8. Trigger the `covid_etl_dag` first. This will upload the files to your s3 bucket. You can view the status of the dag tasks in the `Graph View` of the dag.
9. Once the files are uploaded, you can trigger the `covid_redshift_dag`that will create the necessary tables on redshift and load data to them, as well as perform data quality checks.


## Steps to generate the s3 csv data files
1. Run the covid_etl_dag that will:
    - Pull the Covid19 files from JHU in the `data` directory
    - Run an early transformation with Spark and will create the result csv files into `data\processed` directory.
    - Transfer all raw files from `data` to `s3://udacity-data-lake/landing`
    - Transfer all processed files from `data/processed` to `s3://udacity-data-lake/covid19/staging`

## Results

    - Querying dimension date
![query_dim_date](images/query_dim_date.png "Query Dimension Date")

    - Querying dimension location
![query_dim_location](images/query_dim_location.png "Query Dimension Location")

    - Querying fact COVID cases
![query_fact_covid_cases](images/query_fact_covid_cases.png "Query Fact COVID")

## Suggestion for data update frequency
The data should be updated daily if possible, so that the star schema tables are always updated with the most recent data for a more accurate analysis. 

## Possible Scenarios that may arise and how they can be handled.
- If the data gets increased by 100x:
    - The increase of reads and writes to the database can be handled by increasing the number of compute nodes being used in the redshift cluster using elastic resize that can handle for more storage. 
    - Use of distkeys in case of a need to join the tables.
    - Compress the s3 data.
- If the pipelines were needed to be run on a daily basis by 7am:
    - dags can be scheduled to run daily by setting the start_date config as a datetime value containing both the date and time when it should start running, then setting schedule_interval to @daily which will ensure the dag runs everyday at the time provided in start_date.
- If the database needed to be accessed by 100+ people:
    - Utilizing elastic resize for better performance.
    - Utilizing Concurrency Scaling on Redshift by setting it to auto and allocating it's usage to specific user groups and workloads. This will boost query processing for an increasing amount of users.

## Built With
- Python 3.6, Airflow 2.0.2 and pySpark 3.1.1

## Authors
- Leo Arruda - [Github Profile](https://github.com/LeoArruda/UdacityCapstone)